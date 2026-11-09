// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Adapter-local order book synchronization state for Bitget.
//!
//! [`BookSyncTracker`] applies Bitget `seq`/`pseq` linkage to the shared per-book [`BookSync`]
//! lifecycle and decides whether to accept a book frame, suppress it, or request recovery. A
//! book's position is the `seq` of its last accepted frame.
//!
//! Every snapshot replaces the book, as the venue requires: Bitget sends one after each subscribe
//! on the full-depth `books` channel, resends one (`pseq` 0) after a service restart, and sends
//! only snapshots on `books1`/`books5`. A snapshot can carry a `seq` below the book's position,
//! since the venue serves it from an earlier state. A delta links to the book when its
//! `[pseq, seq)` range contains the book's position: in steady state `pseq` equals the previous
//! `seq`, and the first update after a snapshot spans the snapshot's `seq`. Any other delta is a
//! gap.
//!
//! Claiming, accepting, and resetting recovery stay under the tracker's state lock so competing
//! events cannot independently change ownership. Snapshot gates coordinate acceptance with
//! transport sends. This module performs state transitions; [`super::recovery`] runs the
//! asynchronous subscription and retry work.
//!
//! Every delta for an unsynced book goes through [`BookSync::gap`] once the initial subscription
//! write completes, so a book that no running recovery or armed deadline owns keeps requesting
//! recovery. The data client claims only after it has a depth and socket to recover with; an early
//! return leaves the book unowned for the next delta.

use std::sync::Arc;

use ahash::AHashMap;
use nautilus_common::live::dst::time::{Duration, Instant};
use nautilus_live::book::{snapshot::SnapshotGate, sync::BookSync};
use nautilus_model::identifiers::InstrumentId;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::{BookSequenceOutcome, BookSyncSignal, BookSyncSignalKind, recovery::BookRecovery};
use crate::{common::enums::BitgetProductType, websocket::error::BitgetWsError};

type Book = BookSync<BitgetWsError, u64>;

#[derive(Debug, Clone, Default)]
pub(crate) struct BookSyncTracker {
    books: Arc<Mutex<AHashMap<InstrumentId, Book>>>,
}

impl BookSyncTracker {
    pub(crate) fn record_subscription(
        &self,
        instrument_id: InstrumentId,
        now: Instant,
        gate: SnapshotGate,
    ) -> CancellationToken {
        let mut book = Book::new(now);
        let cancel = book.expect_snapshot(gate);
        self.books.lock().insert(instrument_id, book);
        cancel
    }

    pub(crate) fn remove(&self, instrument_id: InstrumentId) {
        self.books.lock().remove(&instrument_id);
    }

    pub(crate) fn clear(&self) {
        self.books.lock().clear();
    }

    /// Removes a book whose initial subscription failed, returning `false` when a later
    /// unsubscribe or resubscribe already replaced it.
    pub(crate) fn remove_subscription(
        &self,
        instrument_id: InstrumentId,
        cancel: &CancellationToken,
    ) -> bool {
        let mut books = self.books.lock();

        if cancel.is_cancelled() {
            return false;
        }

        books.remove(&instrument_id).is_some()
    }

    pub(crate) fn validate(
        &self,
        instrument_id: InstrumentId,
        is_snapshot: bool,
        update_id: u64,
        prev_update_id: u64,
        now: Instant,
    ) -> BookSequenceOutcome {
        let mut books = self.books.lock();

        let Some(book) = books.get_mut(&instrument_id) else {
            return BookSequenceOutcome::Suppress;
        };

        if book.is_send_pending() {
            return BookSequenceOutcome::Suppress;
        }

        // A reset sequence (`pseq` 0) restarts the book like a snapshot
        if is_snapshot || prev_update_id == 0 {
            return if book.accept_snapshot(update_id, now) {
                BookSequenceOutcome::Accept
            } else {
                BookSequenceOutcome::Suppress
            };
        }

        let last_update_id = book.position().copied();

        if last_update_id.is_some_and(|last| prev_update_id <= last && last < update_id) {
            book.advance(update_id, now);
            return BookSequenceOutcome::Accept;
        }

        let outcome = book.gap();

        if outcome == BookSequenceOutcome::Recover {
            log::warn!(
                "Book update gap for {instrument_id}: last_seq={last_update_id:?}, \
                 pseq={prev_update_id}, seq={update_id}; requesting a fresh snapshot"
            );
        }

        outcome
    }

    pub(crate) fn reset_on_reconnect(&self, product_type: BitgetProductType) {
        let mut books = self.books.lock();

        for (_, book) in scoped(&mut books, product_type) {
            book.reset_on_reconnect();
        }
    }

    pub(crate) fn seed_pending_snapshots(
        &self,
        product_type: BitgetProductType,
        timeout: Duration,
        now: Instant,
    ) -> usize {
        let deadline = now + timeout;
        let mut books = self.books.lock();

        scoped(&mut books, product_type)
            .map(|(_, book)| book.arm_deadline(deadline))
            .filter(|armed| *armed)
            .count()
    }

    pub(crate) fn take_expired_snapshots(
        &self,
        product_type: BitgetProductType,
        now: Instant,
    ) -> Vec<BookSyncSignal> {
        let mut books = self.books.lock();

        let mut expired = scoped(&mut books, product_type)
            .filter_map(|(instrument_id, book)| {
                book.take_expired(now).then_some(BookSyncSignal {
                    instrument_id: *instrument_id,
                    kind: BookSyncSignalKind::SnapshotMissing,
                })
            })
            .collect::<Vec<_>>();

        // Sort by instrument; the book map iterates in per-process hash order
        expired.sort_by_key(|signal| signal.instrument_id);
        expired
    }

    pub(crate) fn claim_subscription_recovery(
        &self,
        instrument_id: InstrumentId,
        cancel: &CancellationToken,
    ) -> Option<Arc<BookRecovery>> {
        let mut books = self.books.lock();

        if cancel.is_cancelled() {
            return None;
        }

        books.get_mut(&instrument_id)?.claim()
    }

    pub(crate) fn claim_recovery(&self, instrument_id: InstrumentId) -> Option<Arc<BookRecovery>> {
        self.books
            .lock()
            .get_mut(&instrument_id)
            .filter(|book| !book.is_send_pending())?
            .claim()
    }
}

// Each product type has its own socket, and an instrument's symbol suffix names its product type
fn scoped(
    books: &mut AHashMap<InstrumentId, Book>,
    product_type: BitgetProductType,
) -> impl Iterator<Item = (&InstrumentId, &mut Book)> {
    books.iter_mut().filter(move |(instrument_id, _)| {
        BitgetProductType::from_suffix(instrument_id.symbol.as_str()) == Some(product_type)
    })
}

#[cfg(test)]
mod tests {
    use nautilus_common::live::dst::time::{Duration, Instant};
    use nautilus_live::book::{snapshot::SnapshotGate, sync::BookPhase};
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::{BookSequenceOutcome, BookSyncSignalKind, BookSyncTracker};
    use crate::common::enums::BitgetProductType;

    const LINEAR: &str = "BTCUSDT-PERP.BITGET";
    const SPOT: &str = "BTCUSDT-SPOT.BITGET";

    fn synced(tracker: &BookSyncTracker, instrument_id: InstrumentId, update_id: u64) {
        let now = Instant::now();
        tracker.record_subscription(instrument_id, now, SnapshotGate::default());
        let outcome = tracker.validate(instrument_id, true, update_id, 0, now);
        assert_eq!(outcome, BookSequenceOutcome::Accept);
    }

    fn phase(tracker: &BookSyncTracker, instrument_id: InstrumentId) -> BookPhase<u64> {
        *tracker.books.lock()[&instrument_id].phase()
    }

    #[rstest]
    fn delta_following_position_advances_book() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);

        let first = tracker.validate(id, false, 101, 100, Instant::now());
        let second = tracker.validate(id, false, 102, 101, Instant::now());

        assert_eq!(first, BookSequenceOutcome::Accept);
        assert_eq!(second, BookSequenceOutcome::Accept);
        assert_eq!(phase(&tracker, id), BookPhase::Synced(102));
    }

    // The first update after a snapshot may start before the snapshot's seq
    #[rstest]
    fn first_delta_spanning_snapshot_seq_advances_book() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);

        let first = tracker.validate(id, false, 105, 95, Instant::now());
        let second = tracker.validate(id, false, 110, 105, Instant::now());

        assert_eq!(first, BookSequenceOutcome::Accept);
        assert_eq!(second, BookSequenceOutcome::Accept);
        assert_eq!(phase(&tracker, id), BookPhase::Synced(110));
    }

    // A service restart resets the sequence and the venue pushes `pseq` 0
    #[rstest]
    fn reset_sequence_replaces_book() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);

        let reset = tracker.validate(id, false, 7, 0, Instant::now());

        assert_eq!(reset, BookSequenceOutcome::Accept);
        assert_eq!(phase(&tracker, id), BookPhase::Synced(7));
    }

    #[rstest]
    #[case::skipped(102)]
    #[case::repeated(100)]
    #[case::behind(99)]
    fn unlinked_delta_requests_recovery_until_claimed(#[case] update_id: u64) {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);

        let first = tracker.validate(id, false, update_id, update_id - 1, Instant::now());
        let again = tracker.validate(id, false, 101, 100, Instant::now());
        let recovery = tracker.claim_recovery(id).unwrap();
        let owned = tracker.validate(id, false, 102, 101, Instant::now());

        assert_eq!(first, BookSequenceOutcome::Recover);
        assert_eq!(again, BookSequenceOutcome::Recover);
        assert_eq!(owned, BookSequenceOutcome::Suppress);
        assert!(recovery.is_running());
        assert_eq!(phase(&tracker, id), BookPhase::Recovering);
    }

    // The venue serves a replacement snapshot from an earlier state, so its update ID can go back
    #[rstest]
    #[case::behind(90)]
    #[case::ahead(500)]
    #[case::restart(1)]
    fn snapshot_replaces_synced_book_at_any_update_id(#[case] update_id: u64) {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);

        let snapshot = tracker.validate(id, true, update_id, 0, Instant::now());
        let delta = tracker.validate(id, false, update_id + 1, update_id, Instant::now());

        assert_eq!(snapshot, BookSequenceOutcome::Accept);
        assert_eq!(delta, BookSequenceOutcome::Accept);
        assert_eq!(phase(&tracker, id), BookPhase::Synced(update_id + 1));
    }

    #[rstest]
    fn snapshot_completes_running_recovery_once_write_confirmed() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);
        assert_eq!(
            tracker.validate(id, false, 102, 101, Instant::now()),
            BookSequenceOutcome::Recover
        );
        let recovery = tracker.claim_recovery(id).unwrap();
        assert!(recovery.begin_replacement());

        let during_write = tracker.validate(id, true, 200, 0, Instant::now());
        recovery.gate.open();
        let after_write = tracker.validate(id, true, 200, 0, Instant::now());

        assert_eq!(during_write, BookSequenceOutcome::Suppress);
        assert_eq!(after_write, BookSequenceOutcome::Accept);
        assert!(recovery.is_accepted());
        assert_eq!(phase(&tracker, id), BookPhase::Synced(200));
    }

    #[rstest]
    #[case::snapshot(true)]
    #[case::delta(false)]
    fn initial_send_gate_suppresses_frames_until_write_completes(#[case] is_snapshot: bool) {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        let now = Instant::now();
        let gate = SnapshotGate::default();
        gate.lock().close();
        let cancel = tracker.record_subscription(id, now, gate.clone());

        let while_sending = tracker.validate(id, is_snapshot, 100, 99, now);
        let claimed_while_sending = tracker.claim_recovery(id).is_some();
        gate.open();
        let snapshot = tracker.validate(id, true, 100, 0, now);

        assert_eq!(while_sending, BookSequenceOutcome::Suppress);
        assert!(!claimed_while_sending);
        assert_eq!(snapshot, BookSequenceOutcome::Accept);
        assert!(cancel.is_cancelled());
        assert!(tracker.claim_subscription_recovery(id, &cancel).is_none());
    }

    #[rstest]
    fn untracked_book_suppresses_frames() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);

        let snapshot = tracker.validate(id, true, 100, 0, Instant::now());
        let delta = tracker.validate(id, false, 101, 100, Instant::now());

        assert_eq!(snapshot, BookSequenceOutcome::Suppress);
        assert_eq!(delta, BookSequenceOutcome::Suppress);
        assert!(tracker.books.lock().is_empty());
    }

    #[rstest]
    #[case::removed(false, true)]
    #[case::replaced(true, false)]
    fn remove_subscription_removes_only_current_book(
        #[case] resubscribed: bool,
        #[case] expected: bool,
    ) {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        let cancel = tracker.record_subscription(id, Instant::now(), SnapshotGate::default());

        if resubscribed {
            tracker.record_subscription(id, Instant::now(), SnapshotGate::default());
        }

        let removed = tracker.remove_subscription(id, &cancel);

        assert_eq!(removed, expected);
        assert_eq!(tracker.books.lock().contains_key(&id), !expected);
    }

    #[rstest]
    fn reconnect_resyncs_only_books_of_its_product_type() {
        let tracker = BookSyncTracker::default();
        let linear = InstrumentId::from(LINEAR);
        let spot = InstrumentId::from(SPOT);
        synced(&tracker, linear, 100);
        synced(&tracker, spot, 200);
        let now = Instant::now();

        tracker.reset_on_reconnect(BitgetProductType::Linear);
        let armed =
            tracker.seed_pending_snapshots(BitgetProductType::Linear, Duration::from_secs(10), now);
        let linear_delta = tracker.validate(linear, false, 101, 100, now);
        let spot_delta = tracker.validate(spot, false, 201, 200, now);
        let spot_expired =
            tracker.take_expired_snapshots(BitgetProductType::Spot, now + Duration::from_secs(10));
        let linear_expired = tracker
            .take_expired_snapshots(BitgetProductType::Linear, now + Duration::from_secs(10));

        assert_eq!(armed, 1);
        assert_eq!(linear_delta, BookSequenceOutcome::Suppress);
        assert_eq!(spot_delta, BookSequenceOutcome::Accept);
        assert!(spot_expired.is_empty());
        assert_eq!(linear_expired.len(), 1);
        assert_eq!(linear_expired[0].instrument_id, linear);
        assert_eq!(linear_expired[0].kind, BookSyncSignalKind::SnapshotMissing);
        assert_eq!(phase(&tracker, spot), BookPhase::Synced(201));
    }

    #[rstest]
    fn reconnect_keeps_running_recovery() {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);
        let recovery = tracker.claim_recovery(id).unwrap();

        tracker.reset_on_reconnect(BitgetProductType::Linear);

        assert!(recovery.is_running());
        assert!(tracker.claim_recovery(id).is_none());
    }

    #[rstest]
    fn take_expired_snapshots_returns_instruments_in_sorted_order() {
        let tracker = BookSyncTracker::default();
        let now = Instant::now();
        let ids = [
            "SOLUSDT-PERP.BITGET",
            "BTCUSDT-PERP.BITGET",
            "XRPUSDT-PERP.BITGET",
            "ETHUSDT-PERP.BITGET",
        ]
        .map(InstrumentId::from);

        for id in ids {
            synced(&tracker, id, 100);
        }

        tracker.reset_on_reconnect(BitgetProductType::Linear);
        tracker.seed_pending_snapshots(BitgetProductType::Linear, Duration::from_secs(1), now);
        let expired =
            tracker.take_expired_snapshots(BitgetProductType::Linear, now + Duration::from_secs(1));

        let mut sorted = ids.to_vec();
        sorted.sort();
        assert_eq!(
            expired
                .iter()
                .map(|signal| signal.instrument_id)
                .collect::<Vec<_>>(),
            sorted
        );
    }

    #[rstest]
    #[case::removed(false)]
    #[case::cleared(true)]
    fn removing_book_cancels_its_recovery(#[case] clear: bool) {
        let tracker = BookSyncTracker::default();
        let id = InstrumentId::from(LINEAR);
        synced(&tracker, id, 100);
        let recovery = tracker.claim_recovery(id).unwrap();

        if clear {
            tracker.clear();
        } else {
            tracker.remove(id);
        }

        assert!(recovery.cancellation.is_cancelled());
        assert!(tracker.books.lock().is_empty());
    }
}
