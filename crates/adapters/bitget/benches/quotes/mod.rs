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

use std::hint::black_box;

use criterion::{Criterion, Throughput};

use super::*;

const MESSAGE_COUNT: usize = 128;

#[derive(Debug)]
struct QuoteStream {
    client: BitgetDataClient,
    ws: BitgetWebSocketClient,
    tasks: TaskSpawner,
    instruments: AHashMap<Ustr, InstrumentAny>,
    product_type: BitgetProductType,
    funding_cache: AHashMap<Ustr, super::super::FundingCacheEntry>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    clock: AtomicTime,
}

// Bitget lists no options, so the option ticker quote group has no input data; the benchmark
// covers the `books1` quote path
pub(crate) fn bench_quotes(c: &mut Criterion) {
    let mut stream = QuoteStream::new(linear_instrument());
    bench_books(c, &mut stream);
}

fn bench_books(c: &mut Criterion, stream: &mut QuoteStream) {
    let instrument = linear_instrument();
    let instrument_id = instrument.id();
    stream.instruments = build_instruments(&[instrument]);
    stream.product_type = BitgetProductType::Linear;
    stream.client.quote_subs.insert(instrument_id);
    let mut initial = orderbook_msg("snapshot", 1_207_360_136_242, true);
    initial.topic = "books1.BTCUSDT".into();
    stream.handle(&BitgetWsMessage::Orderbook(initial.clone()));

    let DataEvent::Data(Data::Quote(quote)) = stream.receiver.try_recv().unwrap() else {
        panic!("Expected quote");
    };

    assert_eq!(
        quote,
        QuoteTick::new(
            instrument_id,
            Price::from("75242.8"),
            Price::from("75242.9"),
            Quantity::from("3.5691"),
            Quantity::from("3.9769"),
            UnixNanos::new(1_794_192_890_496_000_000),
            UnixNanos::new(1_700_000_000_000_000_000),
        )
    );
    stream.receiver.try_recv().unwrap_err();
    let mut group = c.benchmark_group("bitget/book_quotes");
    group.throughput(Throughput::Elements(MESSAGE_COUNT as u64));

    for (name, changed) in [("unchanged", false), ("changed", true)] {
        let messages: Vec<_> = (0..MESSAGE_COUNT)
            .map(|index| {
                let mut message = initial.clone();
                message.ts += (index + 1) as i64;
                if changed {
                    message.data.b[0][1] =
                        if index % 2 == 0 { "3.6000" } else { "3.5691" }.to_string();
                }

                BitgetWsMessage::Orderbook(message)
            })
            .collect();

        assert_eq!(stream.run(&messages), (MESSAGE_COUNT, 0));

        // Depth-one book feeds deliver every quote, including unchanged market fields
        group.bench_function(name, |b| {
            b.iter(|| black_box(stream.run(black_box(&messages))));
        });
    }

    group.finish();
}

impl QuoteStream {
    fn new(instrument: InstrumentAny) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        set_data_event_sender(sender);

        let client = BitgetDataClient::new(
            *BITGET_CLIENT_ID,
            BitgetDataClientConfig {
                product_types: vec![BitgetProductType::Linear],
                ..Default::default()
            },
        )
        .unwrap();

        let ws = client
            .get_ws_client_for_product(BitgetProductType::Linear)
            .unwrap()
            .clone();
        let tasks = client.command_tasks.spawner().unwrap();

        Self {
            client,
            ws,
            tasks,
            instruments: build_instruments(&[instrument]),
            product_type: BitgetProductType::Linear,
            funding_cache: AHashMap::new(),
            receiver,
            clock: AtomicTime::new(false, UnixNanos::new(1_700_000_000_000_000_000)),
        }
    }

    fn run(&mut self, messages: &[BitgetWsMessage]) -> (usize, usize) {
        for message in messages {
            self.handle(message);
        }

        self.drain()
    }

    fn handle(&mut self, message: &BitgetWsMessage) {
        handle_ws_message(
            message,
            &self.client.data_sender,
            &self.instruments,
            Some(self.product_type),
            &self.client.trade_subs,
            &self.client.ticker_subs,
            &self.client.quote_subs,
            &self.client.book_depths,
            &self.client.book_sync,
            &self.ws,
            &self.client.option_greeks_subs,
            &self.client.liquidation_subs,
            self.ws.bar_types_cache(),
            true,
            &self.client.last_quotes,
            &mut self.funding_cache,
            &mut AHashMap::new(),
            Duration::ZERO,
            &self.tasks,
            &self.clock,
        );
    }

    fn drain(&mut self) -> (usize, usize) {
        let mut quotes = 0;
        let mut marks = 0;

        while let Ok(event) = self.receiver.try_recv() {
            match event {
                DataEvent::Data(Data::Quote(quote)) => {
                    black_box(quote);
                    quotes += 1;
                }
                DataEvent::Data(Data::MarkPrice(mark)) => {
                    black_box(mark);
                    marks += 1;
                }
                other => panic!("Unexpected event: {other:?}"),
            }
        }

        (quotes, marks)
    }
}
