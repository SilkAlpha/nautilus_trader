# Bitget

Bitget is a cryptocurrency exchange offering spot markets and USDT-, USDC- and coin-margined
futures, including perpetual contracts on tokenized US stocks and ETFs.

NautilusTrader provides Bitget integration for live market data and execution through the Bitget
Unified Trading Account (UTA) **v3 API**. The adapter is implemented in Rust and exposed to Python
through the same public configurations, factories, and data types as the Bybit adapter it is
derived from.

## Examples

- Python examples: none yet. The Python configuration mirrors the Bybit examples with `Bitget` names
  and an additional `api_passphrase`.
- [Rust examples](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/adapters/bitget/examples/)

## Overview

This guide assumes a trader is setting up for both live market data feeds and trade execution.
The Bitget adapter includes multiple components, which can be used together or separately depending
on the use case.

- `BitgetDataClientConfig` and `BitgetExecutionClientConfig`: Live client configuration.
- `BitgetDataClientFactory` and `BitgetExecutionClientFactory`: Trading node client factories.
- `BitgetDataClient`: A market data feed manager, built by the data client factory.
- `BitgetExecutionClient`: An account management and trade execution gateway, built by the execution
  client factory.
- `BitgetHttpClient`: Low-level HTTP API connectivity.
- `BitgetWebSocketClient`: Low-level WebSocket API connectivity for Rust callers.
- `BitgetLiquidation`: Custom data for public liquidations on USDT- and coin-margined futures, see
  [Liquidations](#liquidations).
- `BITGET`, `BITGET_CLIENT_ID`, `BITGET_VENUE`: Public identifiers.
- `BitgetEnvironment`, `BitgetProductType`, `BitgetMarginMode`, `BitgetPositionIdx`,
  `BitgetPositionMode`: Public enums used by the configurations and order params.

:::note
Most users configure a live trading node as shown below. They do not need to use the lower-level components
directly.
:::

## Bitget documentation

Read the [Bitget UTA API documentation](https://www.bitget.com/docs/uta/quick-start) with this
NautilusTrader integration guide.

## Products

A product is an umbrella term for a group of related instrument types.

:::note
Product is also referred to as `category` in the Bitget v3 API. REST requests use the uppercase
names (`SPOT`, `USDT-FUTURES`, `COIN-FUTURES`); WebSocket subscriptions use the lowercase `instType`
(`spot`, `usdt-futures`, `coin-futures`), and private channels use `UTA`.
:::

The adapter keeps Bybit's four product types and maps them to Bitget categories:

| Product Type                | Bitget category | Supported | Notes                                          |
| --------------------------- | --------------- | --------- | ---------------------------------------------- |
| Spot cryptocurrencies       | `SPOT`          | ✓         | Native spot markets.                           |
| Linear perpetual contracts  | `USDT-FUTURES`  | ✓         | USDT-margined perpetual swaps (`LINEAR`).      |
| Linear futures contracts    | `USDT-FUTURES`  | ✓         | Delivery contracts (`type=delivery`).          |
| Inverse perpetual contracts | `COIN-FUTURES`  | ✓         | Coin-margined perpetual swaps (`INVERSE`).     |
| Inverse futures contracts   | `COIN-FUTURES`  | ✓         | Coin-margined delivery futures.                |
| Option contracts            | -               | -         | Bitget lists no options; requests are refused. |

`USDC-FUTURES` is not mapped to a product type.

## Symbology

To distinguish between different product types on Bitget, Nautilus appends a product suffix to the
Bitget raw symbol:

- `-SPOT`: Spot cryptocurrencies
- `-PERP`: `USDT-FUTURES` perpetual and delivery contracts (`LINEAR`)
- `-INVERSE`: `COIN-FUTURES` perpetual and delivery contracts
- `-OPTION`: Reserved; Bitget lists no options

For example:

- The Ether/Tether spot currency pair is `ETHUSDT-SPOT`.
- The BTCUSDT USDT-margined perpetual is `BTCUSDT-PERP`, and the Tesla stock perpetual is
  `TSLAUSDT-PERP`.
- The BTCUSD coin-margined perpetual is `BTCUSD_CM-INVERSE`. Bitget's raw coin-margined symbols carry
  a `_CM` suffix; delivery contracts look like `BTCCMZ26`.

The linear suffix is `-PERP` rather than Bybit's `-LINEAR` because the trading engine this adapter
was written for keys its instrument bindings on `<SYMBOL>USDT-PERP.BITGET`. Delivery contracts in
`USDT-FUTURES` use the same suffix and load as `CryptoFuture`.

## Instrument loading

The data and execution clients load all instruments for their configured `product_types` when they
connect, from `GET /api/v3/market/instruments`. The default is `LINEAR`. Include each product type
required by your subscriptions or orders.

Instrument fields map as follows: the price increment is `priceMultiplier` and the size increment
is `quantityMultiplier` (not always `10^-precision`), `min_notional` is `minOrderAmount`, and
`max_quantity` is `maxOrderQty` (`0` means no limit). The instrument `info` keeps
`maker_fee_rate`, `taker_fee_rate` and `fund_interval` (hours).

## Environments

Bitget provides a production and a demo environment. Configure the environment with the
`environment` enum on your client configuration.

| Environment | Config                      | Description                                                    |
| ----------- | --------------------------- | -------------------------------------------------------------- |
| **Mainnet** | `BitgetEnvironment.MAINNET` | Production trading with real funds.                            |
| **Demo**    | `BitgetEnvironment.DEMO`    | Demo trading with simulated funds.                             |
| **Testnet** | `BitgetEnvironment.TESTNET` | Kept for parity; Bitget has no testnet, so it behaves as Demo. |

### Mainnet (Production)

The default environment for live trading with real funds.

```python
from nautilus_trader.adapters.bitget import BitgetEnvironment
from nautilus_trader.adapters.bitget import BitgetExecutionClientConfig

config = BitgetExecutionClientConfig(
    api_key="YOUR_API_KEY",
    api_secret="YOUR_API_SECRET",
    api_passphrase="YOUR_API_PASSPHRASE",
    environment=BitgetEnvironment.MAINNET,
)
```

Environment variables: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`

Endpoints:

- REST API: `https://api.bitget.com`
- Public WebSocket: `wss://ws.bitget.com/v3/ws/public` (one URL for every product type)
- Private WebSocket: `wss://ws.bitget.com/v3/ws/private`

### Demo trading

Demo trading uses demo API keys created on the Bitget demo trading page.

```python
from nautilus_trader.adapters.bitget import BitgetEnvironment
from nautilus_trader.adapters.bitget import BitgetExecutionClientConfig

config = BitgetExecutionClientConfig(
    api_key="YOUR_DEMO_API_KEY",
    api_secret="YOUR_DEMO_API_SECRET",
    api_passphrase="YOUR_DEMO_API_PASSPHRASE",
    environment=BitgetEnvironment.DEMO,
)
```

Environment variables: `BITGET_DEMO_API_KEY`, `BITGET_DEMO_API_SECRET`, `BITGET_DEMO_API_PASSPHRASE`

Demo REST requests go to the production host `https://api.bitget.com` with the header
`paptrading: 1`. Demo WebSockets use `wss://wspap.bitget.com/v3/ws/public` and
`wss://wspap.bitget.com/v3/ws/private`.

:::warning
**Demo environment notes**:

- Unlike Bybit, Bitget demo supports WebSocket order entry (`op:"trade"`), so orders are routed the
  same way as on mainnet.
- The demo venue lists far fewer symbols than mainnet (45 USDT-futures symbols when checked). Its
  public books are synthetic. A data client can run on `MAINNET` while the execution client runs on
  `DEMO`, but only symbols listed on demo can be traded there.
- `GET /api/v3/account/fee-rate` returns `40404` on demo; the adapter uses
  `GET /api/v3/account/all-fee-rate`.

:::

### Testnet

Bitget has no testnet. `BitgetEnvironment.TESTNET` is kept for configuration parity with the Bybit
adapter and resolves exactly like `DEMO`: the `paptrading: 1` header, the `wspap.bitget.com` hosts,
and the `BITGET_DEMO_*` environment variables.

### Testnet setup

Not applicable. Use [Demo trading](#demo-trading): create demo API keys (key, secret and passphrase)
from the Bitget demo trading page and export the `BITGET_DEMO_*` variables:

```bash
export BITGET_DEMO_API_KEY="YOUR_DEMO_API_KEY"
export BITGET_DEMO_API_SECRET="YOUR_DEMO_API_SECRET"
export BITGET_DEMO_API_PASSPHRASE="YOUR_DEMO_API_PASSPHRASE"
```

## Quotes and order books

Quote subscriptions use Bitget's `books1` channel, which pushes a full one-level snapshot on every
change. `books1` sends nothing on subscribe and nothing while the book is unchanged, so the data
client seeds each quote from `GET /api/v3/market/orderbook?limit=1` after every successful quote
subscription and again for every quote subscription after a reconnect.

Book subscriptions map the requested depth to a channel. Valid depths are 1, 5, 50 and 1000:

| Depth | Channel   | Push model                                     |
| ----- | --------- | ---------------------------------------------- |
| 1     | `books1`  | Snapshot on every push (shared with quotes).   |
| 5     | `books5`  | Snapshot on every push.                        |
| 50    | `books50` | Snapshot on every push (observed live).        |
| 1000  | `books`   | One full snapshot, then incremental `update`s. |

The default book depth is 50. Only depth 1000 (`books`) is incremental, so only it exercises the
sequence validation and recovery below. Quotes and depth-1 book subscriptions share one topic, as in
the Bybit adapter. Subscribe to one book depth per instrument.

Full-depth `books` frames can contain deep levels whose prices are finer than the instrument tick.
The adapter rounds prices to the instrument precision, so such levels can merge with a neighbour.

## Order book recovery

### Sequence validation

Every `books` frame carries `seq` and `pseq`. A delta is accepted when its `pseq` matches the book's
last accepted `seq` (`pseq <= last < seq`). Any other delta suppresses book output and requests
recovery, as does a delta that reaches a book before its first snapshot.

Every snapshot replaces the book. Bitget documents that after a service restart the sequence can
reset, signalled by a push with `pseq = 0`; the adapter treats such a frame as a new snapshot and
continues from it. A snapshot that arrives while the book's subscription or replacement write is
still in flight is discarded. The REST order book carries no sequence number, so recovery never
stitches REST data into the WebSocket book.

### Snapshot deadlines

Initial and recovery subscriptions wait up to `book_snapshot_timeout_secs` (default **10 seconds**)
for a snapshot after the subscription write completes. Books on a reconnected socket wait the same
time from the reconnect. A missing snapshot starts or retries recovery.

Set `book_snapshot_timeout_secs` to `0` to disable snapshot deadlines. A gap still starts recovery,
but a recovery attempt whose snapshot never arrives then waits until the 180-second retry budget
ends.

A depth-1 book shares its topic with quotes. Because `books1` sends no snapshot on subscribe, a
depth-1 book synchronizes from the next `books1` push.

### Retry limits and reconnects

Recovery unsubscribes and resubscribes the book's channel on the same connection, and the venue
answers with a fresh snapshot. Each recovery episode makes **up to eight attempts within 180
seconds**, with exponential backoff and jitter. It then retries until it accepts a snapshot. The
retry interval doubles from one minute to fifteen minutes. Recovery never ends in a failed state.
Unsubscribe and shutdown cancel it.

A reconnect resynchronizes every book on the reconnected socket from the snapshot of its replayed
subscription and reseeds quotes from REST. It keeps a running recovery with its remaining budget.

See [Order book recovery ownership](../developer_guide/adapters.md#order-book-recovery-ownership)
for the shared recovery machinery and adapter responsibilities.

### Live recovery validation

The `bitget-book-stress` harness is a development tool for changes to book synchronization and
recovery. It connects to Bitget mainnet public market data and submits no orders. It subscribes
full-depth `books` (depth 1000), because the default depth 50 maps to `books50`, which has no
seq/pseq chain. It checks the book stream contract and compares the best 50 levels against an
independent reconstruction of the feed. Gaps are injected by corrupting `pseq`.

From the repository root, run:

```bash
CARGO_BUILD_JOBS=16 bash scripts/strip-adapter-env.bash \
  cargo test -p nautilus-bitget --features examples --test bitget-book-stress -- --timeout 10 --rounds 21
```

`--scenario` selects the run (`churn` by default, `initial`, `turnover`, `boundaries`), with the same
meaning as for the Bybit harness. `--timeout` sets the snapshot timeout in seconds, where `0`
disables snapshot deadlines. `--rounds` sets the number of rounds.

The harness requires the mainnet public WebSocket and the public `/api/v3/market/instruments` REST
API. See [Stress harnesses](../developer_guide/spec_data_testing.md#stress-harnesses) for the shared
flags and output format.

## Orders capability

Plain orders (market and limit) are sent over the private WebSocket with `op:"trade"`. Conditional
orders are Bitget strategy orders (`type=trigger`) and always go over REST
(`/api/v3/trade/place-strategy-order`); their updates arrive on the `strategy-order` channel.

### Order types

| Order Type             | Spot | Linear | Inverse | Option | Notes                                         |
| ---------------------- | ---- | ------ | ------- | ------ | --------------------------------------------- |
| `MARKET`               | ✓    | ✓      | ✓       | -      | Spot market buys need a quote quantity.       |
| `LIMIT`                | ✓    | ✓      | ✓       | -      |                                               |
| `STOP_MARKET`          | ✓    | ✓      | ✓       | -      | REST strategy order, `triggerOrderType=market`. |
| `STOP_LIMIT`           | ✓    | ✓      | ✓       | -      | REST strategy order, `triggerOrderType=limit`.  |
| `MARKET_IF_TOUCHED`    | ✓    | ✓      | ✓       | -      | REST strategy order (trigger).                |
| `LIMIT_IF_TOUCHED`     | ✓    | ✓      | ✓       | -      | REST strategy order (trigger).                |
| `TRAILING_STOP_MARKET` | -    | -      | -       | -      | See [Trailing stops](#trailing-stops).        |

At submission, the adapter denies an unsupported order type locally with an `UNSUPPORTED_ORDER_TYPE`
reason. It does not send the order to the venue.

Bitget derives the trigger direction from the trigger price, so no direction is sent. Triggers use
the last price (`market`) or the mark price (`mark`); index-price triggers are rejected.

### Execution instructions

| Instruction   | Spot | Linear | Inverse | Option | Notes                                                      |
| ------------- | ---- | ------ | ------- | ------ | ---------------------------------------------------------- |
| `post_only`   | ✓    | ✓      | ✓       | -      | Limit orders only; sent as `timeInForce=post_only`.        |
| `reduce_only` | -    | ✓      | ✓       | -      | Sent as `reduceOnly=YES`. *Not supported for Spot*.        |

A post-only order that would cross is accepted by the venue and then pushed `cancelled` with
`cancelReason=post_only_fill_cancel`. The adapter reports it as `OrderRejected` with
`due_post_only=True`. For that reason a post-only order is accepted on its first `new`/`live` push,
not on the request acknowledgement. Other orders are accepted on an acknowledgement with code `0`.

### Time in force

| Time in force | Spot | Linear | Inverse | Option | Notes                           |
| ------------- | ---- | ------ | ------- | ------ | ------------------------------- |
| `GTC`         | ✓    | ✓      | ✓       | -      | Good Till Canceled.             |
| `GTD`         | -    | -      | -       | -      | *Not supported*; sent as `GTC`. |
| `FOK`         | ✓    | ✓      | ✓       | -      | Fill or Kill.                   |
| `IOC`         | ✓    | ✓      | ✓       | -      | Immediate or Cancel.            |

### Advanced order features

| Feature            | Spot | Linear | Inverse | Option | Notes                                                       |
| ------------------ | ---- | ------ | ------- | ------ | ----------------------------------------------------------- |
| Order Modification | ✓    | ✓      | ✓       | -      | Plain orders via WS `modify-order`; conditional via REST.   |
| Bracket/OCO Orders | -    | -      | -       | -      | Not implemented; submit legs yourself.                      |
| Iceberg Orders     | -    | -      | -       | -      | Not implemented.                                            |

Bitget modifies asynchronously: a successful acknowledgement does not mean the order changed. The
result arrives on the `order` channel. Reduce-only orders cannot be modified. Conditional-order
modify (`/api/v3/trade/modify-strategy-order`) always sends a quantity, falling back to the cached
order quantity.

### Batch operations

| Operation    | Spot | Linear | Inverse | Option | Notes                                     |
| ------------ | ---- | ------ | ------- | ------ | ----------------------------------------- |
| Batch Submit | ✓    | ✓      | ✓       | -      | Submit multiple orders in single request. |
| Batch Modify | -    | -      | -       | -      | Not wired into the execution client.      |
| Batch Cancel | ✓    | ✓      | ✓       | -      | Cancel multiple orders in single request. |

Batch submit and batch cancel use WebSocket `batch-place` and `batch-cancel`, up to 20 orders per
request. `batch-place` carries no reduce-only, conditional or BBO orders, so an order list containing
any of them is sent as individual REST requests. A batch cancel that includes a conditional order is
also sent as individual REST requests.

### Position management

| Feature          | Spot | Linear | Inverse | Option | Notes                                                  |
| ---------------- | ---- | ------ | ------- | ------ | ------------------------------------------------------ |
| Query positions  | -    | ✓      | ✓       | -      | `GET /api/v3/position/current-position`.               |
| Position mode    | -    | ✓      | ✓       | -      | Account-wide `one_way_mode` / `hedge_mode`.            |
| Leverage control | -    | ✓      | ✓       | -      | Per symbol via `/api/v3/account/set-leverage`.         |
| Margin mode      | -    | ✓      | ✓       | -      | Account level via `/api/v3/account/adjust-account-mode`. |

Set `margin_mode` on the execution client config to apply a `BitgetMarginMode` when the client
connects: `REGULAR_MARGIN` maps to Bitget `basic`, `PORTFOLIO_MARGIN` to `advanced`, and
`ISOLATED_MARGIN` to `isolated`. Orders are always sent with `marginMode=crossed`.

#### Hedge mode (BothSides)

Bitget's position mode is account-wide (`POST /api/v3/account/set-hold-mode`), and switching it
requires no open positions or orders. In one-way mode the adapter sends no `posSide`. In hedge
mode, pass `position_idx` through the order `params` (`1` long, `2` short); the adapter sends
`posSide=long|short`.

Orders and reports in one-way mode carry no venue position ID. For hedge-mode positions the adapter
maps reports to venue position IDs ending in `-LONG` and `-SHORT`, derived from Bitget's
`holdMode` and `posSide`/`holdSide` fields.

```python
params = {"position_idx": 1}  # 0 one-way, 1 long, 2 short
```

### Risk events

| Feature              | Spot | Linear | Inverse | Option | Notes                                          |
| -------------------- | ---- | ------ | ------- | ------ | ---------------------------------------------- |
| Liquidation handling | -    | ✓      | ✓       | -      | Takeover fills flagged as exchange-generated.  |
| ADL handling         | -    | ✓      | ✓       | -      | Auto-deleveraging fills flagged and logged.    |
| ADL rank warnings    | -    | -      | -       | -      | Bitget position pushes carry no ADL rank.      |

Bitget fills carry no execution type. The adapter derives it from `tradeSide`:

- `burst_*`: liquidation takeover (`BustTrade`).
- `dte_sys_adl_*`: auto-deleveraging (`AdlTrade`).
- `delivery_*`: delivery settlement (`Delivery`).
- `dte_rebase_*`: corporate action rebase, such as a stock split (`CorporateAction`).

The adapter flags each as exchange-generated and logs a warning containing the execution ID,
symbol, side, quantity, and price. These fills flow through the normal `FillReport` path. Funding
settlements are financial records on Bitget, not fills, so they never reach the fill path.

Upstream references:

- [UTA enums (tradeSide, cancelReason)](https://www.bitget.com/docs/uta/enum)

### Order querying

| Feature              | Spot | Linear | Inverse | Option | Notes                                                    |
| -------------------- | ---- | ------ | ------- | ------ | -------------------------------------------------------- |
| Query open orders    | ✓    | ✓      | ✓       | -      | `unfilled-orders` and `unfilled-strategy-orders`.        |
| Query order history  | ✓    | ✓      | ✓       | -      | `history-orders` and `history-strategy-orders`.          |
| Order status updates | ✓    | ✓      | ✓       | -      | `order` and `strategy-order` channels.                   |
| Trade history        | ✓    | ✓      | ✓       | -      | `GET /api/v3/trade/fills`.                               |

Bitget keeps cancelled API orders in `history-orders` and `order-info` for only 2 hours. List
endpoints return `"list": null` when empty, which the adapter decodes as an empty list.

### Contingent orders

| Feature            | Spot | Linear | Inverse | Option | Notes                                  |
| ------------------ | ---- | ------ | ------- | ------ | -------------------------------------- |
| Order lists        | ✓    | ✓      | ✓       | -      | Submitted as a batch via WebSocket.    |
| OCO orders         | -    | -      | -       | -      | Not implemented; submit legs yourself. |
| Bracket orders     | -    | -      | -       | -      | Not implemented; submit legs yourself. |
| Conditional orders | ✓    | ✓      | ✓       | -      | Stop and if-touched as strategy orders. |

An order list is validated as a unit before any leg is sent. When one leg fails validation, that leg
is denied with its specific reason. The remaining legs are denied with `ORDER_LIST_DENIED`.

### Order parameters

Individual orders can be customized using the `params` dictionary when submitting orders:

| Parameter          | Type             | Description                                                              |
| ------------------ | ---------------- | ------------------------------------------------------------------------ |
| `is_leverage`      | `bool`           | Spot only. Sent as `autoBorrow=yes` (borrow if needed). Default: `False`. |
| `take_profit`      | `str` or `float` | TP trigger price. Attaches a native TP to the order.                     |
| `stop_loss`        | `str` or `float` | SL trigger price. Attaches a native SL to the order.                     |
| `tp_trigger_by`    | `str`            | TP trigger type: `"LastPrice"` or `"MarkPrice"`.                         |
| `sl_trigger_by`    | `str`            | SL trigger type: `"LastPrice"` or `"MarkPrice"`.                         |
| `tp_order_type`    | `str`            | TP execution type: `"Market"` or `"Limit"`.                              |
| `sl_order_type`    | `str`            | SL execution type: `"Market"` or `"Limit"`.                              |
| `tp_limit_price`   | `str` or `float` | Limit price for TP when `tp_order_type` is `"Limit"`.                    |
| `sl_limit_price`   | `str` or `float` | Limit price for SL when `sl_order_type` is `"Limit"`.                    |
| `tp_trigger_price` | `str` or `float` | Validated for parity; Bitget place-order has no field for it.            |
| `sl_trigger_price` | `str` or `float` | Validated for parity; Bitget place-order has no field for it.            |
| `tpsl_mode`        | `str`            | Validated for parity; Bitget place-order has no TP/SL mode.              |
| `close_on_trigger` | `bool`           | Validated for parity; not sent.                                          |
| `position_idx`     | `int`            | Hedge-mode position index. See [Hedge mode](#hedge-mode-bothsides).      |
| `bbo_side_type`    | `str`            | BBO side: `"Queue"` or `"Counterparty"`.                                 |
| `bbo_level`        | `str` or `int`   | BBO book level, combined with the side into `bboLevel` (e.g. `queue1`).  |
| `smp_type`         | `str`            | Self-match prevention. See [SMP](#self-match-prevention).                |

Parameters left unset are omitted from the request, so Bitget's own defaults apply. TP/SL fields go
out as `takeprofit`/`stoploss` (WebSocket) or `takeProfit`/`stopLoss` (REST), with trigger types
`market` or `mark`.

:::warning
`close_on_trigger` is not the generic `close_position` whole-position exit contract used by the risk
engine. Do not add `BITGET` to `full_position_exit_venues`.
:::

The adapter validates these params before emitting `OrderSubmitted` and denies the order with a
`VALIDATION_FAILED` reason when a rule is broken:

- Every TP override field requires `take_profit`, and every SL override field likewise requires
  `stop_loss`.
- `tp_order_type="Limit"` requires `tp_limit_price`, and `tp_limit_price` requires
  `tp_order_type="Limit"`. The same pairing applies to the SL fields.
- `bbo_side_type` and `bbo_level` must be provided together.
- `smp_type` must be `"None"`, `"CancelMaker"`, `"CancelTaker"`, or `"CancelBoth"`, matched
  case-insensitively.
- Trigger types other than `"LastPrice"` and `"MarkPrice"` are rejected.

Client order IDs are sent verbatim as `clientOid`, which must match `[A-Za-z0-9_:-]{1,32}`. The REST
order path checks this locally; the venue rejects other IDs.

When `bbo_side_type` and `bbo_level` are set, the adapter sends Bitget's `bboLevel`
(`queue1`..`queue5`, `counterparty1`..`counterparty5`) and omits the order price. BBO orders cannot
be batched.

#### Self-match prevention

Self-match prevention (SMP) tells Bitget what to do when one of your orders would trade against
another of your own orders. The adapter accepts the Bybit-style names and sends Bitget's `stpMode`:

| Value         | Bitget `stpMode` | Behavior                         |
| ------------- | ---------------- | -------------------------------- |
| `None`        | `none`           | No self-match prevention.        |
| `CancelMaker` | `cancel_maker`   | Cancel the resting maker order.  |
| `CancelTaker` | `cancel_taker`   | Cancel the incoming taker order. |
| `CancelBoth`  | `cancel_both`    | Cancel both orders.              |

Set `smp_type` on the execution client config to send that value on every order the client submits:

```python
from nautilus_trader.adapters.bitget import BitgetExecutionClientConfig

config = BitgetExecutionClientConfig(
    api_key="YOUR_API_KEY",
    api_secret="YOUR_API_SECRET",
    api_passphrase="YOUR_API_PASSPHRASE",
    smp_type="CancelMaker",
)
```

Pass `smp_type` through the order `params` to override the configured default for a single order:

```python
params = {"smp_type": "CancelBoth"}
```

With neither the config nor the param set, the adapter omits `stpMode`, so the account default
applies. Bitget applies the taker order's mode.

#### Example: Order with native TP/SL

```python
order = strategy.order_factory.limit(
    instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BITGET"),
    order_side=OrderSide.BUY,
    quantity=Quantity.from_str("0.01"),
    price=Price.from_str("60000.0"),
    params={
        "take_profit": "65000.0",
        "stop_loss": "58000.0",
        "tp_trigger_by": "LastPrice",
        "sl_trigger_by": "MarkPrice",
    },
)
strategy.submit_order(order)
```

#### Example: BBO order

```python
order = strategy.order_factory.limit(
    instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BITGET"),
    order_side=OrderSide.BUY,
    quantity=Quantity.from_str("0.01"),
    price=Price.from_str("60000.0"),
    params={"bbo_side_type": "Queue", "bbo_level": 1},
)
strategy.submit_order(order)
```

#### Example: Spot margin trading

```python
# Submit a Spot order that may borrow (sent as autoBorrow=yes)
order = strategy.order_factory.market(
    instrument_id=InstrumentId.from_str("BTCUSDT-SPOT.BITGET"),
    order_side=OrderSide.SELL,
    quantity=Quantity.from_str("0.001"),
    params={"is_leverage": True},
)
strategy.submit_order(order)
```

:::note
Without `is_leverage=True`, Spot orders are sent with `autoBorrow=no` and use the available balance.
:::

### Spot trading limitations

The following limitations apply to Spot products, as positions are not tracked on the venue side:

- `reduce_only` orders are *not supported*.
- Trailing stop orders are *not supported*.
- Market buys must be sized in quote currency (`quote_quantity=True`); a base-sized market buy is
  denied, because Bitget always reads a spot market-buy `qty` as quote coin.

### Options trading

Bitget lists no options. The option code paths (`-OPTION` symbols, option tickers, Greeks, option
chain) are kept for API parity with the Bybit adapter: option instrument and ticker requests return
a "not supported by Bitget" error, and option-chain reference prices resolve to none.

#### Options data

Not available.

#### Options order parameters

Not available. `order_iv` and `mmp` are validated as in the Bybit adapter but never sent.

#### Options trading limitations

All option functionality is unavailable on Bitget. The `node_greeks`, `node_option_chain` and
`node_delta_neutral` Rust examples exit with a message.

### Trailing stops

The adapter does not submit Nautilus `TRAILING_STOP_MARKET` orders to Bitget. Submitting one denies
the order locally with an `UNSUPPORTED_ORDER_TYPE` reason. Bitget offers `trailing_stop` strategy
orders, which the adapter does not map.

## Spot margin borrowing and repayment

The automatic repayment feature of the Bybit adapter is kept and posts to Bitget's
`POST /api/v3/account/repay`.

### Background

On a Bitget UTA account, a spot sale of a coin you do not hold creates a borrow (`debt` in
`/api/v3/account/assets`) that accrues interest until repaid, unless the account repay mode is
`auto`.

### Automatic repayment (recommended)

The execution client can repay the base-coin borrow after a Spot BUY order fully fills. This feature
is disabled by default; set `auto_repay_spot_borrows=True` to opt in.

**How it works**:

1. When a Spot BUY order fully fills on the standard `fill` channel, the execution client attempts
   to repay the base-coin borrow read from the account `debt`.
1. The repayment is capped at the lesser of the outstanding borrow and the base quantity acquired.
1. A failed request is logged without crashing the execution client.

**Example**:

```python
from nautilus_trader.adapters.bitget import BitgetExecutionClientConfig

config = BitgetExecutionClientConfig(
    api_key="YOUR_API_KEY",
    api_secret="YOUR_API_SECRET",
    api_passphrase="YOUR_API_PASSPHRASE",
    product_types=[BitgetProductType.SPOT],
    auto_repay_spot_borrows=True,  # Opt in; default is False
)
```

### UTC blackout window

Bitget documents no repayment blackout window. The adapter never defers repayments.

### Auto-repayment configuration

| Option                    | Type   | Default | Description                                                                 |
| ------------------------- | ------ | ------- | --------------------------------------------------------------------------- |
| `auto_repay_spot_borrows` | `bool` | `False` | If `True`, automatically repay Spot borrows after BUY orders fully fill.     |

### Auto-repayment notes

- Auto-repayment only triggers on **Spot BUY orders**, not derivatives.
- Bitget has one repay endpoint; the Bybit MNT no-convert special case posts to the same endpoint.
- The repay flow has not been exercised against the venue.

## Funding rates

The adapter takes funding rates from the public `ticker` channel (`fundingRate`,
`nextFundingTime`). Bitget's ticker carries no funding interval, so the adapter fills the
`interval` field of `FundingRateUpdate` from the instrument's `fundInterval` (hours, converted to
minutes). Funding updates are emitted only when the rate or the next funding time changes. Mark and
index prices from the same channel are emitted on every ticker push; the index (sent with up to 16
decimals) is rounded to the instrument price precision.

Historical funding rates come from `GET /api/v3/market/history-fund-rate` (last 90 days, paged by
page number). The interval is computed from consecutive funding timestamps; the oldest record in a
response has no interval.

## Bitget specific data

### Liquidations

The adapter emits `BitgetLiquidation` custom data from Bitget's public `liquidation` WebSocket
channel. It is available for `USDT-FUTURES` and `COIN-FUTURES`, so the adapter rejects Spot and
Option subscriptions. Bitget pushes about once per second and aggregates one record per side; the
adapter emits one `BitgetLiquidation` per record.

| Field              | Type           | Description                                                 |
| ------------------ | -------------- | ----------------------------------------------------------- |
| `instrument_id`    | `InstrumentId` | Instrument of the liquidated position.                      |
| `position_side`    | `PositionSide` | Side from Bitget `side`, mapped as for Bybit.               |
| `bankruptcy_price` | `Price`        | Bitget `price` of the liquidation record.                   |
| `quantity`         | `Quantity`     | Bitget `amount` of the liquidation record.                  |
| `ts_event`         | `int`          | UNIX timestamp in nanoseconds of the record (`ts`).         |
| `ts_init`          | `int`          | UNIX timestamp in nanoseconds when the object was built.    |

The field names are kept from `BybitLiquidation`; Bitget documents `price` as the liquidation price,
not a bankruptcy price.

Subscribe from an actor or strategy with `DataType(BitgetLiquidation.__name__)`. The
`instrument_id` metadata key is required:

```python
from nautilus_trader.adapters.bitget import BitgetLiquidation
from nautilus_trader.model import ClientId
from nautilus_trader.model import CustomData
from nautilus_trader.model import DataType

self.subscribe_data(
    data_type=DataType(
        BitgetLiquidation.__name__,
        metadata={"instrument_id": "BTCUSDT-PERP.BITGET"},
    ),
    client_id=ClientId("BITGET"),
)


def on_data(self, data: CustomData) -> None:
    liquidation = data.data
    if isinstance(liquidation, BitgetLiquidation):
        self.log.info(
            f"{liquidation.instrument_id} {liquidation.position_side} liquidated: "
            f"{liquidation.quantity} @ {liquidation.bankruptcy_price}",
        )
```

The subscription acknowledgement may omit the symbol, so a symbol-scoped liquidation subscription
can remain pending in the subscription state even while data flows.

`BitgetLiquidation` supports Arrow/Parquet catalog persistence under
`data/custom/BitgetLiquidation/{identifier}`. Rust builds need the `nautilus-bitget` `arrow` feature
flag for this persistence.

:::warning
The feed contains the liquidation events Bitget publishes. Treat it as a venue-reported signal, not
as exchange-wide ground truth of every liquidation.
:::

## Rate limiting

The adapter queues requests against rolling windows before it creates an authentication timestamp
or signature. HTTP clients and the trade WebSocket share UID state for the same API key and
environment (Bitget counts REST and WebSocket orders against the same quota).

| Scope                         | Limit used by the adapter                     | Adapter behavior                     |
| ----------------------------- | --------------------------------------------- | ------------------------------------ |
| HTTP IP                       | 6,000 requests per minute                     | Shared by origin and proxy           |
| HTTP and trade WebSocket UID  | Per endpoint (table below)                    | Shared by API key and environment    |
| Trade WebSocket IP            | 3,000 requests per second (local guard only)  | Shared by WebSocket origin and proxy |
| WebSocket connection attempts | 1 per second per domain                       | Shared across connects and reconnects |

Per-UID endpoint quotas (requests per second):

| Endpoints                                                                                                   | Limit |
| ----------------------------------------------------------------------------------------------------------- | ----- |
| `account/adjust-account-mode`, `account/borrow`, `account/repay`, `trade/countdown-cancel-all`              | 1     |
| `account/fee-rate`, `account/all-fee-rate`                                                                  | 3     |
| `trade/place-batch`, `trade/cancel-batch`, `trade/cancel-symbol-order`, `trade/close-positions`             | 5     |
| `trade/place-order`, `trade/cancel-order`, `trade/modify-order`, `trade/batch-modify-order`, `account/set-leverage`, `account/set-hold-mode`, others | 10 |
| `account/assets`, `account/settings`, `account/financial-records`, `trade/unfilled-orders`, `trade/history-orders`, `trade/order-info`, `trade/fills`, strategy-order queries, `position/current-position`, `position/history-position` | 20 |

A batch request counts once against its batch endpoint quota. REST responses carry
`x-mbx-used-remain-limit` (remaining quota of the endpoint), and WebSocket trade acknowledgements
carry `rateLimit: [{limit, remaining}]`; both feed the matching UID window. The 10 per second order
quota is the default account level; higher VIP levels have more.

`recv_window_ms` is kept in the configurations for parity but is not sent: Bitget's per-order
`receiveWindow` would cancel resting orders after the window.

:::warning
Bitget answers an exceeded quota with HTTP `429` (or codes `25004`, `40500`). A 429 starts a shared
5-minute HTTP cooldown for the IP scope instead of a fast retry.
:::

:::warning
Coordination is process-local. Another process or host using the same API key or public IP can
consume venue quota that this adapter cannot reserve in advance.
:::

Explicit venue rate-limit responses are terminal rejections for the affected order operation.
Codes `40010`, `40725` and `45001` (timeout / service error / unknown error) do not prove whether the
venue accepted the order; the adapter never resends and leaves the outcome to reconciliation.

:::info
For more details on rate limiting, see the official documentation: <https://www.bitget.com/docs/uta/quick-start>.
:::

## Account types

The execution client factory determines the account type and OMS type from the configured product
types:

- **Spot only**: `CASH` account type with a `HEDGING` OMS type.
- **Derivatives or mixed products**: `MARGIN` account type (UTA) with a `NETTING` OMS type.

:::info
**Unified Trading Accounts (UTA)**

The adapter uses only the Bitget UTA v3 API. Classic (non-UTA) accounts and the v2 API are not
supported. Balances are reported per coin with total = `equity` and free = `available`; per-coin
margins are not reported.
:::

## Fee currency logic

The adapter takes the commission from the venue: the signed sum of `feeDetail[].fee` (positive means
a cost, negative a rebate) in `feeDetail[].feeCoin`. When the fee coin is missing the settlement
currency is used.

### Spot trading fees

Bitget reports the fee coin per fill in `feeDetail`; the adapter uses it as reported.

#### Normal fees (positive)

Positive `fee` values are costs and become positive commissions.

#### Maker rebates (negative fees)

Negative `fee` values are rebates and become negative commissions.

#### Example: BTCUSDT Spot

The fee coin follows whatever Bitget reports in `feeDetail`; the adapter does not infer it from the
order side.

### Derivatives trading fees

For derivatives the fee is charged in the settlement currency:

| Product Type | Settlement Currency              | Fee Currency |
| ------------ | -------------------------------- | ------------ |
| LINEAR       | USDT                             | USDT         |
| INVERSE      | Base coin (e.g., BTC for BTCUSD) | Base coin    |

Verified on demo: a TSLAUSDT taker fill reported `feeDetail` USDT `0.00288088` on 7.2022 notional
(4 bp), mapped to a positive commission.

### Missing fee data

Bitget's `fast-fill` channel carries no fee. Fill reports parsed from it carry zero commission.
The execution client takes fills from the standard `fill` channel and deduplicates them by
`execId`. The `order` channel carries no fills.

### Official documentation

- [Bitget UTA fill channel](https://www.bitget.com/docs/uta/websocket/private/Fill-Channel)
- [Bitget UTA order channel](https://www.bitget.com/docs/uta/websocket/private/Order-Channel)

## Configuration

The product types for each client must be specified in the configurations.

### Data client configuration options

| Option                             | Default    | Description                                                                                                         |
| ---------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------- |
| `product_types`                    | `[LINEAR]` | Sequence of `BitgetProductType` values to enable.                                                                   |
| `environment`                      | `MAINNET`  | Bitget environment enum. Use `BitgetEnvironment.MAINNET`, `BitgetEnvironment.DEMO`, or `BitgetEnvironment.TESTNET`. |
| `api_key`                          | `None`     | API key; loaded from the matching environment variable when omitted.                                                |
| `api_secret`                       | `None`     | API secret; loaded from the matching environment variable when omitted.                                             |
| `api_passphrase`                   | `None`     | API passphrase (`ACCESS-PASSPHRASE`); loaded from the matching environment variable when omitted.                   |
| `base_url_http`                    | `None`     | Override for the REST base URL.                                                                                     |
| `base_url_ws_public`               | `None`     | Override for the public WebSocket URL.                                                                              |
| `base_url_ws_private`              | `None`     | Override for the private WebSocket URL.                                                                             |
| `proxy_url`                        | `None`     | Optional proxy URL for HTTP and WebSocket transports.                                                               |
| `http_timeout_secs`                | `60`       | Timeout (seconds) for REST requests.                                                                                |
| `max_retries`                      | `3`        | Maximum retry attempts for REST requests.                                                                           |
| `retry_delay_initial_ms`           | `1,000`    | Initial retry delay (milliseconds).                                                                                 |
| `retry_delay_max_ms`               | `10,000`   | Maximum retry delay (milliseconds).                                                                                 |
| `heartbeat_interval_secs`          | `20`       | Interval (seconds) of the text `ping` heartbeat. Bitget closes a socket without a ping for 2 minutes.               |
| `recv_window_ms`                   | `5,000`    | Kept for parity; not sent to Bitget.                                                                                |
| `update_instruments_interval_mins` | `60`       | Interval (minutes) between instrument catalog refreshes.                                                            |
| `instrument_poll_interval_secs`    | `60`       | Interval (seconds) between instrument and status polls; `None` disables polling.                                    |
| `book_snapshot_timeout_secs`       | `10`       | Initial, reconnect, and recovery snapshot wait; `0` disables it.                                                    |
| `transport_backend`                | `Sockudo`  | WebSocket transport backend.                                                                                        |
| `bars_timestamp_on_close`          | `True`     | Timestamp bars on the interval close; `False` timestamps them on the interval open.                                 |

Bars are built from the `kline` channel. Bitget pushes the forming candle without a close flag, so a
bar is emitted when a push with a later start time arrives.

### Execution client configuration options

| Option                      | Default    | Description                                                                                                         |
| --------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------- |
| `product_types`             | `[LINEAR]` | Sequence of `BitgetProductType` values to enable.                                                                   |
| `environment`               | `MAINNET`  | Bitget environment enum. Use `BitgetEnvironment.MAINNET`, `BitgetEnvironment.DEMO`, or `BitgetEnvironment.TESTNET`. |
| `api_key`                   | `None`     | API key; loaded from the matching environment variable when omitted.                                                |
| `api_secret`                | `None`     | API secret; loaded from the matching environment variable when omitted.                                             |
| `api_passphrase`            | `None`     | API passphrase; loaded from the matching environment variable when omitted.                                         |
| `base_url_http`             | `None`     | Override for the REST base URL.                                                                                     |
| `base_url_ws_private`       | `None`     | Override for the private WebSocket URL (order, fill, position, account pushes).                                     |
| `base_url_ws_trade`         | `None`     | Override for the order-entry WebSocket URL; defaults to the private URL.                                            |
| `proxy_url`                 | `None`     | Optional proxy URL for HTTP and WebSocket transports.                                                               |
| `http_timeout_secs`         | `60`       | Timeout (seconds) for REST requests.                                                                                |
| `max_retries`               | `3`        | Maximum retry attempts for REST requests. Order-mutation requests are never retried.                                |
| `retry_delay_initial_ms`    | `1,000`    | Initial retry delay (milliseconds).                                                                                 |
| `retry_delay_max_ms`        | `10,000`   | Maximum retry delay (milliseconds).                                                                                 |
| `heartbeat_interval_secs`   | `20`       | Interval (seconds) of the text `ping` heartbeat.                                                                    |
| `auth_timeout_secs`         | `None`     | WebSocket login wait (seconds); orders queued before login are rejected locally after it (default 10 s).            |
| `recv_window_ms`            | `5,000`    | Kept for parity; not sent to Bitget.                                                                                |
| `account_id`                | `None`     | Optional account ID associated with this client.                                                                    |
| `use_spot_position_reports` | `False`    | Report Spot wallet balances as positions for scoped requests; bulk reports omit Spot (no pair attribution).         |
| `auto_repay_spot_borrows`   | `False`    | Automatically repay Spot borrows after BUY orders fully fill.                                                       |
| `futures_leverages`         | `None`     | Leverage per symbol, applied with `set-leverage` on connect.                                                        |
| `position_mode`             | `None`     | Position mode per symbol; Bitget applies hold mode account-wide.                                                    |
| `margin_mode`               | `None`     | Account margin mode, applied with `adjust-account-mode` on connect.                                                 |
| `smp_type`                  | `None`     | Self-match prevention sent on every order. See [SMP](#self-match-prevention).                                       |
| `transport_backend`         | `Sockudo`  | WebSocket transport backend.                                                                                        |

The compiled default is Sockudo when the `transport-sockudo` Cargo feature is enabled and
Tungstenite otherwise.

The execution client opens two private WebSocket connections to the same Bitget URL, both logged in:
one subscribes to `order`, `strategy-order`, `fill`, `position` and `account`, the other carries
`op:"trade"` order requests. Bitget has no separate trade endpoint. Orders submitted before the trade
socket is logged in are queued and rejected locally (never sent) if login does not complete within
the authentication timeout. The account state is loaded from `GET /api/v3/account/assets` on connect
and updated from the `account` channel.

Use `BitgetDataClientConfig` with `BitgetDataClientFactory` and `BitgetExecutionClientConfig` with
`BitgetExecutionClientFactory`.

### API credentials

Bitget API keys have a key, a secret and a passphrase. Either pass `api_key`, `api_secret` and
`api_passphrase` to the configuration objects, or set the following environment variables.

For Bitget live clients, you can set:

- `BITGET_API_KEY`
- `BITGET_API_SECRET`
- `BITGET_API_PASSPHRASE`

For Bitget demo clients (and `TESTNET`, which resolves to demo), you can set:

- `BITGET_DEMO_API_KEY`
- `BITGET_DEMO_API_SECRET`
- `BITGET_DEMO_API_PASSPHRASE`

REST requests are signed with `ACCESS-SIGN = base64(HMAC_SHA256(secret, timestamp + METHOD +
path[?query] + body))` and sent with `ACCESS-KEY`, `ACCESS-TIMESTAMP` (milliseconds) and
`ACCESS-PASSPHRASE`. WebSocket login signs `timestamp + "GET" + "/user/verify"` with a millisecond
timestamp.

:::tip
We recommend using environment variables to manage your credentials.
:::

When starting the trading node, you'll receive immediate confirmation of whether your
credentials are valid and have trading permissions.

## Contributing

:::info
For additional features or to contribute to the Bitget adapter, please see our
[contributing guide](https://github.com/nautechsystems/nautilus_trader/blob/develop/CONTRIBUTING.md).
:::
