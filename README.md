# PyneCore Bybit Plugin

[Bybit](https://www.bybit.com) v5 integration for
[PyneCore](https://pynesys.io): historical and live market data plus live
order execution over the Bybit v5 Open API, covering the **spot**,
**linear** (USDT/USDC perpetuals and futures) and **inverse** (coin-margined)
categories.

## Status

Both the **data provider** (`LiveProviderPlugin`) and **live order
execution** (`BrokerPlugin`) are implemented and verified against the Bybit
demo environment: historical plus live OHLCV for all three categories, and
order execution with entries, exit brackets, cancels, in-place amends,
crash recovery and fill reconciliation over the private WebSocket event
stream.

## Demo first

Bybit's demo environment (`api-demo.*`) runs on **real mainnet prices with
simulated balances** — exactly the paper-trade path you should run before
risking funds. The plugin defaults to `demo = true`; set it to `false` only
once your strategy has proven itself on demo.

Demo API keys are generated from your mainnet account after switching the
dashboard to Demo Trading. Demo covers spot, linear and inverse alike, and
the private order/execution stream works the same as live. (Demo hosts serve
no public market-data stream; public data always comes from the mainnet
stream, which is fine — demo trades on mainnet prices.)

## Configuration

Settings live in `workdir/config/plugins/bybit.toml`, auto-generated from
[`BybitConfig`](src/pynecore_bybit/config.py) on first run. Fill in your API
key pair and pick the environment:

```toml
demo = true            # demo (api-demo.*) vs. live real funds
region = "global"      # global | eu | nl | tr | kz | ge | ae | id | testnet
api_key = ""
api_secret = ""
```

- **`demo`** selects the paper-trade environment (see above).
- **`region`** is your account's legal entity / host family, decided by your
  residency — it is **not** auto-detected. The `(region, demo)` pair resolves
  to a REST + WebSocket host triple (see
  [`hosts.py`](src/pynecore_bybit/hosts.py)).
- Optional `rest_host` / `ws_public_host` / `ws_private_host` overrides are an
  escape hatch for unlisted regional domains.

### API key expiry and rotation

Bybit API keys **without an IP whitelist expire after a limited period**
(currently three months) — a long-running bot will silently lose
authentication when the key lapses. For unattended operation either bind the
key to your server's IP address (whitelisted keys do not expire) or put a
key-rotation reminder in your operations calendar.

### The EU entity

Bybit EU (`region = "eu"`, the MiCA-regulated entity for EEA residents) does
**not** allow generating API keys directly: API access is only available
through Bybit's third-party-application program, and that path is currently
not usable in practice. Until this changes, EU-entity accounts cannot be used
with this plugin — the host table and the `eu` region are wired and waiting,
but there is no way to obtain a key. Development and verification run on the
global entity's demo environment.

## Symbols

The provider accepts TradingView-style notation:

- `BTCUSDT` — plain name: spot first, then other categories are probed
- `BTCUSDT.P`, `BTCUSD.P` — `.P` suffix: perpetual, linear probed first
- `BTCUSDT-26JUN26` — dated futures under their native Bybit name

## Architecture

- **Transport**: raw `httpx` (REST) + `websockets` (WS), no vendor SDK — the
  v5 API is plain JSON REST and JSON WebSocket, and the official `pybit` SDK
  is thread-based, which would fight the asyncio broker event loop.
- **Authentication**: API key + secret, HMAC-SHA256 request signing; the
  private WebSocket authenticates with the same key pair.
- **One plugin, category-based model selection.** The traded symbol's
  category (spot / linear / inverse) is resolved at connect time and selects
  both the execution model and the capability profile.
- **Push order events**: order state and fills arrive over the private
  WebSocket `order` / `execution` / `position` topics — no polling. Crash
  recovery reconciles missed fills from REST history on restart.

## Category notes

### Spot

- **Inventory model, not position model.** Spot has no exchange-side
  position object; PyneCore's spot-inventory core folds the plugin's
  execution ledger into a synthetic position. Short selling is unsupported
  (the core rejects shorting scripts on spot).
- **Fee dust**: Bybit charges the spot buy fee in the *base* coin, so round
  trips leave sub-precision dust behind. Balances below one quantity-grid
  step are treated as flat.

### Linear (USDT/USDC perpetuals and futures)

- Real venue position model with native short selling and `reduceOnly`.
- One-way position mode is the native path; hedge-mode accounts run through
  PyneCore's one-way emulation layer, so Pine one-way semantics hold on both.

### Inverse (coin-margined)

- Contracts are whole-USD denominated and settle in the base coin; the
  plugin maps Pine's base-quantity model onto USD contracts at dispatch, so
  strategies behave exactly as on linear (this matches TradingView, which
  also applies linear `qty * Δprice` accounting to inverse symbols).
- **Collateral switch gotcha**: on a Unified Trading Account the settle
  coin's collateral switch must be **ON**, otherwise every inverse order is
  rejected with `retCode 110101`. Enable it under margin/collateral settings
  for the coin you trade (e.g. BTC for `BTCUSD`).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
