# PyneCore Bybit Plugin

[Bybit](https://www.bybit.com) v5 **spot** integration for
[PyneCore](https://pynesys.io): historical and live market data plus spot
order execution over the Bybit v5 Open API.

## Status

Early scaffold. The configuration and host-resolution layer is in place; the
data provider and broker layers are being built in phases (M1–M4) against the
Bybit **demo** environment, which runs on real mainnet prices with simulated
balances — the paper-trade path PyneCore requires before a broker plugin ships.

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

- **`demo`** selects the paper-trade environment (real prices, simulated
  balances). Set to `false` only to trade with real funds.
- **`region`** is your account's legal entity / host family, decided by your
  residency — it is **not** auto-detected. EEA residents use `eu`
  (`api.bybit.eu`, Bybit EU / MiCA). The `(region, demo)` pair resolves to a
  REST + WebSocket host triple (see
  [`hosts.py`](src/pynecore_bybit/hosts.py)).
- Optional `rest_host` / `ws_public_host` / `ws_private_host` overrides are an
  escape hatch for unlisted regional domains.

Demo API keys are generated from the mainnet account switched to Demo Trading.
Note that Bybit API keys without an IP whitelist expire after a limited period
— a bot-operations gotcha to plan for.

## Architecture

- **Transport**: raw `httpx` (REST) + `websockets` (WS), no vendor SDK — the
  v5 API is plain JSON REST and JSON WebSocket, and the official `pybit` SDK
  is thread-based, which would fight the asyncio broker event loop.
- **Authentication**: API key + secret, HMAC-SHA256 request signing; the
  private WebSocket authenticates with the same key pair.
- **Spot = inventory model, not position model.** Spot has no exchange-side
  position object; PyneCore's `SpotInventoryManager` folds the execution
  ledger into a synthetic position, and this plugin supplies the thin
  `SpotInventoryPort`. Short selling is unsupported (the spot gate).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
