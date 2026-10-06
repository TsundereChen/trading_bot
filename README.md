# Automated Trader

Experimental multi-pair long-only crypto spot bot for Binance, driven by a local [Ollaya](https://github.com/ollaya-dev/ollaya) decision model (`winnow:e4b`). Go, SQLite or PostgreSQL, Prometheus exporter, Podman Compose.

**Not production-ready and no claim of profitability.** Trading live is your decision and your risk.

## Run

```sh
go test -race ./...
go build -o trader .
export CONTROL_TOKEN="$(openssl rand -hex 24)"
export METRICS_TOKEN="$(openssl rand -hex 24)"
export OLLAYA_URL="http://$OLLAYA_HOST:11435"
export TRADING_PAIRS=BTCUSDT,ETHUSDT
./trader
```

Set `OLLAYA_HOST` to the host running Ollaya. Startup is always paused. **Orders require `ENABLE_TRADING=true`**; otherwise decisions are recorded and nothing is sent. There is no frontend; control the bot over HTTP. See `.env.example` for all settings.

## Venue and trading switch

| Setting | Effect |
|---|---|
| `BINANCE_BASE_URL` unset | Binance Spot **Testnet** (default) |
| `BINANCE_BASE_URL=https://api.binance.com` | Binance Spot **live**, real funds |
| `ENABLE_TRADING=true` | The only switch that permits orders, on either venue |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | Required when `ENABLE_TRADING=true` |

Live trading sends real orders at your own risk. The bot logs a warning when live. Use a dedicated account, quote assets limited to USDT, and disable third-asset fee payment.

## Pairs

`TRADING_PAIRS` is a comma-separated list of USDT spot pairs (max 8). Each pair is fully independent: its own cash allocation, position, stop, setup tracking, and daily-loss baseline. `PER_PAIR_BUDGET` is the starting cash per pair, so total capital is roughly `pairs × PER_PAIR_BUDGET`.

Change the list at runtime:

```sh
curl -H "Authorization: Bearer $CONTROL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"pairs","pairs":["BTCUSDT","ETHUSDT","SOLUSDT"]}' http://127.0.0.1:8080/api/control
```

A pair holding a position or a pending order cannot be removed. New pairs start paused.

**Decision scheduling:** inference runs one request at a time, so a pair's decision interval is `5s × pair count`. One pair is evaluated every 5 seconds; three pairs every 15 seconds. Protective exits and reconciliation still run every second for every pair. `/api/status` reports `decision_cycle_seconds`.

## Strategy and limits

- WebSocket `bookTicker` and `kline_1m` per pair; only completed candles drive signals. Reconnects bootstrap history over REST and back off 1–30s.
- Signals: EMA20/50 trend, 14-period ATR and simple-window RSI, relative volume, and an EMA20 pullback-recovery setup.
- Sizing per pair: minimum of max position value, risk budget ÷ 1.5×ATR, and that pair's cash. Defaults: 1000 USDT per pair, 100 USDT position, 2.5 USDT risk, 10 USDT daily loss.
- Local stop-loss (1.5×ATR) and target (3×ATR) exit independent of inference.
- No leverage, shorts, pyramiding, or averaging down. The model cannot set size or bypass limits.
- Order intents are persisted before submission; uncertain outcomes block new orders and are reconciled by client ID.

**Known limits:** exits are not exchange-native, so outages are unprotected. Dust positions may block closing. Full recovery from external trading or venue resets is not automated. Model probabilities are not probabilities of profit.

## API

All `/api/*` require `Authorization: Bearer $CONTROL_TOKEN`. Omitting `pair` on `start`, `pause`, or `close` applies to every pair.

```sh
curl -H "Authorization: Bearer $CONTROL_TOKEN" http://127.0.0.1:8080/api/status
curl -H "Authorization: Bearer $CONTROL_TOKEN" http://127.0.0.1:8080/api/events
curl -H "Authorization: Bearer $CONTROL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"start","pair":"BTCUSDT"}' http://127.0.0.1:8080/api/control
# {"action":"pause"} | {"action":"close"} | {"action":"pairs","pairs":[...]}
```

`GET /healthz` is unauthenticated liveness. Bind to the LAN with `HTTP_ADDR=<address>:8080` and put TLS and firewall rules in front of it. Never expose the control API publicly.

## Monitoring

```sh
BOT_URL=http://127.0.0.1:8080 EXPORTER_ADDR=127.0.0.1:9091 ./trader exporter
```

Scrape the exporter on 9091, not the bot API. Add `prometheus.yml.example` to Prometheus and import `grafana-dashboard.json`. Equity, exposure, feed health, and decisions carry a `pair` label; `trader_portfolio_equity_quote` is the total. `trader_exporter_up` distinguishes process health from upstream availability.

## Storage

`DATABASE_URL` selects PostgreSQL; otherwise SQLite. Both share the `Repository` interface and atomically persist state with an audit event per write. PostgreSQL applies embedded migrations and holds an advisory lock so only one bot uses a database. Switching databases does not migrate data.

## Podman Compose

```sh
cp .env.example .env   # set tokens, POSTGRES_PASSWORD, TRADING_PAIRS, OLLAYA_URL
chmod 600 .env
podman compose up --build -d
```

Runs bot, PostgreSQL, and exporter. `podman compose down` keeps data; `down -v` discards it. If Compose cannot reach Podman, start the socket with `systemctl --user start podman.socket`.

## Backtesting

Rule-only simulation over historical candles. No Ollaya or exchange calls. Signals use preceding candles; fills use the next candle open with adverse slippage and fees. It does not replay five-second live behaviour, spreads, or latency.

```sh
./trader backtest --input history.json > result.json
```

Or `POST` the same JSON to `/api/backtest`. Input is 61–50,000 consecutive one-minute candles plus optional risk fields. Output includes trades, equity curve, P&L, fees, win rate, and drawdown.

## Tests

```sh
go test -race ./...
TEST_DATABASE_URL='postgres://test:password@localhost:5432/disposable_test?sslmode=disable' go test -race ./...
```

PostgreSQL tests write to the target database, so point them at a disposable one.

## Not implemented

Futures, exchange-native stops, Ollaya-based historical simulation, automatic data migration, audit retention, and alert rules.