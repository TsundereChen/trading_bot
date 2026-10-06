# Automated Trader

Experimental long-only crypto spot bot for **Binance Spot Testnet**, driven by a local [Ollaya](https://github.com/ollaya-dev/ollaya) decision model (`winnow:e4b`). Go, SQLite or PostgreSQL, Prometheus exporter, Podman Compose.

**Not production-ready and no claim of profitability.** Order traffic is hardcoded to `testnet.binance.vision`.

## Run

Requires Go 1.26+, Binance Spot Testnet access, and a reachable Ollaya daemon.

```sh
go test -race ./...
go build -o trader .
export CONTROL_TOKEN="$(openssl rand -hex 24)"
export METRICS_TOKEN="$(openssl rand -hex 24)"
export OLLAYA_URL="http://$OLLAYA_HOST:11435"
./trader
```

Set `OLLAYA_HOST` to the host running Ollaya. Startup is always paused and **default mode is observe-only**: decisions are recorded, no orders submitted. There is no frontend; control the bot over HTTP.

Configuration reference: `.env.example`. Native runs do not load `.env`; Compose does. Keep credentials out of git and chat.

To enable testnet orders, set `BINANCE_TESTNET_API_KEY`, `BINANCE_TESTNET_API_SECRET`, `ENABLE_TESTNET_TRADING=true`, then start entries explicitly. Use a dedicated testnet account, quote assets limited to USDT, and disable third-asset fee payment.

## Strategy and limits

- WebSocket `bookTicker` and `kline_1m` feeds; only completed candles drive signals. Reconnects bootstrap history over REST and back off 1–30s.
- Ollaya decision every 5 seconds with a 4-second deadline; no overlapping requests.
- Signals: EMA20/50 trend, 14-period ATR and simple-window RSI, relative volume, and an EMA20 pullback-recovery setup.
- Sizing: minimum of max position value, risk budget ÷ 1.5×ATR, and available cash. Defaults: 1000 USDT allocation, 100 USDT position, 2.5 USDT risk, 10 USDT daily loss limit.
- Local stop-loss (1.5×ATR) and target (3×ATR) exit independent of inference, checked every second.
- No leverage, shorts, pyramiding, or averaging down. The model cannot set size or bypass limits.
- Order intents are persisted before submission; uncertain outcomes block new orders and are reconciled by client ID.

**Known limits:** exits are not exchange-native, so outages are unprotected. Dust positions may block closing or pair changes. Full recovery from external trading or testnet resets is not automated. Model probabilities are not probabilities of profit.

## API

All `/api/*` endpoints require `Authorization: Bearer $CONTROL_TOKEN`.

```sh
curl -H "Authorization: Bearer $CONTROL_TOKEN" http://127.0.0.1:8080/api/status
curl -H "Authorization: Bearer $CONTROL_TOKEN" http://127.0.0.1:8080/api/events
curl -H "Authorization: Bearer $CONTROL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"start"}' http://127.0.0.1:8080/api/control
# pause | {"action":"pair","pair":"ETHUSDT"} | {"action":"close"}
```

`GET /healthz` is unauthenticated liveness. Bind to the LAN with `HTTP_ADDR=<address>:8080`; put TLS and firewall rules in front of it. Never expose the control API publicly.

## Monitoring

Run the exporter separately; it scrapes the authenticated bot endpoint and republishes cached metrics on port 9091.

```sh
BOT_URL=http://127.0.0.1:8080 EXPORTER_ADDR=127.0.0.1:9091 ./trader exporter
```

Scrape the exporter, not the bot API. Add `prometheus.yml.example` to Prometheus and import `grafana-dashboard.json` into Grafana.

Prometheus is monitoring, not the financial ledger. Decisions, intents, and order updates live in the database, and `trader_exporter_up` distinguishes process health from upstream availability.

## Storage

`DATABASE_URL` selects PostgreSQL; otherwise SQLite is used. Both share the `Repository` interface and atomically persist state with an audit event per write. PostgreSQL applies embedded migrations and holds an advisory lock so only one bot uses a database.

Switching databases does not migrate data. Stop the bot, export and verify data, reconcile with the exchange, then resume manually.

## Podman Compose

```sh
cp .env.example .env   # set CONTROL_TOKEN, METRICS_TOKEN, POSTGRES_PASSWORD, OLLAYA_URL
chmod 600 .env
podman compose up --build -d
```

Runs bot, PostgreSQL with persistent storage, and exporter. Use `podman compose down` to keep data; `down -v` discards it. If Compose cannot reach Podman, start the socket with `systemctl --user start podman.socket`.

## Backtesting

Rule-only simulation over historical candles. No Ollaya or exchange calls. Signals use preceding candles; fills use the next candle open with adverse slippage and fees. It does not replay five-second live behaviour, spreads, or latency.

```sh
./trader backtest --input history.json > result.json
```

Or `POST` the same JSON to `/api/backtest`. Input is 61–50,000 consecutive one-minute candles plus optional `budget`, `max_position`, `risk_per_trade`, `daily_loss_limit`, `fee_bps`, `slippage_bps`, `quantity_step`, and `min_notional`. Output includes trades, equity curve, P&L, fees, win rate, and drawdown.

## Tests

```sh
go test -race ./...
TEST_DATABASE_URL='postgres://test:password@localhost:5432/disposable_test?sslmode=disable' go test -race ./...
```

PostgreSQL tests write to the target database, so point them at a disposable one.

## Not implemented

Futures, production trading, exchange-native stops, Ollaya-based historical simulation, automatic data migration, audit retention, and alert rules.