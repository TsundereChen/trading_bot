# Automated Trader

Experimental multi-pair long-only crypto spot bot for Binance, driven by a local [Ollaya](https://github.com/ollaya-dev/ollaya) decision model (`winnow:e4b`). **Not production-ready, no claim of profitability, live trading is your risk.**

## Run

```sh
go build -o trader .
export CONTROL_TOKEN="$(openssl rand -hex 24)" METRICS_TOKEN="$(openssl rand -hex 24)"
export OLLAYA_URL="http://$OLLAYA_HOST:11435" TRADING_PAIRS=BTCUSDT,ETHUSDT
./trader
```

Startup is paused and no frontend exists; control it over HTTP. See `.env.example` for all settings.

| Setting | Effect |
|---|---|
| `BINANCE_BASE_URL` unset | Binance Spot **testnet** (default) |
| `BINANCE_BASE_URL=https://api.binance.com` | Binance Spot **live**, real funds |
| `ENABLE_TRADING=true` | The only switch that permits orders, on either venue |
| `BINANCE_API_KEY` / `_SECRET` | Required when trading is enabled |
| `DATABASE_URL` unset | SQLite; set it for PostgreSQL |

## Pairs

`TRADING_PAIRS` takes up to 8 USDT spot pairs. Each is independent: cash, position, stop, and daily-loss baseline of its own (`PER_PAIR_BUDGET` each). Inference runs one request at a time, so a pair's decision interval is **5s × pair count**; exits and reconciliation still run every second per pair.

## Strategy and limits

Trend-following pullbacks on completed 1-minute candles: EMA20/50 trend, EMA20 pullback recovery, ATR/RSI/volume context. Long-only, no leverage, shorts, pyramiding, or averaging down. Per pair, sizing is the minimum of max position value, risk ÷ 1.5×ATR, and available cash. Local ATR stop (1.5×) and target (3×) exit independently of the model. Order intents persist before submission; uncertain outcomes block and reconcile by client ID.

**Limits:** exits are not exchange-native, so outages are unprotected. Dust may block closing. Model probabilities are not probabilities of profit.

## API

All `/api/*` need `Authorization: Bearer $CONTROL_TOKEN`. Omitting `pair` applies to every pair.

```sh
curl -H "Authorization: Bearer $CONTROL_TOKEN" localhost:8080/api/status
curl -H "Authorization: Bearer $CONTROL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"start","pair":"BTCUSDT"}' localhost:8080/api/control
# pause | close | {"action":"pairs","pairs":[...]}
```

Keep it off the public internet; use `HTTP_ADDR` and TLS plus firewall rules for LAN access.

## Monitoring, containers, backtesting

```sh
BOT_URL=http://127.0.0.1:8080 ./trader exporter   # Prometheus scrapes 9091, not the API
./trader backtest --input history.json             # or POST to /api/backtest
cp .env.example .env && chmod 600 .env && podman compose up --build -d
```

Compose runs bot, PostgreSQL, and exporter. Import `grafana-dashboard.json`; metrics carry a `pair` label. Backtests are rule-only over historical candles with slippage and fees, and do not replay live timing or spreads.

## Tests

```sh
go test -race ./...
TEST_DATABASE_URL='postgres://test:password@localhost:5432/disposable_test?sslmode=disable' go test -race ./...
```

Not implemented: futures, exchange-native stops, Ollaya-based backtests, automatic data migration, audit retention, alerting.