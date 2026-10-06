# Automated Trader

Experimental multi-pair long-only crypto spot bot for Binance, driven by a local [Ollaya](https://github.com/ollaya-dev/ollaya) decision model (`winnow:e4b`). **Not production-ready, no claim of profitability, live trading is your risk.**

## Run

```sh
go build -o trader .
export CONTROL_TOKEN="$(openssl rand -hex 24)" METRICS_TOKEN="$(openssl rand -hex 24)"
export OLLAYA_URL="http://$OLLAYA_HOST:11435" TRADING_PAIRS=BTCUSDT,ETHUSDT
./trader
```

Requires Go 1.26.8 or newer. Startup is paused and no frontend exists; control it over HTTP. See `.env.example` for all settings. Replace example secret placeholders before running; they are rejected.

| Setting | Effect |
|---|---|
| `BINANCE_BASE_URL` unset | Binance Spot **testnet** (default) |
| `BINANCE_BASE_URL=https://api.binance.com` | Binance Spot **live**, real funds |
| `ENABLE_TRADING=true` | The only switch that permits orders, on either venue |
| `BINANCE_API_KEY` / `_SECRET` | Required when trading is enabled |
| `DATABASE_URL` unset | SQLite; set it for PostgreSQL |

SQLite permits one bot owner per database on Unix; use a filesystem `DATABASE_PATH`, not a SQLite URI/DSN. Use PostgreSQL on other platforms; it also enforces a singleton lease. Do not run independent databases/bots against the same trading allocation: these leases protect a database, not the entire Binance account.

### State identity and existing databases

State is bound to the venue and, when execution is enabled, the Binance account UID returned by `/api/v3/account`. Known mismatches stop startup; use separate databases for testnet/live and different accounts. Missing account UIDs fail closed.

Older databases have no identity metadata and are **not automatically adopted**. Back up the database, verify its original venue and account, then set `STATE_BINDING_CONFIRM=paper:<original Binance UID>` (or `live:<original Binance UID>`) for the first verified trading startup. Remove the setting afterward. This attestation never overrides a known mismatch and does not reconcile unknown orders or balances for you. For a legacy observe-only database with no account, `paper:`/`live:` binds only the venue; existing exposure still needs account verification before execution.

Live API keys must allow reading and spot trading, restrict access by IP, and disallow withdrawals, internal/universal transfers, margin, futures, and options. The bot verifies actual key permissions via `/sapi/v1/account/apiRestrictions`, not account-level withdrawal capability. Keys and their exchange permissions are never changed by the bot.

## Pairs

`TRADING_PAIRS` takes up to 8 alphanumeric USDT spot pairs. Each is independent: cash, position, stop, and daily-loss baseline of its own (`PER_PAIR_BUDGET` each). Inference runs one request at a time, so a pair's nominal decision interval is **5s × pair count**; execution latency can stretch it. Exits and reconciliation are dispatched every second per pair, independently of other pairs and inference. Busy pairs do not accumulate overlapping execution workers.

## Strategy and limits

Trend-following pullbacks on completed 1-minute candles: EMA20/50 trend, EMA20 pullback recovery, ATR/RSI/volume context. Long-only, no leverage, shorts, pyramiding, or averaging down. Per pair, sizing is the minimum of max position value, risk ÷ entry-to-stop distance (including spread), and available allocated cash. After a buy is filled and its complete fill history is reconciled, a native Binance `STOP_LOSS` sell protects the sellable quantity at the ATR stop (1.5×). Targets (3×), trend reversals, and daily-loss exits remain local.

Native stops survive bot/feed outages. Their intents persist before submission; manual/model/local exits cancel and reconcile the stop before placing a market sell, including cancellation/fill races. If native placement is definitely rejected, the bot attempts an emergency close. Uncertain outcomes retain their intents and never blindly resubmit. Markets without native `STOP_LOSS` support cannot be used for execution.

Fee accounting verifies cumulative fill quantity and quote totals before finalizing an order and paginates large fill histories. BNB/other third-asset fees are tracked in asset units and valued conservatively at the available USDT ask when reconciled; `external_fee_quote_estimate` is an estimate, not historical fill-time valuation. Unvalued fees block new entries and mark equity unavailable, but do not prevent tracking/protecting the filled principal.

**Limits:** there is still a gap between entry execution and native-stop acceptance, especially after uncertain submissions or delayed fill history. Exchange outages/rejections and market gaps can prevent or worsen exits. Native stops do not guarantee a fill price, and rounded dust remains unprotected and may block closing/removal. Targets and daily limits cannot execute locally while the bot is down. Model probabilities are not probabilities of profit. Exercise the full lifecycle on testnet before considering live funds.

## API

All `/api/*` need `Authorization: Bearer $CONTROL_TOKEN`. Omitting `pair` applies to every pair.

```sh
curl -H "Authorization: Bearer $CONTROL_TOKEN" localhost:8080/api/status
curl -H "Authorization: Bearer $CONTROL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"start","pair":"BTCUSDT"}' localhost:8080/api/control
# pause | close | {"action":"pairs","pairs":[...]}
```

Keep it off the public internet; use `HTTP_ADDR` and TLS plus firewall rules for LAN access.

Starting multiple pairs is all-or-nothing after revalidation. Closing multiple pairs pauses them together, then reports per-pair results; exchange orders themselves cannot be a transactional batch. HTTP 409 can indicate a busy, unresolved, or residual/dust position. Pause does not cancel native protection. Only one API backtest runs at a time.

## Monitoring, containers, backtesting

```sh
BOT_URL=http://127.0.0.1:8080 ./trader exporter   # Prometheus scrapes 9091, not the API
./trader backtest --input history.json             # or POST to /api/backtest
cp .env.example .env && chmod 600 .env
# Generate the secrets in .env before running:
podman compose pull
podman compose up -d
```

Compose runs bot, PostgreSQL, and exporter. Import `grafana-dashboard.json`; metrics carry a `pair` label. Backtests are rule-only over historical candles with slippage and fees, and do not replay live timing or spreads.

CI builds and publishes `ghcr.io/tsunderechen/trading_bot` after the Go checks
pass on `main` (pushes or manual runs). Images are tagged `latest` and
`sha-<full commit SHA>`; pull-request builds do not log in or publish.
Publishing uses the workflow's `GITHUB_TOKEN` with `packages: write`, so no
additional registry secret is required.

Compose pulls the GHCR image for both bot and exporter instead of building
locally. Override `TRADER_IMAGE` in `.env` to pin a commit tag or image digest.
The image must be published before the first deployment. For a private GHCR
package, run `podman login ghcr.io` with a token permitted to read the package;
otherwise make the package public in GitHub's package settings for anonymous
pulls. The workflow does not change package visibility.

Generate the control/metrics secrets and PostgreSQL password in `.env` before starting Compose. Audit retention defaults to 30 days and 100,000 events (`AUDIT_RETENTION_DAYS`, `AUDIT_MAX_EVENTS`). Pruning runs in batches of at most 1,000 rows each minute, so catch-up is gradual; current positions and order/native-stop intents are never pruned. Export audit records separately if longer retention is required. Database files may retain their high-water size until offline maintenance.

Disconnected/stale held positions produce `NaN` equity/exposure instead of disappearing from portfolio totals. Missing quotes have infinite age; removed pairs lose their metrics series. Third-asset fee units are exposed separately. Gate alerts on feed freshness and exporter availability rather than treating missing valuations as zero.

## Source layout

This is a single-command Go module. The root `main.go` only dispatches the bot,
exporter, backtest, and healthcheck commands; `go build -o trader .` and the
container entry point remain unchanged.

```text
main.go                         CLI entry point
internal/
  exporter/                     Isolated Prometheus metrics proxy
  strategy/                     Candle validation, indicators, decimal rules,
                                and rule-only backtests
  trader/
    app.go, state.go            State ownership and durable publication
    config.go, run.go           Configuration and startup/shutdown
    http.go, control.go         Authenticated HTTP API
    engine.go, poll.go          Scheduling and independent pair workers
    decision.go, ollaya.go      Decision policy and model client
    execution.go                Order preflight and submission
    reconciliation.go           Fill and fee accounting
    protection.go               Native-stop lifecycle
    market.go, binance.go        Market feeds and exchange adapter
    metrics.go                  Bot metrics and freshness
    repository.go               Persistence contract
    sqlite.go, postgres.go      Database adapters
    lease_*.go                  Platform-specific SQLite ownership
    migrations/postgres/        Embedded PostgreSQL migrations
```

Tests live beside the packages they exercise. The trading runtime deliberately
keeps state, database adapters, and execution in one package so its locking and
transaction details do not become a cross-package API. Strategy calculations
are independent of the trading runtime; the exporter has no access to control
or execution internals. Application-only packages stay under `internal/` rather
than exposing a public library or introducing generic `utils` packages.

## Tests

```sh
go test -race ./...
TEST_DATABASE_URL='postgres://test:password@localhost:5432/disposable_test?sslmode=disable' go test -race ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
```

Not implemented: futures, atomic exchange-native entry/exit brackets or OCO targets, Ollaya-based backtests, automatic database-backend migration, alerting.
