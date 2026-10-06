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

Set `OLLAYA_MODEL` to select the Ollaya decision model (default `winnow:e4b`).
The model must support Ollaya's `/api/decide` structured choice/probability
contract; arbitrary chat models are not interchangeable. Responses naming a
different model are rejected. Restart the bot after changing configuration.

| Setting | Effect |
|---|---|
| `BINANCE_BASE_URL` unset or `https://demo-api.binance.com` | Binance Spot **Demo Mode**, virtual funds (default) |
| `BINANCE_BASE_URL=https://api.binance.com` | Binance Spot **live**, real funds |
| `ENABLE_TRADING=true` | The only switch that permits orders, on either venue |
| `BINANCE_API_KEY` / `_SECRET` | Required when trading is enabled |
| `DATABASE_URL` unset | SQLite; set it for PostgreSQL |

SQLite permits one bot owner per database on Unix; use a filesystem `DATABASE_PATH`, not a SQLite URI/DSN. Use PostgreSQL on other platforms; it also enforces a singleton lease. Do not run independent databases/bots against the same trading allocation: these leases protect a database, not the entire Binance account.

### State identity and existing databases

State is bound to the venue and, when execution is enabled, the Binance account UID returned by `/api/v3/account`. Known mismatches stop startup; use separate databases for demo/live and different accounts. Missing account UIDs fail closed. Only Demo Mode and live mode are supported. Persisted `paper` state belongs to the previously used Spot Testnet and cannot be adopted as Demo Mode: use a separate database. PostgreSQL users must also use a separate database when switching venues.

Older databases have no identity metadata and are **not automatically adopted**. Back up the database, verify its original venue and account, then set `STATE_BINDING_CONFIRM=demo:<original Binance UID>` or `live:<original Binance UID>` for the first verified trading startup. Remove the setting afterward. This attestation never overrides a known mismatch and does not reconcile unknown orders or balances for you. For a legacy observe-only database with no account, `demo:`/`live:` binds only the venue; existing exposure still needs account verification before execution. Do not attest old Testnet state as Demo Mode.

### Paper trading with Demo Mode

Create a **Demo Mode** API key and secret at [Binance Demo API Management](https://demo.binance.com/en/my/settings/api-management), not in live-account or Spot Testnet API management. Set `BINANCE_API_KEY`, `BINANCE_API_SECRET`, and `ENABLE_TRADING=true`; leave `BINANCE_BASE_URL` unset (or use `https://demo-api.binance.com/api`). Startup remains paused: use `/api/control` to start after feeds and account reconciliation are ready. With `ENABLE_TRADING=false`, the bot only observes and does not submit virtual orders.

Per [Binance's Demo Mode documentation](https://developers.binance.com/en/docs/products/spot/demo-mode/general-info), the Spot `/api/v3` order/account endpoints and signing rules remain the same; only the service hosts change. REST uses `demo-api.binance.com`, and combined market streams use `demo-stream.binance.com/stream`. The `demo-ws-api` service is a separate request/response API and is not the market-stream endpoint. Demo Mode uses virtual balances and realistic—but not identical to live—market data. Balance resets and maintenance require reconciliation; do not reset balances while the bot has tracked exposure.

Live API keys must allow reading and spot trading, restrict access by IP, and disallow withdrawals, internal/universal transfers, margin, futures, and options. The bot verifies actual key permissions via `/sapi/v1/account/apiRestrictions`, not account-level withdrawal capability. Keys and their exchange permissions are never changed by the bot.

## Pairs

`TRADING_PAIRS` takes up to 8 alphanumeric USDT/USDC spot pairs. Each is independent: cash, position, stop, and daily-loss baseline of its own (`PER_PAIR_BUDGET` each, in that pair's quote currency). Every `DECISION_INTERVAL_SECONDS` (default 30), a round evaluates **all pairs sequentially in configured order**, with no interval between pairs. Retained pairs outside the configuration are evaluated afterward in alphabetical order. Requests and rounds never overlap. If a round takes longer than the interval, the next starts after it finishes, without queued catch-up rounds; actual per-pair cadence then exceeds 30 seconds. `OLLAYA_TIMEOUT_SECONDS` defaults to 20 per request and must be less than the round interval. The candle timeframe does not throttle requests. Stale quotes, invalid candles, and unresolved orders can skip an evaluation; exchange settlement can delay subsequent pairs. Status reports `model_cadence=sequential_all_pairs` and `model_round_interval_seconds`; `decision_cycle_seconds` is the configured round interval, not multiplied by the pair count. Exits and reconciliation are dispatched every five seconds per pair independently of inference. A monitor heartbeat is logged every 30 seconds.

## Strategy and limits

Trend-following pullbacks on completed candles (`SIGNAL_INTERVAL=1m`, or `5m`): EMA20/50 trend, EMA20 pullback recovery, ATR14/RSI/volume context on the same timeframe. Long-only, no leverage, shorts, pyramiding, or averaging down. Per pair, sizing is the minimum of max position value, risk ÷ entry-to-stop distance (including spread), and available allocated cash. After a buy is filled and its complete fill history is reconciled, a native Binance `STOP_LOSS` sell protects the sellable quantity at the ATR stop (1.5×). Targets (3× ATR, twice the planned stop distance), trend reversals, and daily-loss exits remain local. A consumed pullback cannot be reused for re-entry; a new eligible candle is required.

For the earlier five-minute pullback Demo trial, use `SIGNAL_INTERVAL=5m`,
`PER_PAIR_BUDGET=4000`, `MAX_POSITION_QUOTE=4000`,
`RISK_PER_TRADE_QUOTE=10`, and `DAILY_LOSS_LIMIT_QUOTE=400`.
These are quote-USDT limits, not a guarantee of realized loss. Fees, slippage,
and gaps can exceed the planned stop risk. Changing the budget does not rewrite
persisted cash allocations; start a separate trial database only after all old
tracked exposure and orders are resolved. Historical backtest input accepts
`"signal_interval":"5m"`; its default remains `1m` and it excludes Ollaya.

### Small, model-led Demo trades

The active small-trade trial and new deployment defaults use this profile:

```sh
TRADING_PAIRS=BTCUSDT,BTCUSDC,ETHUSDT,ETHUSDC
SIGNAL_INTERVAL=1m
CONTEXT_INTERVAL=5m
ENTRY_POLICY=ollaya
DECISION_INTERVAL_SECONDS=30
OLLAYA_TIMEOUT_SECONDS=20
REENTRY_COOLDOWN_SECONDS=10
MAX_POSITION_QUOTE=20
RISK_PER_TRADE_QUOTE=0.10
DAILY_LOSS_LIMIT_QUOTE=400
```

`ENTRY_POLICY=ollaya` allows the model to choose entries without a mandatory
EMA trend/pullback, and exits without a mandatory trend reversal. Balance,
spread, pause, cooldown, sizing, exchange minimums, unresolved-order and fee
checks still apply. Only one active position per pair is allowed. The optional
`pullback` policy still requires a new technical signal for re-entry.
The stop/target remain 1.5×/3× ATR14. More model evaluations do not guarantee
more trades or profit; short trades may lose money after spread, fees and slippage.
The prompt includes a provisional 20-bps round-trip fee estimate, not a verified
account fee schedule. Daily limits are per pair, not one shared portfolio limit.

Proven sub-step residuals are retained as `dust_quantity`/`dust_cost`, not written
off. They remain unprotected, are included in equity and position limits, and
are absorbed into the next filled buy before protecting the combined position.
Pairs with tracked dust cannot be silently removed. Larger unsellable residuals
remain active and can still block closing. Existing allocations and day-start
equity are preserved; changing configuration does not reset losses.

Third-asset fees are valued in each pair's actual quote currency. USDT and USDC
are not assumed interchangeable: per-pair metrics remain available, while
aggregate portfolio equity/exposure is NaN when quotes are mixed because no
cross-currency valuation is implemented.

Native stops survive bot/feed outages. Their intents persist before submission; manual/model/local exits cancel and reconcile the stop before placing a market sell, including cancellation/fill races. If native placement is definitely rejected, the bot attempts an emergency close. Uncertain outcomes retain their intents and never blindly resubmit. Markets without native `STOP_LOSS` support cannot be used for execution.

Fee accounting verifies cumulative fill quantity and quote totals before finalizing an order and paginates large fill histories. BNB/other third-asset fees are tracked in asset units and valued conservatively at the available ask in the pair's quote currency when reconciled; `external_fee_quote_estimate` is an estimate, not historical fill-time valuation. Unvalued fees block new entries and mark equity unavailable, but do not prevent tracking/protecting the filled principal.

**Limits:** there is still a gap between entry execution and native-stop acceptance, especially after uncertain submissions or delayed fill history. Exchange outages/rejections and market gaps can prevent or worsen exits. Native stops do not guarantee a fill price, and rounded dust remains unprotected and may block closing/removal. Targets and daily limits cannot execute locally while the bot is down. Model probabilities are not probabilities of profit. Exercise the full lifecycle in Demo Mode before considering live funds.

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

### Signal windows

The default model input combines **1-minute signals with 5-minute context**.
`SIGNAL_INTERVAL=1m` drives short-term features and ATR-based stop/target sizing;
`CONTEXT_INTERVAL=5m` supplies a separate EMA/trend/volatility feature set to
Ollaya as `market_context`. Each window retains up to 100 completed candles
(roughly 100 minutes and 500 minutes of history), with independent REST
bootstrap, stream updates, and freshness validation. Unfinished 5-minute bars
never influence the context. Context informs Ollaya's judgment, not a mandatory
trend filter; a higher-timeframe downtrend does not mechanically block entry.

Missing, invalid, or stale context skips that pair's model evaluation with
`context_unavailable`; a context bar changing during inference blocks a new
entry with `context_changed`. Independent reconciliation, native stops, and
protective exits continue. `trader_context_ready`,
`trader_context_candle_age_seconds`, status `context_interval`/`context_close_time`,
and the heartbeat expose context freshness. Rounds still evaluate every pair
sequentially every 30 seconds; context updates only on completed 5-minute bars.

Compose starts **bot, PostgreSQL, and exporter together**. The exporter uses
the same image with the `exporter` command; the bot serves its authenticated
source at `/internal/metrics`, and the exporter exposes a cached Prometheus
scrape endpoint on port 9091. Generate **different** `CONTROL_TOKEN` and
`METRICS_TOKEN` secrets; both are required, and the exporter receives only the
metrics credential.

The exporter `/metrics` endpoint contains the **same trading and process metrics**
as the bot's authenticated `/internal/metrics`, plus exporter availability,
snapshot age, and fetch-error counters. It is cached, not redacted: it includes
pair equity/exposure, P&L, fees, trade statistics, positions/stops/targets, model
outcomes, and feed health. It does not expose API keys or authentication tokens.
On a failed fetch or expired snapshot, it serves only exporter health metrics.
`METRICS_TOKEN` authenticates the exporter to the bot; the exporter listener
itself is unauthenticated. Keep it loopback/private or protect external access
with an authenticated proxy. Bot `/api/*` remains control-token-only.

```sh
BOT_URL=http://127.0.0.1:8080 ./trader exporter   # Prometheus scrapes 9091, not the API
./trader backtest --input history.json             # or POST to /api/backtest
cp .env.example .env && chmod 600 .env
# Generate the secrets in .env before running:
podman compose pull
podman compose up -d
```

Compose runs bot, PostgreSQL, and exporter. Import `grafana-dashboard.json`; metrics carry a `pair` label. Backtests are rule-only over historical candles with slippage and fees, and do not replay live timing or spreads.

The dashboard includes realized/unrealized/net P&L, UTC daily P&L, estimated
commissions, completed trades, win rate, average win/loss, sampled maximum
drawdown, entry cost/stop/target, native-stop status, and latest model action,
latency, decision-to-order outcome, and no-trade reasons. Performance values
carry `quote_asset`: aggregate only within USDT or USDC, never across both
without conversion. For example:

```promql
sum by (quote_asset) (trader_net_pnl_quote{job="automated-trader"})
trader_last_no_trade_reason{job="automated-trader"}
trader_entry_block_reason{job="automated-trader"}
```

Performance state survives restarts and audit pruning. Tracking begins at
`trader_performance_since_timestamp_seconds`; inherited positions keep their
original cost basis, but their cycles are excluded from win/loss statistics.
Unrealized P&L includes retained dust. Completed-cycle results include only
realized proceeds (remaining dust is not written off). Zero-profit cycles
count as completed but neither wins nor losses. Win rate/average win or loss
are `NaN` until the relevant samples exist; unavailable quote/fee valuations
are not reported as zero. Base and third-asset commission quote values are
estimates, and fees are already accounted in P&L—do not subtract them again.
Drawdown starts from the first observed fresh valuation for inherited exposure
and is sampled, not an intratick maximum. Order acknowledgement is not proof
of a fill; the status/audit API preserves the linked client ID and uncertainty.
No-trade counters and request/order counters are process-lifetime metrics;
durable trade totals are gauges. Health or winning trades alone do not prove
strategy profitability.

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

After starting Compose, verify the complete metrics path:

```sh
podman compose ps
curl -fsS http://127.0.0.1:9091/metrics
podman compose logs --tail=30 exporter
```

Look for `trader_exporter_up 1`, a recent
`trader_exporter_snapshot_age_seconds`, and `trader_*` metrics for all configured
pairs. `/healthz` checks process liveness only; `trader_exporter_up` indicates
fresh authenticated bot metrics. Failed or expired snapshots are not served as
current trading data. See `prometheus.yml.example` for scrape configuration.
Port 9091 is unauthenticated and binds to host loopback by default. For a remote
Prometheus server, set `EXPORTER_BIND_IP` to a reachable trusted-network address
and restrict access with your firewall; do not expose it publicly.

Follow runtime logs with `podman compose logs -f bot` (or
`podman logs -f trader-demo` for a standalone container named `trader-demo`).
Logs show startup/model configuration, feed connection/retries, each scheduled
model decision with entry eligibility and trend/pullback flags, committed
order/control events, faults, and shutdown. A `HOLD` decision with
`entry_eligible=false` is normal while waiting for the strategy conditions;
a model decision is not an order confirmation. Authenticated `/api/status`
and `/api/events` provide position, pending-order, native-stop, and audit details.
Healthchecks only prove HTTP liveness, not successful trading. Every restart
pauses entries until an explicit start command. Credentials and full signed
exchange request URLs are not included in the new runtime logs.

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
# Optional real Demo Mode public REST/stream check; no keys or orders:
TEST_BINANCE_DEMO_PUBLIC=true go test -run TestDemoPublicConnectivity -v ./internal/trader
```

Not implemented: futures, atomic exchange-native entry/exit brackets or OCO targets, Ollaya-based backtests, automatic database-backend migration, alerting.
