# MC Candle Telegram bot

Receives completed 4-hour OHLC candles via a concurrent FastAPI webhook, stores
them in SQLite (WAL mode), detects MC patterns, and queues Telegram alerts in a
durable outbox. HTTP requests never wait for Telegram.

## Strategy implemented

- **3-day bias:** 18 contiguous 4-hour candles **before the previous candle**.
  Compare the oldest and newest closes in that window: higher = bullish, lower =
  bearish, equal = neutral. The previous candle is excluded to avoid overlapping
  the MC pattern's comparison candle.
- **Bullish MC:** bullish bias, current low < previous low, current close >
  previous close.
- **Bearish MC:** bearish bias, current high > previous high, current close <
  previous close.
- Strict comparisons; candle body color and true full-range engulfing are **not**
  required by these rules. No signal until enough consecutive history arrives.
- A candle's `timestamp` is its **open** time; send it only after that 4-hour
  bar has closed. Timestamps must contain a timezone. Each symbol is independent.

## Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env: WEBHOOK_SECRET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
uvicorn app:app --host 0.0.0.0 --port 8000
```

Send the last 19+ historical 4-hour candles **oldest first** per symbol to warm up
the bias, then send each completed candle on the 4-hour schedule. Example:

```bash
curl -X POST http://localhost:8000/webhook \
  -H 'Content-Type: application/json' \
  -H 'X-Webhook-Secret: replace-with-long-random-secret' \
  -d '{"symbol":"EURUSD","timestamp":"2026-11-17T16:00:00Z","timeframe":"4h","open":1.10,"high":1.12,"low":1.09,"close":1.11}'
```

The response includes `bias`, `mc` (`bullish`, `bearish` or `null`), and
`alert_queued`. Duplicate timestamp/symbol pairs are ignored. Older candles
arriving after a newer one are ignored (send historical backfill in order).

## TradingView Pine Script indicator + webhook

Open **tradingview_mc_indicator.pine**, paste it into TradingView's Pine Editor,
save and add it to a **4-hour chart**. It displays `MC ▲`/`MC ▼` on qualifying
candles. It uses exactly the same 18-candle first-versus-last close bias and
the specified wick/close conditions as the Python app. Both require consecutive
four-hour bars, so markets closed over weekends will need a session-aware
variant if you want to evaluate signals immediately after market reopening.

1. Deploy Python at an HTTPS URL (e.g. `https://example.com`).
2. In TradingView, create an alert on your indicator and select
   **Any alert() function call**; set the webhook URL to
   `https://example.com/webhook?key=YOUR_WEBHOOK_SECRET`.
3. Select the appropriate expiration and activate the alert. Repeat for each
   symbol's 4-hour chart. The indicator sends **every completed 4-hour OHLC**,
   not only MC patterns: the Python server requires the earlier candles to
   reconstruct its own 3-day bias and trigger Telegram signals.
4. TradingView alert creation does not send historical bars retroactively.
   Backfill at least **19 earlier 4h candles** for each symbol via the webhook
   (oldest first), or wait for that history to accumulate after enabling alerts.

TradingView does not allow arbitrary custom HTTP authentication headers on
webhook alerts, so the Python receiver also accepts `?key=` as an alternative
to the `X-Webhook-Secret` header. **Treat this URL as a secret**; only use HTTPS,
avoid logging query strings, and rotate the secret if the URL is exposed.
TradingView only supports webhook destination ports 80 and 443 and may cancel
requests taking over 3 seconds. Put a public HTTPS reverse proxy ahead of
the Python app; port 8000 in the local example is not a valid TradingView
public webhook destination.

## Reliability notes

- Parallel requests are handled asynchronously; short SQLite transactions run
  in background threads. Telegram sends and retries run separately.
- Alerts survive process restarts. Telegram failures retry with exponential
  backoff. A process crash between Telegram delivery and marking a message sent
  **can** cause a duplicate Telegram message (at-least-once delivery).
- Run **one worker process** (`uvicorn` default); multiple worker processes
  would each try to send pending alerts. For a larger multi-instance deployment,
  switch the outbox to an atomic claim-and-lease queue such as PostgreSQL.
- Use HTTPS and keep `WEBHOOK_SECRET` private in production. No broker or
  exchange connection is required, and the bot never places trades.

## Verify

```bash
python -m unittest discover -s tests -v
```
