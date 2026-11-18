"""Concurrent 4h OHLC webhook receiver with a durable Telegram alert outbox."""

import asyncio
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mc-bot")

DATABASE_PATH = os.getenv("DATABASE_PATH", "mc_bot.sqlite3")
BIAS_CANDLES = max(2, int(os.getenv("BIAS_CANDLES", "18")))
POLL_SECONDS = max(0.1, float(os.getenv("WORKER_POLL_SECONDS", "1")))
FOUR_HOURS = 4 * 60 * 60


class Candle(BaseModel):
    symbol: str = Field(min_length=1, max_length=40, pattern=r"^[A-Za-z0-9_./:-]+$")
    timestamp: datetime  # OPEN time of a completed 4h candle; must contain timezone
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    timeframe: Literal["4h"] = "4h"

    @model_validator(mode="after")
    def validate_candle(self):
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include a timezone (e.g. Z)")
        if self.low > self.high or not self.low <= self.open <= self.high or not self.low <= self.close <= self.high:
            raise ValueError("OHLC values are inconsistent")
        if self.timestamp.timestamp() + FOUR_HOURS > time.time() + 5:
            raise ValueError("only completed 4-hour candles are accepted")
        return self


def connect():
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL, ts INTEGER NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
                PRIMARY KEY(symbol, ts)
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL, ts INTEGER NOT NULL, direction TEXT NOT NULL,
                message TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                UNIQUE(symbol, ts, direction)
            );
        """)


def process_candle(c: Candle) -> dict:
    symbol = c.symbol.upper()
    ts = int(c.timestamp.astimezone(timezone.utc).timestamp())
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        latest = conn.execute("SELECT MAX(ts) FROM candles WHERE symbol=?", (symbol,)).fetchone()[0]
        if latest is not None and ts < latest:
            return {"status": "stale_ignored", "symbol": symbol, "mc": None}
        previous = conn.execute(
            "SELECT * FROM candles WHERE symbol=? AND ts<? ORDER BY ts DESC LIMIT 1", (symbol, ts)
        ).fetchone()
        # Exclude the prior candle from the 3-day bias window to prevent signal leakage.
        history = []
        if previous:
            history = conn.execute(
                "SELECT * FROM candles WHERE symbol=? AND ts<=? ORDER BY ts DESC LIMIT ?",
                (symbol, previous["ts"] - FOUR_HOURS, BIAS_CANDLES),
            ).fetchall()
        existing = conn.execute("SELECT 1 FROM candles WHERE symbol=? AND ts=?", (symbol, ts)).fetchone()
        if existing:
            return {"status": "duplicate_ignored", "symbol": symbol, "mc": None}
        # A gap invalidates a two-candle pattern and a 3-day trend window.
        contiguous = bool(previous and previous["ts"] == ts - FOUR_HOURS)
        ordered = list(reversed(history))
        complete_bias = (len(ordered) == BIAS_CANDLES and
                         all(ordered[i]["ts"] - ordered[i-1]["ts"] == FOUR_HOURS
                             for i in range(1, len(ordered))))
        bias = "insufficient_history"
        if contiguous and complete_bias:
            # Compare closing prices across the 18 candles before the previous candle.
            first, last = ordered[0]["close"], ordered[-1]["close"]
            bias = "bullish" if last > first else "bearish" if last < first else "neutral"
        direction = None
        if contiguous and bias == "bullish" and c.low < previous["low"] and c.close > previous["close"]:
            direction = "bullish"
        if contiguous and bias == "bearish" and c.high > previous["high"] and c.close < previous["close"]:
            direction = "bearish"
        conn.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?)",
                     (symbol, ts, c.open, c.high, c.low, c.close))
        if direction:
            time_label = c.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            msg = (f"🚨 {direction.upper()} MC candle | {symbol} | 4H\n"
                   f"Candle open: {time_label}\n"
                   f"O {c.open:g}  H {c.high:g}  L {c.low:g}  C {c.close:g}\n"
                   f"Previous close: {previous['close']:g} | 3-day bias: {bias}")
            conn.execute("INSERT OR IGNORE INTO alerts(symbol, ts, direction, message) VALUES(?,?,?,?)",
                         (symbol, ts, direction, msg))
        return {"status": "accepted", "symbol": symbol, "bias": bias,
                "mc": direction, "alert_queued": bool(direction),
                "note": None if contiguous or previous is None else "prior 4h candle missing"}


def pending_alerts():
    with connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM alerts WHERE status='pending' AND next_attempt<=? ORDER BY id LIMIT 25",
            (time.time(),)
        )]


def mark_sent(alert_id: int):
    with connect() as conn:
        conn.execute("UPDATE alerts SET status='sent' WHERE id=?", (alert_id,))


def mark_retry(alert_id: int, attempts: int):
    with connect() as conn:
        conn.execute("UPDATE alerts SET attempts=?, next_attempt=? WHERE id=?",
                     (attempts, time.time() + min(300, 2 ** min(attempts, 8)), alert_id))


async def alert_worker():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
                for alert in await asyncio.to_thread(pending_alerts):
                    if not token or not chat_id:
                        log.warning("Telegram credentials missing; alert %s remains queued", alert["id"])
                        await asyncio.sleep(10)
                        break
                    try:
                        response = await client.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat_id, "text": alert["message"]})
                        response.raise_for_status()
                        if not response.json().get("ok"):
                            raise RuntimeError("Telegram returned ok=false")
                        await asyncio.to_thread(mark_sent, alert["id"])
                    except Exception as exc:
                        log.warning("Telegram alert %s failed: %s", alert["id"], exc)
                        await asyncio.to_thread(mark_retry, alert["id"], alert["attempts"] + 1)
                await asyncio.sleep(POLL_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Alert worker error")
                await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(init_db)
    worker = asyncio.create_task(alert_worker())
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


app = FastAPI(title="MC Candle Webhook Bot", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(candle: Candle, x_webhook_secret: str | None = Header(default=None),
                  key: str | None = Query(default=None)):
    import secrets
    expected = os.getenv("WEBHOOK_SECRET", "")
    supplied = x_webhook_secret or key
    if not expected or not supplied or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    # SQLite work is moved off the event loop; Telegram never runs in this handler.
    return await asyncio.to_thread(process_candle, candle)
