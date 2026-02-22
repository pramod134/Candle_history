"""
main.py

Alpaca -> Supabase candle ingestion (RTH-only) with two modes:

1) MODE=backfill
   - Fetches 1-minute candles from "now" back to YEARS_BACK (default 2)
   - Stores ONLY RTH minutes (09:30–16:00 ET, DST-aware)
   - AUTO-DISABLE: If DB already contains >= YEARS_BACK of history (earliest_ts <= cutoff),
     it skips backfill and exits.
   - Updates ingest_state.last_ts to the newest candle in DB.

2) MODE=live
   - Runs continuously
   - Polls Alpaca for new 1m candles since last cursor
   - Stores ONLY RTH minutes
   - Cursor is persisted in ingest_state so restarts are safe.

Required env vars:
- APCA_API_KEY_ID
- APCA_API_SECRET_KEY
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY   (recommended) or SUPABASE_KEY

Recommended tables (Supabase SQL):

create table if not exists candle_history (
  symbol text not null,
  ts timestamptz not null,
  open double precision not null,
  high double precision not null,
  low double precision not null,
  close double precision not null,
  volume double precision not null,
  vwap double precision,
  trade_count integer,
  primary key (symbol, ts)
);
create index if not exists idx_candle_history_symbol_ts
on candle_history (symbol, ts);

create table if not exists ingest_state (
  symbol text primary key,
  last_ts timestamptz,
  updated_at timestamptz not null default now()
);

Optional env vars:
- MODE=backfill|live
- SYMBOL=SPY
- TABLE_NAME=candle_history
- STATE_TABLE=ingest_state
- YEARS_BACK=2
- LIMIT=4000
- ALPACA_FEED=iex
- TIMEFRAME=1Min
- ADJUSTMENT=raw
- API_SLEEP_SECONDS=0.35
- UPSERT_CHUNK=500
- POLL_SECONDS=20
- LIVE_LOOKBACK_MIN=120
- VERIFY_AFTER_BACKFILL=1 (set 0 to skip)
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client, Client
from dateutil import tz

# =============================
# ENV CONFIG
# =============================
ALPACA_KEY = os.environ.get("APCA_API_KEY_ID")
ALPACA_SECRET = os.environ.get("APCA_API_SECRET_KEY")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")

SYMBOL = os.environ.get("SYMBOL", "SPY")
TABLE_CANDLES = os.environ.get("TABLE_NAME", "candle_history")
TABLE_STATE = os.environ.get("STATE_TABLE", "ingest_state")

MODE = os.environ.get("MODE", "backfill").lower()  # backfill | live

ALPACA_FEED = os.environ.get("ALPACA_FEED", "iex")
TIMEFRAME = os.environ.get("TIMEFRAME", "1Min")
ADJUSTMENT = os.environ.get("ADJUSTMENT", "raw")

YEARS_BACK = float(os.environ.get("YEARS_BACK", "2"))
LIMIT = int(os.environ.get("LIMIT", "4000"))

API_SLEEP_SECONDS = float(os.environ.get("API_SLEEP_SECONDS", "0.35"))
UPSERT_CHUNK = int(os.environ.get("UPSERT_CHUNK", "500"))

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "20"))
LIVE_LOOKBACK_MIN = int(os.environ.get("LIVE_LOOKBACK_MIN", "120"))

VERIFY_AFTER_BACKFILL = os.environ.get("VERIFY_AFTER_BACKFILL", "1") == "1"

ALPACA_BARS_URL = f"https://data.alpaca.markets/v2/stocks/{SYMBOL}/bars"

ET = tz.gettz("America/New_York")


# =============================
# HELPERS
# =============================
def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def chunked(rows: List[Dict[str, Any]], n: int):
    for i in range(0, len(rows), n):
        yield rows[i : i + n]


def is_rth_1m(ts_utc: datetime) -> bool:
    """
    RTH minutes only: 09:30 <= time < 16:00 ET (DST-aware), Mon-Fri.
    """
    ts_et = ts_utc.astimezone(ET)
    if ts_et.weekday() >= 5:
        return False

    t = ts_et.time()
    # inclusive 09:30
    if t < datetime(2000, 1, 1, 9, 30).time():
        return False
    # exclusive 16:00
    if t >= datetime(2000, 1, 1, 16, 0).time():
        return False
    return True


def alpaca_get_bars(
    end_ts_utc: Optional[datetime] = None,
    start_ts_utc: Optional[datetime] = None,
    limit: int = 4000,
) -> List[Dict[str, Any]]:
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    params = {
        "timeframe": TIMEFRAME,
        "limit": limit,
        "adjustment": ADJUSTMENT,
        "feed": ALPACA_FEED,
    }
    if start_ts_utc is not None:
        params["start"] = iso_z(start_ts_utc)
    if end_ts_utc is not None:
        params["end"] = iso_z(end_ts_utc)

    r = requests.get(ALPACA_BARS_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return (r.json() or {}).get("bars", []) or []


def normalize_rows(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Alpaca bar fields:
      t (ts), o, h, l, c, v, vw, n
    """
    out: List[Dict[str, Any]] = []
    for b in bars:
        out.append(
            {
                "symbol": SYMBOL,
                "ts": b["t"],
                "open": float(b["o"]),
                "high": float(b["h"]),
                "low": float(b["l"]),
                "close": float(b["c"]),
                "volume": float(b.get("v") or 0),
                "vwap": float(b.get("vw") or 0) if b.get("vw") is not None else None,
                "trade_count": int(b.get("n") or 0),
            }
        )
    return out


def upsert_candles(sb: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    for batch in chunked(rows, UPSERT_CHUNK):
        sb.table(TABLE_CANDLES).upsert(batch, on_conflict="symbol,ts").execute()


def get_db_min_ts(sb: Client) -> Optional[str]:
    resp = (
        sb.table(TABLE_CANDLES)
        .select("ts")
        .eq("symbol", SYMBOL)
        .order("ts", desc=False)
        .limit(1)
        .execute()
    )
    return resp.data[0]["ts"] if resp.data else None


def get_db_max_ts(sb: Client) -> Optional[str]:
    resp = (
        sb.table(TABLE_CANDLES)
        .select("ts")
        .eq("symbol", SYMBOL)
        .order("ts", desc=True)
        .limit(1)
        .execute()
    )
    return resp.data[0]["ts"] if resp.data else None


def get_state_last_ts(sb: Client) -> Optional[str]:
    resp = sb.table(TABLE_STATE).select("last_ts").eq("symbol", SYMBOL).limit(1).execute()
    if resp.data and resp.data[0].get("last_ts"):
        return resp.data[0]["last_ts"]
    return None


def set_state_last_ts(sb: Client, last_ts: Optional[str]) -> None:
    payload = {
        "symbol": SYMBOL,
        "last_ts": last_ts,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    sb.table(TABLE_STATE).upsert(payload, on_conflict="symbol").execute()


def verify_quick(sb: Client, cutoff_utc: datetime, now_utc: datetime) -> None:
    """
    Lightweight verification:
    - min/max ts
    - row count in range (exact count)
    """
    start_ts = iso_z(cutoff_utc)
    end_ts = iso_z(now_utc)

    print("\n=== VERIFY (quick) ===")
    min_ts = get_db_min_ts(sb)
    max_ts = get_db_max_ts(sb)
    print(f"[VERIFY] DB min_ts: {min_ts}")
    print(f"[VERIFY] DB max_ts: {max_ts}")

    resp = (
        sb.table(TABLE_CANDLES)
        .select("ts", count="exact")
        .eq("symbol", SYMBOL)
        .gte("ts", start_ts)
        .lte("ts", end_ts)
        .execute()
    )
    print(f"[VERIFY] count in [{start_ts} .. {end_ts}] = {resp.count}")


# =============================
# BACKFILL
# =============================
def run_backfill(sb: Client) -> None:
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(days=int(365 * YEARS_BACK))

    print(f"[BACKFILL] symbol={SYMBOL} tf={TIMEFRAME} feed={ALPACA_FEED}")
    print(f"[BACKFILL] target range: {iso_z(now_utc)} back to {iso_z(cutoff_utc)}")
    print(f"[BACKFILL] writing RTH-only to: {TABLE_CANDLES}")

    # --- AUTO-DISABLE if we already have >= YEARS_BACK of history ---
    existing_min_ts = get_db_min_ts(sb)
    if existing_min_ts:
        existing_min_dt = parse_ts(existing_min_ts)
        if existing_min_dt <= cutoff_utc:
            print(
                f"[BACKFILL] History already present "
                f"(earliest_ts={existing_min_ts} <= cutoff={iso_z(cutoff_utc)}). Skipping backfill."
            )
            # Ensure state cursor is aligned for live mode
            max_ts = get_db_max_ts(sb)
            set_state_last_ts(sb, max_ts)
            print(f"[BACKFILL] state last_ts set to {max_ts}")
            return

    end_cursor = now_utc
    calls = 0
    total_rth = 0

    while end_cursor > cutoff_utc:
        bars = alpaca_get_bars(end_ts_utc=end_cursor, start_ts_utc=None, limit=LIMIT)
        calls += 1

        if not bars:
            print("[BACKFILL] No bars returned; stopping.")
            break

        bars_sorted = sorted(bars, key=lambda x: x["t"])  # oldest -> newest

        # RTH-only filter
        rth_bars = [b for b in bars_sorted if is_rth_1m(parse_ts(b["t"]))]

        # Store
        rows = normalize_rows(rth_bars)
        upsert_candles(sb, rows)
        total_rth += len(rows)

        oldest_ts_str = bars_sorted[0]["t"]
        newest_ts_str = bars_sorted[-1]["t"]
        print(f"[BACKFILL {calls}] got={len(bars_sorted)} rth={len(rows)}  {oldest_ts_str} -> {newest_ts_str}")

        # Move cursor back 1 minute before oldest returned bar to avoid overlap
        end_cursor = parse_ts(oldest_ts_str) - timedelta(minutes=1)

        time.sleep(API_SLEEP_SECONDS)

        if end_cursor <= cutoff_utc:
            break

    print(f"[BACKFILL] done. calls={calls} total_rth_upserts≈{total_rth}")

    # Update state cursor to latest DB max ts
    max_ts = get_db_max_ts(sb)
    set_state_last_ts(sb, max_ts)
    print(f"[BACKFILL] state last_ts set to {max_ts}")

    if VERIFY_AFTER_BACKFILL:
        verify_quick(sb, cutoff_utc, now_utc)


# =============================
# LIVE
# =============================
def run_live(sb: Client) -> None:
    print(f"[LIVE] symbol={SYMBOL} tf={TIMEFRAME} feed={ALPACA_FEED}")
    print(f"[LIVE] polling every {POLL_SECONDS}s; storing RTH-only to {TABLE_CANDLES}")

    # Cursor selection: state -> DB max -> small lookback
    state_ts = get_state_last_ts(sb)
    if state_ts:
        last_saved = parse_ts(state_ts)
        print(f"[LIVE] loaded cursor from state: {state_ts}")
    else:
        db_max = get_db_max_ts(sb)
        if db_max:
            last_saved = parse_ts(db_max)
            print(f"[LIVE] no state; using DB max: {db_max}")
        else:
            last_saved = datetime.now(timezone.utc) - timedelta(minutes=LIVE_LOOKBACK_MIN)
            print(f"[LIVE] no state and no DB data; using lookback cursor: {iso_z(last_saved)}")

        set_state_last_ts(sb, iso_z(last_saved))

    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            # Pull a small window since last_saved (with slight overlap to be safe)
            start = last_saved - timedelta(minutes=2)
            end = now_utc

            bars = alpaca_get_bars(start_ts_utc=start, end_ts_utc=end, limit=LIMIT)
            if bars:
                bars_sorted = sorted(bars, key=lambda x: x["t"])

                new_rth = []
                for b in bars_sorted:
                    ts = parse_ts(b["t"])
                    if ts <= last_saved:
                        continue
                    if not is_rth_1m(ts):
                        continue
                    new_rth.append(b)

                if new_rth:
                    rows = normalize_rows(new_rth)
                    upsert_candles(sb, rows)

                    last_saved = parse_ts(new_rth[-1]["t"])
                    set_state_last_ts(sb, iso_z(last_saved))
                    print(f"[LIVE] wrote {len(rows)} new RTH bars; last_ts={iso_z(last_saved)}")

        except Exception as e:
            # Keep running in Railway
            print(f"[LIVE] ERROR: {e}")

        time.sleep(POLL_SECONDS)


# =============================
# MAIN
# =============================
def main():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise SystemExit("Missing Alpaca keys. Set APCA_API_KEY_ID and APCA_API_SECRET_KEY.")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Missing Supabase creds. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY).")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if MODE == "backfill":
        run_backfill(sb)
    elif MODE == "live":
        run_live(sb)
    else:
        raise SystemExit("MODE must be 'backfill' or 'live'.")


if __name__ == "__main__":
    main()
