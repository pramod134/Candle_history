"""
main.py — Alpaca -> Supabase candle ingestion with robust pagination support.

Supports modes:
  MODE=backfill   : Backfill from now back to YEARS_BACK (default 2 years), then exit.
                   Auto-skips if DB already contains >= YEARS_BACK of history (earliest_ts <= cutoff).
  MODE=live       : Continuous updater; polls and upserts only new 1m candles (with optional session filtering).
  MODE=daily_once : One-shot fetch (cron-friendly) for daily bars.
  MODE=weekly_once: One-shot fetch (cron-friendly) for weekly bars.

Key features:
  ✅ Always supplies BOTH start and end to Alpaca (avoids empty responses).
  ✅ Supports Alpaca pagination via next_page_token (optional exhaust mode).
  ✅ Session labeling is DST-safe (America/New_York).
  ✅ State cursor keyed by (symbol, timeframe) to support multiple TF ingestion.
  ✅ Gap-aware cursor advance (won’t skip missing minutes in 1m live mode).
  ✅ Optional 1m RTH integrity verification (390 minutes) + repair fetch for missing minutes.
  ✅ SESSION_MODE can be intraday (premarket+rth only) or all (store everything Alpaca returns).

Required ENV:
  APCA_API_KEY_ID
  APCA_API_SECRET_KEY
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY (recommended) or SUPABASE_KEY

Recommended ENV defaults:
  TABLE_NAME=candle_history_1m
  STATE_TABLE=ingest_state_v2
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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
TABLE_CANDLES = os.environ.get("TABLE_NAME", "candle_history_1m")
TABLE_STATE = os.environ.get("STATE_TABLE", "ingest_state_v2")

TABLE_INTEGRITY = os.environ.get("INTEGRITY_TABLE", "candle_integrity_day")

MODE = os.environ.get("MODE", "backfill").lower()  # backfill | live | daily_once | weekly_once

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

SESSION_MODE = os.environ.get("SESSION_MODE", "intraday").lower()  # intraday | all
STATE_TIMEFRAME_KEY = os.environ.get("STATE_TIMEFRAME_KEY")  # e.g., 1m, 1d, 1w

ENABLE_INTEGRITY = os.environ.get("ENABLE_INTEGRITY", "1") == "1"
INTEGRITY_REPAIR = os.environ.get("INTEGRITY_REPAIR", "1") == "1"
INTEGRITY_LOOKBACK_DAYS = int(os.environ.get("INTEGRITY_LOOKBACK_DAYS", "2"))

FETCH_ALL_PAGES = os.environ.get("FETCH_ALL_PAGES", "0") == "1"
MAX_PAGE_LOOPS = int(os.environ.get("MAX_PAGE_LOOPS", "50"))

BACKFILL_END_DELAY_MIN = int(os.environ.get("BACKFILL_END_DELAY_MIN", "16"))
LIVE_END_DELAY_MIN = int(os.environ.get("LIVE_END_DELAY_MIN", "15"))

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


def classify_session(ts_utc: datetime) -> Optional[str]:
    """premarket/rth classifier in ET; returns None for afterhours/overnight/weekends."""
    ts_et = ts_utc.astimezone(ET)
    if ts_et.weekday() >= 5:
        return None

    t = ts_et.time()
    pre_start = datetime(2000, 1, 1, 4, 0).time()
    rth_start = datetime(2000, 1, 1, 9, 30).time()
    rth_end = datetime(2000, 1, 1, 16, 0).time()

    if pre_start <= t < rth_start:
        return "premarket"
    if rth_start <= t < rth_end:
        return "rth"
    return None


def _infer_timeframe_key() -> str:
    """Map Alpaca timeframe strings to compact key used in ingest_state."""
    tf = (TIMEFRAME or "").strip().lower()
    mapping = {
        "1min": "1m",
        "3min": "3m",
        "5min": "5m",
        "15min": "15m",
        "1hour": "1h",
        "1day": "1d",
        "1week": "1w",
    }
    return mapping.get(tf, tf)


def _alpaca_request(params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    r = requests.get(ALPACA_BARS_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json() or {}


def alpaca_get_bars(
    start_ts_utc: datetime,
    end_ts_utc: datetime,
    limit: int = 4000,
    fetch_all_pages: bool = False,
) -> List[Dict[str, Any]]:
    """Fetch bars for [start, end], optionally following next_page_token."""
    base_params = {
        "timeframe": TIMEFRAME,
        "start": iso_z(start_ts_utc),
        "end": iso_z(end_ts_utc),
        "limit": limit,
        "adjustment": ADJUSTMENT,
        "feed": ALPACA_FEED,
    }

    bars: List[Dict[str, Any]] = []
    payload = _alpaca_request(base_params)
    bars.extend(payload.get("bars", []) or [])
    next_token = payload.get("next_page_token")

    if not fetch_all_pages:
        return bars

    loops = 0
    while next_token and loops < MAX_PAGE_LOOPS:
        loops += 1
        params = dict(base_params)
        params["page_token"] = next_token
        payload = _alpaca_request(params)
        bars.extend(payload.get("bars", []) or [])
        next_token = payload.get("next_page_token")
        time.sleep(API_SLEEP_SECONDS)

    return bars


def normalize_rows(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert Alpaca bars into DB rows."""
    out: List[Dict[str, Any]] = []
    for b in bars:
        ts_utc = parse_ts(b["t"])
        if SESSION_MODE == "all":
            session = "all"
        else:
            session = classify_session(ts_utc)
            if session is None:
                continue

        out.append(
            {
                "symbol": SYMBOL,
                "ts": b["t"],
                "session": session,
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
    tf_key = STATE_TIMEFRAME_KEY or _infer_timeframe_key()
    resp = (
        sb.table(TABLE_STATE)
        .select("last_ts")
        .eq("symbol", SYMBOL)
        .eq("timeframe", tf_key)
        .limit(1)
        .execute()
    )
    if resp.data and resp.data[0].get("last_ts"):
        return resp.data[0]["last_ts"]
    return None


def set_state_last_ts(sb: Client, last_ts: Optional[str]) -> None:
    tf_key = STATE_TIMEFRAME_KEY or _infer_timeframe_key()
    payload = {
        "symbol": SYMBOL,
        "timeframe": tf_key,
        "last_ts": last_ts,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    sb.table(TABLE_STATE).upsert(payload, on_conflict="symbol,timeframe").execute()


def _et_day_bounds_utc(et_date: datetime.date) -> Tuple[datetime, datetime]:
    start_et = datetime(et_date.year, et_date.month, et_date.day, 9, 30, tzinfo=ET)
    end_et = datetime(et_date.year, et_date.month, et_date.day, 16, 0, tzinfo=ET)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def _expected_rth_minutes_utc(et_date: datetime.date) -> List[datetime]:
    start_utc, end_utc = _et_day_bounds_utc(et_date)
    cur = start_utc
    out: List[datetime] = []
    while cur < end_utc:
        out.append(cur)
        cur += timedelta(minutes=1)
    return out


def verify_rth_day(sb: Client, et_date: datetime.date) -> Tuple[bool, List[str], int, int]:
    """Verify 1m RTH completeness (390 minutes) for et_date."""
    start_utc, end_utc = _et_day_bounds_utc(et_date)
    expected = _expected_rth_minutes_utc(et_date)
    expected_count = len(expected)

    resp = (
        sb.table(TABLE_CANDLES)
        .select("ts")
        .eq("symbol", SYMBOL)
        .eq("session", "rth")
        .gte("ts", iso_z(start_utc))
        .lt("ts", iso_z(end_utc))
        .execute()
    )
    actual_ts = {parse_ts(r["ts"]).replace(second=0, microsecond=0) for r in (resp.data or [])}
    actual_count = len(actual_ts)

    missing: List[str] = []
    for t in expected:
        t0 = t.replace(second=0, microsecond=0)
        if t0 not in actual_ts:
            missing.append(iso_z(t0))

    return len(missing) == 0, missing, expected_count, actual_count


def _upsert_integrity_row(
    sb: Client,
    et_date: datetime.date,
    is_complete: bool,
    missing: List[str],
    expected: int,
    actual: int,
) -> None:
    payload = {
        "symbol": SYMBOL,
        "trading_date": et_date.isoformat(),
        "rth_expected": expected,
        "rth_actual": actual,
        "missing_minutes": missing,
        "is_complete": is_complete,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        sb.table(TABLE_INTEGRITY).upsert(payload, on_conflict="symbol,trading_date").execute()
    except Exception as e:
        print(f"[INTEGRITY] WARN: could not upsert integrity row: {e}")


def _missing_to_ranges(missing_isoz: List[str]) -> List[Tuple[datetime, datetime]]:
    if not missing_isoz:
        return []
    missing_dt = sorted(parse_ts(x) for x in missing_isoz)
    ranges: List[Tuple[datetime, datetime]] = []
    start = missing_dt[0]
    prev = missing_dt[0]
    for cur in missing_dt[1:]:
        if cur == prev + timedelta(minutes=1):
            prev = cur
            continue
        ranges.append((start, prev))
        start = cur
        prev = cur
    ranges.append((start, prev))
    return ranges


def repair_missing_minutes(sb: Client, missing_isoz: List[str]) -> int:
    ranges = _missing_to_ranges(missing_isoz)
    fetched = 0
    for (s, e) in ranges:
        bars = alpaca_get_bars(
            start_ts_utc=s,
            end_ts_utc=e + timedelta(minutes=1),
            limit=LIMIT,
            fetch_all_pages=True,
        )
        fetched += len(bars)
        if bars:
            rows = normalize_rows(sorted(bars, key=lambda x: x["t"]))
            upsert_candles(sb, rows)
            time.sleep(API_SLEEP_SECONDS)
    return fetched


def verify_quick(sb: Client, cutoff_utc: datetime, now_utc: datetime) -> None:
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
def _compute_backfill_window(end_cursor: datetime) -> Tuple[datetime, datetime]:
    return end_cursor - timedelta(days=5), end_cursor


def run_backfill(sb: Client) -> None:
    now_utc = datetime.now(timezone.utc) - timedelta(minutes=BACKFILL_END_DELAY_MIN)
    cutoff_utc = now_utc - timedelta(days=int(365 * YEARS_BACK))

    print(f"[BACKFILL] symbol={SYMBOL} tf={TIMEFRAME} feed={ALPACA_FEED}")
    print(f"[BACKFILL] target range: {iso_z(now_utc)} back to {iso_z(cutoff_utc)}")
    print(f"[BACKFILL] writing to: {TABLE_CANDLES} (SESSION_MODE={SESSION_MODE})")
    print(f"[BACKFILL] fetch_all_pages={FETCH_ALL_PAGES}")

    existing_min_ts = get_db_min_ts(sb)
    if existing_min_ts:
        existing_min_dt = parse_ts(existing_min_ts)
        if existing_min_dt <= cutoff_utc:
            print(
                f"[BACKFILL] History already present "
                f"(earliest_ts={existing_min_ts} <= cutoff={iso_z(cutoff_utc)}). Skipping backfill."
            )
            max_ts = get_db_max_ts(sb)
            set_state_last_ts(sb, max_ts)
            print(f"[BACKFILL] state last_ts set to {max_ts}")
            return

    end_cursor = now_utc
    calls = 0
    total_kept = 0

    while end_cursor > cutoff_utc:
        start_window, end_window = _compute_backfill_window(end_cursor)

        bars = alpaca_get_bars(
            start_ts_utc=start_window,
            end_ts_utc=end_window,
            limit=LIMIT,
            fetch_all_pages=FETCH_ALL_PAGES,
        )
        calls += 1

        if not bars:
            print("[BACKFILL] No bars returned; stopping.")
            break

        bars_sorted = sorted(bars, key=lambda x: x["t"])  # oldest -> newest
        rows = normalize_rows(bars_sorted)
        upsert_candles(sb, rows)
        total_kept += len(rows)

        oldest_ts_str = bars_sorted[0]["t"]
        newest_ts_str = bars_sorted[-1]["t"]

        print(
            f"[BACKFILL {calls}] window={iso_z(start_window)}..{iso_z(end_window)} "
            f"got={len(bars_sorted)} kept={len(rows)}  {oldest_ts_str} -> {newest_ts_str}"
        )

        end_cursor = parse_ts(oldest_ts_str) - timedelta(minutes=1)
        time.sleep(API_SLEEP_SECONDS)

        if end_cursor <= cutoff_utc:
            break

    print(f"[BACKFILL] done. calls={calls} total_upserts≈{total_kept}")

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
    print(f"[LIVE] polling every {POLL_SECONDS}s; SESSION_MODE={SESSION_MODE}; writing to {TABLE_CANDLES}")
    print(f"[LIVE] fetch_all_pages={FETCH_ALL_PAGES}")

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

    last_integrity_check = 0.0

    while True:
        try:
            now_utc = datetime.now(timezone.utc) - timedelta(minutes=LIVE_END_DELAY_MIN)

            start = last_saved - timedelta(minutes=2)
            end = now_utc

            bars = alpaca_get_bars(
                start_ts_utc=start,
                end_ts_utc=end,
                limit=LIMIT,
                fetch_all_pages=FETCH_ALL_PAGES,
            )

            if bars:
                bars_sorted = sorted(bars, key=lambda x: x["t"])

                candidate_bars: List[Dict[str, Any]] = []
                for b in bars_sorted:
                    ts = parse_ts(b["t"]).replace(second=0, microsecond=0)
                    if ts <= last_saved.replace(second=0, microsecond=0):
                        continue
                    candidate_bars.append(b)

                if candidate_bars:
                    rows = normalize_rows(candidate_bars)
                    upsert_candles(sb, rows)

                    kept_ts = {
                        parse_ts(r["ts"]).replace(second=0, microsecond=0)
                        for r in rows
                        if r.get("ts")
                    }
                    cursor = last_saved.replace(second=0, microsecond=0)
                    advanced = 0
                    while True:
                        nxt = cursor + timedelta(minutes=1)
                        if nxt in kept_ts:
                            cursor = nxt
                            advanced += 1
                            continue
                        break

                    if advanced > 0:
                        last_saved = cursor
                        set_state_last_ts(sb, iso_z(last_saved))
                        print(f"[LIVE] wrote {len(rows)} rows; advanced {advanced}m; last_ts={iso_z(last_saved)}")
                    else:
                        print(f"[LIVE] wrote {len(rows)} rows; gap detected after {iso_z(last_saved)}")

            tf_key = STATE_TIMEFRAME_KEY or _infer_timeframe_key()
            if ENABLE_INTEGRITY and tf_key == "1m":
                now_s = time.time()
                if now_s - last_integrity_check >= 300:
                    last_integrity_check = now_s
                    for d in range(INTEGRITY_LOOKBACK_DAYS):
                        et_day = (datetime.now(ET) - timedelta(days=d)).date()
                        ok, missing, expected, actual = verify_rth_day(sb, et_day)
                        _upsert_integrity_row(sb, et_day, ok, missing, expected, actual)
                        if ok:
                            print(f"[INTEGRITY] {et_day} OK ({actual}/{expected})")
                            continue
                        print(f"[INTEGRITY] {et_day} MISSING {len(missing)} minutes ({actual}/{expected})")
                        if INTEGRITY_REPAIR and missing:
                            fetched = repair_missing_minutes(sb, missing)
                            ok2, missing2, expected2, actual2 = verify_rth_day(sb, et_day)
                            _upsert_integrity_row(sb, et_day, ok2, missing2, expected2, actual2)
                            print(
                                f"[INTEGRITY] repair fetched={fetched} -> "
                                f"ok={ok2} missing={len(missing2)} ({actual2}/{expected2})"
                            )

        except Exception as e:
            print(f"[LIVE] ERROR: {e}")

        time.sleep(POLL_SECONDS)


def run_once(sb: Client) -> None:
    """One-shot fetcher for daily/weekly bars using state cursor (cron-friendly)."""
    print(f"[ONCE] symbol={SYMBOL} tf={TIMEFRAME} feed={ALPACA_FEED}")
    print(f"[ONCE] SESSION_MODE={SESSION_MODE}; writing to {TABLE_CANDLES}")

    state_ts = get_state_last_ts(sb)
    if state_ts:
        start = parse_ts(state_ts) + timedelta(seconds=1)
        print(f"[ONCE] loaded cursor from state: {state_ts}")
    else:
        db_max = get_db_max_ts(sb)
        if db_max:
            start = parse_ts(db_max) + timedelta(seconds=1)
            print(f"[ONCE] no state; using DB max: {db_max}")
        else:
            now_utc = datetime.now(timezone.utc) - timedelta(minutes=LIVE_END_DELAY_MIN)
            start = now_utc - timedelta(days=int(365 * YEARS_BACK))
            print(f"[ONCE] no state and no DB data; using YEARS_BACK start: {iso_z(start)}")
        set_state_last_ts(sb, iso_z(start - timedelta(seconds=1)))

    end = datetime.now(timezone.utc) - timedelta(minutes=LIVE_END_DELAY_MIN)
    if start >= end:
        print("[ONCE] start>=end; nothing to do.")
        return

    bars = alpaca_get_bars(
        start_ts_utc=start,
        end_ts_utc=end,
        limit=LIMIT,
        fetch_all_pages=True,
    )
    if not bars:
        print("[ONCE] no bars returned")
        return

    bars_sorted = sorted(bars, key=lambda x: x["t"])
    rows = normalize_rows(bars_sorted)
    upsert_candles(sb, rows)

    newest = parse_ts(bars_sorted[-1]["t"]).replace(second=0, microsecond=0)
    set_state_last_ts(sb, iso_z(newest))
    print(f"[ONCE] wrote {len(rows)} rows; last_ts={iso_z(newest)}")


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
    elif MODE in ("daily_once", "weekly_once"):
        run_once(sb)
    else:
        raise SystemExit("MODE must be 'backfill', 'live', 'daily_once', or 'weekly_once'.")


if __name__ == "__main__":
    main()
