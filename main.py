"""
main.py — Alpaca -> Supabase candle ingestion with robust pagination support.

Patch: (1) skip integrity checks on weekends/no-data days, (2) stop 1D/1W re-backfill when history already exists (tolerant + freshness).
Adds AUTO orchestration for 1m + derived intraday + 1d/1w (Alpaca direct) with no env changes.

AUTO mode orchestrates backfill + derived TF build + live updates with no env tweaking.
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

# Aggregation (intraday TFs built from 1m RTH)
# Source 1m table (must contain premarket+rth; aggregation uses only rth rows)
TABLE_1M_SOURCE = os.environ.get("TABLE_1M_SOURCE", "candle_history_1m")
# Target derived tables
TABLE_3M = os.environ.get("TABLE_3M", "candle_history_3m")
TABLE_5M = os.environ.get("TABLE_5M", "candle_history_5m")
TABLE_15M = os.environ.get("TABLE_15M", "candle_history_15m")
TABLE_1H = os.environ.get("TABLE_1H", "candle_history_1h")
TABLE_1D = os.environ.get("TABLE_1D", "candle_history_1d")
TABLE_1W = os.environ.get("TABLE_1W", "candle_history_1w")

# Build controls
BUILD_DAYS_BACK = int(os.environ.get("BUILD_DAYS_BACK", "3"))  # used by MODE=build_intraday_tfs
BUILD_VERIFY_COMPLETE = os.environ.get("BUILD_VERIFY_COMPLETE", "1") == "1"  # require 390 1m RTH mins

MODE = os.environ.get("MODE", "auto").lower()  # auto | backfill | live | daily_once | weekly_once | build_intraday_tfs

ALPACA_FEED = os.environ.get("ALPACA_FEED", "iex")
TIMEFRAME = os.environ.get("TIMEFRAME", "1Min")
ADJUSTMENT = os.environ.get("ADJUSTMENT", "raw")

YEARS_BACK = float(os.environ.get("YEARS_BACK", "2"))
LIMIT = int(os.environ.get("LIMIT", "4000"))

API_SLEEP_SECONDS = float(os.environ.get("API_SLEEP_SECONDS", "0.35"))
UPSERT_CHUNK = int(os.environ.get("UPSERT_CHUNK", "500"))

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "20"))
LIVE_LOOKBACK_MIN = int(os.environ.get("LIVE_LOOKBACK_MIN", "120"))

# AUTO orchestration knobs (defaults require no env changes)
AUTO_REBUILD_EVERY_SECONDS = int(os.environ.get("AUTO_REBUILD_EVERY_SECONDS", "300"))  # rebuild derived TFs every 5m
AUTO_REBUILD_DAYS_BACK = int(os.environ.get("AUTO_REBUILD_DAYS_BACK", "3"))  # rebuild last N days in live
AUTO_FULL_BUILD_DAYS_BACK = int(
    os.environ.get("AUTO_FULL_BUILD_DAYS_BACK", str(int(365 * float(os.environ.get("YEARS_BACK", "2")))))
)

# Lightweight min/max coverage check cadence (no candle counting)
AUTO_HISTORY_CHECK_SECONDS = int(os.environ.get("AUTO_HISTORY_CHECK_SECONDS", "600"))  # 10 minutes

# Derived-history detection tolerance (prevents unnecessary full rebuilds after restart)
# - min ts can be slightly newer than cutoff due to RTH-only anchors, holidays, etc.
# - max ts must be "fresh enough" compared to 1m max.
DERIVED_HISTORY_TOL_DAYS = int(os.environ.get("DERIVED_HISTORY_TOL_DAYS", "7"))
DERIVED_STALE_DAYS = int(os.environ.get("DERIVED_STALE_DAYS", "3"))

# 1D/1W history detection tolerance (daily/weekly bars don't align perfectly with cutoff timestamp)
# Min tolerance: allow 1d/1w min_ts to be newer than cutoff by this many days
DAILY_HISTORY_TOL_DAYS = int(os.environ.get("DAILY_HISTORY_TOL_DAYS", "14"))
WEEKLY_HISTORY_TOL_DAYS = int(os.environ.get("WEEKLY_HISTORY_TOL_DAYS", "28"))
# Freshness: if 1d/1w max_ts is older than 1m max by this many days, treat as stale and backfill/refresh
HTF_STALE_DAYS = int(os.environ.get("HTF_STALE_DAYS", "10"))

# 1D/1W auto refresh cadence while service runs
AUTO_DAILY_REFRESH_SECONDS = int(os.environ.get("AUTO_DAILY_REFRESH_SECONDS", "3600"))  # 1 hour
AUTO_WEEKLY_REFRESH_SECONDS = int(os.environ.get("AUTO_WEEKLY_REFRESH_SECONDS", "21600"))  # 6 hours

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


def alpaca_get_bars_tf(
    timeframe: str,
    start_ts_utc: datetime,
    end_ts_utc: datetime,
    limit: int = 4000,
    fetch_all_pages: bool = False,
) -> List[Dict[str, Any]]:
    """
    Same as alpaca_get_bars(), but allows overriding timeframe per call (needed for 1Day/1Week while main runs 1Min).
    """
    base_params = {
        "timeframe": timeframe,
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


def get_table_min_ts(sb: Client, table: str) -> Optional[str]:
    resp = (
        sb.table(table)
        .select("ts")
        .eq("symbol", SYMBOL)
        .order("ts", desc=False)
        .limit(1)
        .execute()
    )
    return resp.data[0]["ts"] if resp.data else None


def get_table_max_ts(sb: Client, table: str) -> Optional[str]:
    resp = (
        sb.table(table)
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


def get_state_last_ts_for(sb: Client, tf_key: str) -> Optional[str]:
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


def set_state_last_ts_for(sb: Client, tf_key: str, last_ts: Optional[str]) -> None:
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


def _day_has_any_candles(sb: Client, table: str, et_date: datetime.date) -> bool:
    """
    Returns True if there is at least one candle row for the ET date in [09:30, 16:00].
    Used to avoid false integrity failures on weekends/holidays.
    """
    start_utc, end_utc = _et_day_bounds_utc(et_date)
    resp = (
        sb.table(table)
        .select("ts")
        .eq("symbol", SYMBOL)
        .gte("ts", iso_z(start_utc))
        .lt("ts", iso_z(end_utc))
        .limit(1)
        .execute()
    )
    return bool(resp.data)


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
        sb.table(TABLE_1M_SOURCE)
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
# DB READ (paged) + AGGREGATION
# =============================
def _sb_select_all_ts(
    sb: Client,
    table: str,
    start_utc: datetime,
    end_utc: datetime,
    session: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Read all 1m rows for [start_utc, end_utc) using range pagination.
    Returns rows with keys: ts, open, high, low, close, volume, vwap, trade_count
    """
    rows: List[Dict[str, Any]] = []
    page = 0
    page_size = 1000  # Supabase default max per request often ~1000
    while True:
        q = (
            sb.table(table)
            .select("ts,open,high,low,close,volume,vwap,trade_count,session")
            .eq("symbol", SYMBOL)
            .gte("ts", iso_z(start_utc))
            .lt("ts", iso_z(end_utc))
            .order("ts", desc=False)
            .range(page * page_size, page * page_size + page_size - 1)
        )
        if session is not None:
            q = q.eq("session", session)
        resp = q.execute()
        data = resp.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        page += 1
    return rows


def _bucket_start_et(ts_et: datetime, minutes_per_bucket: int) -> datetime:
    """Return bucket start ET anchored at 09:30."""
    anchor = ts_et.replace(hour=9, minute=30, second=0, microsecond=0)
    mins = int((ts_et - anchor).total_seconds() // 60)
    idx = mins // minutes_per_bucket
    return anchor + timedelta(minutes=idx * minutes_per_bucket)


def _aggregate_intraday_for_date(
    sb: Client,
    et_date: datetime.date,
    minutes_per_bucket: int,
    target_table: str,
    is_hourly: bool = False,
) -> int:
    """
    Aggregate RTH-only 1m candles for one ET date into target_table.
    - Buckets anchored to 09:30 ET
    - If is_hourly=True: clamp bucket end to 16:00 and allow final partial (15:30-16:00)
    """
    start_utc, end_utc = _et_day_bounds_utc(et_date)

    # Pull 1m RTH rows from source table
    one_min = _sb_select_all_ts(sb, TABLE_1M_SOURCE, start_utc, end_utc, session="rth")
    if not one_min:
        return 0

    # Parse to datetimes and keep only minute precision
    candles = []
    for r in one_min:
        ts = parse_ts(r["ts"]).replace(second=0, microsecond=0)
        candles.append(
            (
                ts,
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r.get("volume") or 0),
                (float(r.get("vwap")) if r.get("vwap") is not None else None),
                int(r.get("trade_count") or 0),
            )
        )

    # Build buckets
    buckets: Dict[datetime, List[tuple]] = {}
    for (ts_utc, o, h, l, c, v, vw, n) in candles:
        ts_et = ts_utc.astimezone(ET)
        # safety: ensure inside RTH
        if not (ts_et.hour > 9 or (ts_et.hour == 9 and ts_et.minute >= 30)):
            continue
        if ts_et.hour >= 16:
            continue

        bstart_et = _bucket_start_et(ts_et, minutes_per_bucket)
        bstart_utc = bstart_et.astimezone(timezone.utc).replace(second=0, microsecond=0)

        if is_hourly:
            # allow last partial hour candle: any bucket that starts before 16:00 is valid
            end_et = bstart_et + timedelta(minutes=minutes_per_bucket)
            rth_end_et = ts_et.replace(hour=16, minute=0, second=0, microsecond=0)
            if bstart_et >= rth_end_et:
                continue
            # clamp end for conceptual correctness (we still group by start)
            _ = min(end_et, rth_end_et)

        buckets.setdefault(bstart_utc, []).append((ts_utc, o, h, l, c, v, vw, n))

    # Aggregate each bucket -> one output row
    out_rows: List[Dict[str, Any]] = []
    for bstart_utc, vals in sorted(buckets.items(), key=lambda x: x[0]):
        vals_sorted = sorted(vals, key=lambda x: x[0])
        o = vals_sorted[0][1]
        c = vals_sorted[-1][4]
        h = max(x[2] for x in vals_sorted)
        l = min(x[3] for x in vals_sorted)
        vol = sum(x[5] for x in vals_sorted)
        tc = sum(x[7] for x in vals_sorted)

        # VWAP: Σ(price*vol)/Σ(vol); use 1m vwap if present else close
        if vol > 0:
            num = 0.0
            den = 0.0
            for (_ts, _o, _h, _l, _c, _v, _vw, _n) in vals_sorted:
                if _v <= 0:
                    continue
                px = _vw if (_vw is not None and _vw != 0) else _c
                num += px * _v
                den += _v
            vwap = (num / den) if den > 0 else None
        else:
            vwap = None

        out_rows.append(
            {
                "symbol": SYMBOL,
                "ts": iso_z(bstart_utc),
                "session": "rth",
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": vol,
                "vwap": vwap,
                "trade_count": tc,
            }
        )

    if out_rows:
        for batch in chunked(out_rows, UPSERT_CHUNK):
            sb.table(target_table).upsert(batch, on_conflict="symbol,ts").execute()

    return len(out_rows)


def run_build_intraday_tfs(sb: Client) -> None:
    """
    Build 3m/5m/15m/1h candles from 1m RTH data in TABLE_1M_SOURCE.
    RTH-only for derived TFs. Buckets anchored to 09:30 ET.
    1h includes final partial candle (15:30-16:00).
    """
    print(f"[BUILD] symbol={SYMBOL} source={TABLE_1M_SOURCE}")
    print(f"[BUILD] targets: 3m={TABLE_3M} 5m={TABLE_5M} 15m={TABLE_15M} 1h={TABLE_1H}")
    print(f"[BUILD] days_back={BUILD_DAYS_BACK} verify_complete={BUILD_VERIFY_COMPLETE}")

    today_et = datetime.now(ET).date()
    # Build from oldest to newest within lookback
    for i in range(BUILD_DAYS_BACK, -1, -1):
        et_day = today_et - timedelta(days=i)
        # Skip weekends quickly
        if datetime(et_day.year, et_day.month, et_day.day, tzinfo=ET).weekday() >= 5:
            continue

        if BUILD_VERIFY_COMPLETE:
            ok, missing, expected, actual = verify_rth_day(sb, et_day)
            _upsert_integrity_row(sb, et_day, ok, missing, expected, actual)
            if not ok:
                print(f"[BUILD] {et_day} skipped (incomplete 1m RTH: {actual}/{expected}, missing={len(missing)})")
                continue

        c3 = _aggregate_intraday_for_date(sb, et_day, 3, TABLE_3M, is_hourly=False)
        c5 = _aggregate_intraday_for_date(sb, et_day, 5, TABLE_5M, is_hourly=False)
        c15 = _aggregate_intraday_for_date(sb, et_day, 15, TABLE_15M, is_hourly=False)
        c1h = _aggregate_intraday_for_date(sb, et_day, 60, TABLE_1H, is_hourly=True)

        print(f"[BUILD] {et_day} built: 3m={c3} 5m={c5} 15m={c15} 1h={c1h}")


# =============================
# AUTO ORCHESTRATION
# =============================
def _cutoff_now_utc() -> Tuple[datetime, datetime]:
    now_utc = datetime.now(timezone.utc) - timedelta(minutes=BACKFILL_END_DELAY_MIN)
    cutoff_utc = now_utc - timedelta(days=int(365 * YEARS_BACK))
    return now_utc, cutoff_utc


def _has_required_history(sb: Client, table: str) -> bool:
    """
    History is considered present if earliest ts <= cutoff_utc.
    """
    _now, cutoff_utc = _cutoff_now_utc()
    mn = get_table_min_ts(sb, table)
    if not mn:
        return False
    try:
        return parse_ts(mn) <= cutoff_utc
    except Exception:
        return False


def _derived_history_ok(sb: Client, table: str) -> bool:
    """
    Tolerant + freshness-based check for derived TF tables (3m/5m/15m/1h), to avoid full rebuilds after restart.

    OK when:
      - table has data
      - min_ts is not "too new" relative to cutoff (within tolerance window)
      - max_ts is not stale compared to 1m max (within stale window)

    Rationale:
      Derived TFs are RTH-only and anchored to 09:30 ET, so their first bar can be later than the exact cutoff time.
      Also holidays/early closes can affect earliest available derived candle.
    """
    _now_utc, cutoff_utc = _cutoff_now_utc()

    mn = get_table_min_ts(sb, table)
    mx = get_table_max_ts(sb, table)
    if not mn or not mx:
        return False

    try:
        mn_dt = parse_ts(mn)
        mx_dt = parse_ts(mx)
    except Exception:
        return False

    # 1) Min tolerance: allow derived min to be newer than cutoff by N days
    tol_cutoff = cutoff_utc + timedelta(days=int(DERIVED_HISTORY_TOL_DAYS))
    if mn_dt > tol_cutoff:
        return False

    # 2) Freshness: derived max should be close to 1m max (or at least not stale)
    src_max = get_table_max_ts(sb, TABLE_1M_SOURCE) or get_table_max_ts(sb, TABLE_CANDLES)
    if not src_max:
        # If we can't determine 1m freshness, treat as OK if it has any data and min is within tolerance.
        return True

    try:
        src_max_dt = parse_ts(src_max)
    except Exception:
        return True

    stale_floor = src_max_dt - timedelta(days=int(DERIVED_STALE_DAYS))
    if mx_dt < stale_floor:
        return False

    return True


def _htf_history_ok(sb: Client, table: str, min_tol_days: int) -> bool:
    """
    Tolerant + freshness-based check for higher TF tables (1D/1W).

    OK when:
      - table has data
      - min_ts is not "too new" vs cutoff (within tolerance window)
      - max_ts is not stale vs 1m max (within HTF_STALE_DAYS)
    """
    _now, cutoff_utc = _cutoff_now_utc()
    mn = get_table_min_ts(sb, table)
    mx = get_table_max_ts(sb, table)
    if not mn or not mx:
        return False
    try:
        mn_dt = parse_ts(mn)
        mx_dt = parse_ts(mx)
    except Exception:
        return False

    tol_cutoff = cutoff_utc + timedelta(days=int(min_tol_days))
    if mn_dt > tol_cutoff:
        return False

    src_max = get_table_max_ts(sb, TABLE_1M_SOURCE) or get_table_max_ts(sb, TABLE_CANDLES)
    if not src_max:
        return True
    try:
        src_max_dt = parse_ts(src_max)
    except Exception:
        return True

    stale_floor = src_max_dt - timedelta(days=int(HTF_STALE_DAYS))
    if mx_dt < stale_floor:
        return False
    return True


def _ensure_1m_history(sb: Client) -> None:
    """
    Ensures 2y history exists in TABLE_CANDLES (default candle_history_1m).
    If missing, runs backfill; else sets state cursor to DB max ts.
    """
    now_utc, cutoff_utc = _cutoff_now_utc()
    print(f"[AUTO] ensure 1m history in {TABLE_CANDLES} (need earliest <= {iso_z(cutoff_utc)})")

    if _has_required_history(sb, TABLE_CANDLES):
        mx = get_db_max_ts(sb)
        if mx:
            set_state_last_ts(sb, mx)
            print(f"[AUTO] 1m history OK; state cursor set to DB max {mx}")
        else:
            # no rows but history check says OK shouldn't happen; fall back
            set_state_last_ts(sb, iso_z(now_utc - timedelta(minutes=LIVE_LOOKBACK_MIN)))
            print("[AUTO] 1m history check inconsistent; seeded cursor with lookback.")
        return

    print("[AUTO] 1m history missing; running backfill now...")
    run_backfill(sb)


def _ensure_intraday_derived_history(sb: Client) -> None:
    """
    If derived tables are missing 2y history, build full range once from 1m source.
    """
    print("[AUTO] ensure derived TF history (3m/5m/15m/1h) from 1m source")
    # Use tolerant + freshness-based check for derived TFs to prevent unnecessary rebuilds.
    need_full = False
    for t in (TABLE_3M, TABLE_5M, TABLE_15M, TABLE_1H):
        ok = _derived_history_ok(sb, t)
        if not ok:
            print(f"[AUTO] derived table not OK (will rebuild): {t}")
            need_full = True

    if not need_full:
        print("[AUTO] derived TF history OK")
        return

    # Full build: temporarily override BUILD_DAYS_BACK via local loop rather than env
    print(f"[AUTO] running full derived build for {AUTO_FULL_BUILD_DAYS_BACK} days back...")
    global BUILD_DAYS_BACK
    prev = BUILD_DAYS_BACK
    global BUILD_VERIFY_COMPLETE
    prev_verify = BUILD_VERIFY_COMPLETE
    try:
        BUILD_DAYS_BACK = int(AUTO_FULL_BUILD_DAYS_BACK)
        # For first-time build, don't block on strict completeness; incomplete days will still aggregate what's present.
        # (Integrity repair can fill gaps later.)
        BUILD_VERIFY_COMPLETE = False
        run_build_intraday_tfs(sb)
    finally:
        BUILD_DAYS_BACK = prev
        BUILD_VERIFY_COMPLETE = prev_verify


def run_auto(sb: Client) -> None:
    """
    Full automation:
      1) Ensure 1m history (backfill if needed)
      2) Ensure derived TF history (build full range if needed)
      2b) Ensure 1D/1W history (Alpaca direct; tolerant+freshness)
      3) Enter live loop; periodically rebuild derived TFs for recent days
      4) Every AUTO_HISTORY_CHECK_SECONDS, re-run lightweight history checks (min/max coverage only)
    """
    print(f"[AUTO] starting automation for {SYMBOL}")
    print(f"[AUTO] 1m table={TABLE_CANDLES}, source_1m={TABLE_1M_SOURCE}")

    def _rows_from_higher_tf_bars(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for b in sorted(bars, key=lambda x: x["t"]):
            rows.append(
                {
                    "symbol": SYMBOL,
                    "ts": b["t"],
                    "session": "all",
                    "open": float(b["o"]),
                    "high": float(b["h"]),
                    "low": float(b["l"]),
                    "close": float(b["c"]),
                    "volume": float(b.get("v") or 0),
                    "vwap": float(b.get("vw") or 0) if b.get("vw") is not None else None,
                    "trade_count": int(b.get("n") or 0),
                }
            )
        return rows

    def _ensure_1d_1w_history() -> None:
        now_utc, cutoff_utc = _cutoff_now_utc()
        print(f"[AUTO] ensure 1D/1W history (need earliest <= {iso_z(cutoff_utc)})")

        if _htf_history_ok(sb, TABLE_1D, DAILY_HISTORY_TOL_DAYS):
            mx = get_table_max_ts(sb, TABLE_1D)
            if mx:
                set_state_last_ts_for(sb, "1d", mx)
                print(f"[AUTO] 1D history OK; state(1d)={mx}")
        else:
            print(f"[AUTO] 1D missing history in {TABLE_1D}; backfilling from Alpaca...")
            bars = alpaca_get_bars_tf("1Day", cutoff_utc, now_utc, limit=LIMIT, fetch_all_pages=True)
            rows = _rows_from_higher_tf_bars(bars)
            if rows:
                for batch in chunked(rows, UPSERT_CHUNK):
                    sb.table(TABLE_1D).upsert(batch, on_conflict="symbol,ts").execute()
                newest = rows[-1]["ts"]
                set_state_last_ts_for(sb, "1d", newest)
                print(f"[AUTO] 1D backfill wrote {len(rows)} rows; state(1d)={newest}")

        if _htf_history_ok(sb, TABLE_1W, WEEKLY_HISTORY_TOL_DAYS):
            mx = get_table_max_ts(sb, TABLE_1W)
            if mx:
                set_state_last_ts_for(sb, "1w", mx)
                print(f"[AUTO] 1W history OK; state(1w)={mx}")
        else:
            print(f"[AUTO] 1W missing history in {TABLE_1W}; backfilling from Alpaca...")
            bars = alpaca_get_bars_tf("1Week", cutoff_utc, now_utc, limit=LIMIT, fetch_all_pages=True)
            rows = _rows_from_higher_tf_bars(bars)
            if rows:
                for batch in chunked(rows, UPSERT_CHUNK):
                    sb.table(TABLE_1W).upsert(batch, on_conflict="symbol,ts").execute()
                newest = rows[-1]["ts"]
                set_state_last_ts_for(sb, "1w", newest)
                print(f"[AUTO] 1W backfill wrote {len(rows)} rows; state(1w)={newest}")

    def _refresh_1d_1w_incremental(now_utc: datetime) -> None:
        last_1d = get_state_last_ts_for(sb, "1d")
        start_1d = parse_ts(last_1d) + timedelta(seconds=1) if last_1d else now_utc - timedelta(days=int(365 * YEARS_BACK))
        bars_d = alpaca_get_bars_tf("1Day", start_1d, now_utc, limit=LIMIT, fetch_all_pages=True)
        if bars_d:
            rows = _rows_from_higher_tf_bars(bars_d)
            for batch in chunked(rows, UPSERT_CHUNK):
                sb.table(TABLE_1D).upsert(batch, on_conflict="symbol,ts").execute()
            newest = rows[-1]["ts"]
            set_state_last_ts_for(sb, "1d", newest)
            print(f"[AUTO] 1D refresh wrote {len(rows)} rows; state(1d)={newest}")

        last_1w = get_state_last_ts_for(sb, "1w")
        start_1w = parse_ts(last_1w) + timedelta(seconds=1) if last_1w else now_utc - timedelta(days=int(365 * YEARS_BACK))
        bars_w = alpaca_get_bars_tf("1Week", start_1w, now_utc, limit=LIMIT, fetch_all_pages=True)
        if bars_w:
            rows = _rows_from_higher_tf_bars(bars_w)
            for batch in chunked(rows, UPSERT_CHUNK):
                sb.table(TABLE_1W).upsert(batch, on_conflict="symbol,ts").execute()
            newest = rows[-1]["ts"]
            set_state_last_ts_for(sb, "1w", newest)
            print(f"[AUTO] 1W refresh wrote {len(rows)} rows; state(1w)={newest}")

    def _auto_history_check(now_utc: datetime) -> None:
        """
        Lightweight periodic health/coverage check (min/max only).
        - No counting candles.
        - Repairs only when a table looks incomplete/stale.
        """
        _, cutoff_utc = _cutoff_now_utc()
        print(f"[AUTO][HIST] running coverage check (cutoff={iso_z(cutoff_utc)})")

        # 1) 1m coverage (strict min_ts vs cutoff)
        if not _has_required_history(sb, TABLE_CANDLES):
            print("[AUTO][HIST] 1m coverage not OK -> running backfill")
            run_backfill(sb)
        else:
            # keep state cursor aligned with latest
            mx_1m = get_table_max_ts(sb, TABLE_CANDLES)
            if mx_1m:
                set_state_last_ts(sb, mx_1m)
                print(f"[AUTO][HIST] 1m OK; state cursor set to {mx_1m}")

        # 2) Derived intraday TFs coverage (tolerant + freshness)
        derived_bad = []
        for t in (TABLE_3M, TABLE_5M, TABLE_15M, TABLE_1H):
            if not _derived_history_ok(sb, t):
                derived_bad.append(t)
        if derived_bad:
            print(f"[AUTO][HIST] derived tables not OK -> full rebuild: {derived_bad}")
            # Full derived build, non-blocking on completeness (same as startup behavior)
            global BUILD_DAYS_BACK, BUILD_VERIFY_COMPLETE
            prev_days = BUILD_DAYS_BACK
            prev_verify = BUILD_VERIFY_COMPLETE
            try:
                BUILD_DAYS_BACK = int(AUTO_FULL_BUILD_DAYS_BACK)
                BUILD_VERIFY_COMPLETE = False
                run_build_intraday_tfs(sb)
            finally:
                BUILD_DAYS_BACK = prev_days
                BUILD_VERIFY_COMPLETE = prev_verify
        else:
            print("[AUTO][HIST] derived TF tables OK")

        # 3) 1D/1W coverage (tolerant + freshness)
        if not _htf_history_ok(sb, TABLE_1D, DAILY_HISTORY_TOL_DAYS):
            print("[AUTO][HIST] 1D not OK -> backfill/repair via ensure_1d_1w_history()")
            _ensure_1d_1w_history()
        else:
            mx_1d = get_table_max_ts(sb, TABLE_1D)
            if mx_1d:
                set_state_last_ts_for(sb, "1d", mx_1d)
            print("[AUTO][HIST] 1D OK")

        if not _htf_history_ok(sb, TABLE_1W, WEEKLY_HISTORY_TOL_DAYS):
            print("[AUTO][HIST] 1W not OK -> backfill/repair via ensure_1d_1w_history()")
            _ensure_1d_1w_history()
        else:
            mx_1w = get_table_max_ts(sb, TABLE_1W)
            if mx_1w:
                set_state_last_ts_for(sb, "1w", mx_1w)
            print("[AUTO][HIST] 1W OK")

    # Step 1: Ensure 1m history
    _ensure_1m_history(sb)

    # Step 2: Ensure derived history once
    _ensure_intraday_derived_history(sb)

    # Step 2b: Ensure 1D/1W history once (Alpaca direct)
    _ensure_1d_1w_history()

    # Step 3: Live + periodic builder
    print("[AUTO] entering live mode with periodic derived rebuilds...")
    last_rebuild = 0.0
    last_history_check = 0.0
    last_daily_refresh = 0.0
    last_weekly_refresh = 0.0

    # We re-use live logic but embed periodic derived rebuilds inside the loop.
    # Implemented by running a small "live loop" here instead of calling run_live(), to avoid refactoring.
    state_ts = get_state_last_ts(sb)
    if state_ts:
        last_saved = parse_ts(state_ts)
        print(f"[AUTO] loaded cursor from state: {state_ts}")
    else:
        db_max = get_db_max_ts(sb)
        if db_max:
            last_saved = parse_ts(db_max)
            print(f"[AUTO] no state; using DB max: {db_max}")
        else:
            last_saved = datetime.now(timezone.utc) - timedelta(minutes=LIVE_LOOKBACK_MIN)
            print(f"[AUTO] no state and no DB data; using lookback cursor: {iso_z(last_saved)}")
        set_state_last_ts(sb, iso_z(last_saved))

    last_integrity_check = 0.0

    while True:
        try:
            now_utc = datetime.now(timezone.utc) - timedelta(minutes=LIVE_END_DELAY_MIN)
            start = last_saved - timedelta(minutes=2)
            end = now_utc

            bars = alpaca_get_bars(start_ts_utc=start, end_ts_utc=end, limit=LIMIT, fetch_all_pages=FETCH_ALL_PAGES)
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

                    kept_ts = {parse_ts(r["ts"]).replace(second=0, microsecond=0) for r in rows if r.get("ts")}
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
                        print(f"[AUTO] LIVE wrote {len(rows)} rows; advanced {advanced}m; last_ts={iso_z(last_saved)}")
                    else:
                        print(f"[AUTO] LIVE wrote {len(rows)} rows; gap detected after {iso_z(last_saved)}")

            # Periodic derived rebuild (recent days only)
            now_s = time.time()
            if now_s - last_rebuild >= AUTO_REBUILD_EVERY_SECONDS:
                last_rebuild = now_s
                global BUILD_DAYS_BACK
                prev = BUILD_DAYS_BACK
                try:
                    BUILD_DAYS_BACK = int(AUTO_REBUILD_DAYS_BACK)
                    run_build_intraday_tfs(sb)
                finally:
                    BUILD_DAYS_BACK = prev

            # Periodic lightweight history/coverage check (min/max only)
            if now_s - last_history_check >= AUTO_HISTORY_CHECK_SECONDS:
                last_history_check = now_s
                try:
                    _auto_history_check(now_utc)
                except Exception as e:
                    print(f"[AUTO][HIST] ERROR: {e}")

            # Periodic 1D/1W refresh (Alpaca direct)
            if now_s - last_daily_refresh >= AUTO_DAILY_REFRESH_SECONDS:
                last_daily_refresh = now_s
                _refresh_1d_1w_incremental(now_utc)
            elif now_s - last_weekly_refresh >= AUTO_WEEKLY_REFRESH_SECONDS:
                last_weekly_refresh = now_s
                _refresh_1d_1w_incremental(now_utc)

            # Optional integrity check/repair stays in place for 1m
            tf_key = STATE_TIMEFRAME_KEY or _infer_timeframe_key()
            if ENABLE_INTEGRITY and tf_key == "1m":
                if now_s - last_integrity_check >= 300:
                    last_integrity_check = now_s
                    for d in range(INTEGRITY_LOOKBACK_DAYS):
                        et_day = (datetime.now(ET) - timedelta(days=d)).date()

                        # Skip weekends (no trading)
                        if et_day.weekday() >= 5:
                            continue

                        # Skip non-trading days/holidays where there are no candles at all
                        # (prevents false "390 missing" alarms)
                        if not _day_has_any_candles(sb, TABLE_1M_SOURCE, et_day):
                            continue

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
            print(f"[AUTO] ERROR: {e}")

        time.sleep(POLL_SECONDS)


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

                        # Skip weekends (no trading)
                        if et_day.weekday() >= 5:
                            continue

                        # Skip non-trading days/holidays where there are no candles at all
                        # (prevents false "390 missing" alarms)
                        if not _day_has_any_candles(sb, TABLE_CANDLES, et_day):
                            continue

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

    if MODE == "auto":
        run_auto(sb)
    elif MODE == "backfill":
        run_backfill(sb)
    elif MODE == "live":
        run_live(sb)
    elif MODE in ("daily_once", "weekly_once"):
        run_once(sb)
    elif MODE == "build_intraday_tfs":
        run_build_intraday_tfs(sb)
    else:
        raise SystemExit("MODE must be 'auto', 'backfill', 'live', 'daily_once', 'weekly_once', or 'build_intraday_tfs'.")


if __name__ == "__main__":
    main()
