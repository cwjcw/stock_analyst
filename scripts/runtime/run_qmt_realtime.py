from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.config import RUNTIME
from stock_analyst.db import connect_db, init_database_and_tables
from stock_analyst.indicators import compute_indicators, row_to_json, summarize_signal


def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def get_user_and_watchlist(conn: sqlite3.Connection, user_id: str) -> tuple[str, list[str]]:
    user = conn.execute("SELECT user_id FROM user_profiles WHERE user_id=?", (user_id,)).fetchone()
    if not user:
        raise RuntimeError(f"user not found: {user_id}")
    rows = conn.execute(
        "SELECT DISTINCT stock_id FROM stocks WHERE user_id=? ORDER BY stock_id",
        (user_id,),
    ).fetchall()
    return user_id, [str(row["stock_id"]).upper() for row in rows]


def market_store_dir(ts_code: str) -> Path:
    return ROOT / "data" / "market_store" / ts_code


def load_history(ts_code: str, bars: int = 500) -> pd.DataFrame:
    path = market_store_dir(ts_code) / f"{ts_code}_tushare_daily.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return df
    df = df.sort_values("trade_date").tail(max(1, int(bars))).reset_index(drop=True)
    for col in ["open", "high", "low", "close", "pre_close", "vol", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def merge_realtime_bar(hist_df: pd.DataFrame, ts_code: str, tick: dict) -> pd.DataFrame:
    if hist_df.empty:
        return hist_df

    today = dt.date.today().strftime("%Y%m%d")
    rt_row = {
        "ts_code": ts_code,
        "trade_date": today,
        "open": _to_float(tick.get("open", 0)),
        "high": _to_float(tick.get("high", 0)),
        "low": _to_float(tick.get("low", 0)),
        "close": _to_float(tick.get("lastPrice", tick.get("price", 0))),
        "pre_close": _to_float(tick.get("lastClose", 0)),
        "vol": _to_float(tick.get("volume", 0)),
        "amount": _to_float(tick.get("amount", 0)),
    }
    out = hist_df.copy()
    if len(out) > 0 and str(out.iloc[-1]["trade_date"]).replace("-", "")[:8] == today:
        for key, value in rt_row.items():
            out.at[out.index[-1], key] = value
    else:
        out = pd.concat([out, pd.DataFrame([rt_row])], ignore_index=True)
    return out


def tick_datetime(tick: dict) -> str:
    tick_time = tick.get("timetag")
    if tick_time:
        return dt.datetime.strptime(tick_time, "%Y%m%d %H:%M:%S").isoformat(sep=" ", timespec="seconds")
    if tick.get("time"):
        return dt.datetime.fromtimestamp(int(tick["time"]) / 1000).isoformat(sep=" ", timespec="seconds")
    return dt.datetime.now().isoformat(sep=" ", timespec="seconds")


def persist_tick(conn: sqlite3.Connection, user_id: str, ts_code: str, tick: dict) -> None:
    bid_prices = tick.get("bidPrice", [])
    ask_prices = tick.get("askPrice", [])
    bid_vols = tick.get("bidVol", [])
    ask_vols = tick.get("askVol", [])

    conn.execute(
        """
        INSERT INTO realtime_ticks (
            user_id, ts_code, tick_time, last_price, open, high, low, last_close, amount, volume,
            bid1, ask1, bid1_vol, ask1_vol
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            ts_code,
            tick_datetime(tick),
            _to_float(tick.get("lastPrice", tick.get("price", 0))),
            _to_float(tick.get("open", 0)),
            _to_float(tick.get("high", 0)),
            _to_float(tick.get("low", 0)),
            _to_float(tick.get("lastClose", 0)),
            _to_float(tick.get("amount", 0)),
            _to_float(tick.get("volume", 0)),
            _to_float(bid_prices[0] if bid_prices else 0),
            _to_float(ask_prices[0] if ask_prices else 0),
            _to_float(bid_vols[0] if bid_vols else 0),
            _to_float(ask_vols[0] if ask_vols else 0),
        ),
    )
    conn.commit()


def persist_snapshot(conn: sqlite3.Connection, user_id: str, ts_code: str, last_row: pd.Series) -> None:
    trend, summary = summarize_signal(last_row)
    now = dt.datetime.now().isoformat(sep=" ", timespec="seconds")
    conn.execute(
        """
        INSERT INTO indicator_snapshot
            (user_id, ts_code, calc_time, last_price, trend_direction, signal_summary, indicators_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, ts_code) DO UPDATE SET
            calc_time=excluded.calc_time,
            last_price=excluded.last_price,
            trend_direction=excluded.trend_direction,
            signal_summary=excluded.signal_summary,
            indicators_json=excluded.indicators_json,
            updated_at=excluded.updated_at
        """,
        (
            user_id,
            ts_code,
            now,
            _to_float(last_row.get("close", 0)),
            trend,
            summary,
            row_to_json(last_row),
            now,
        ),
    )
    conn.commit()


def parse_tick_payload(raw: dict) -> list[tuple[str, dict]]:
    return [(code, tick) for code, tick in raw.items() if isinstance(tick, dict)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Read QMT realtime ticks and compute indicators from local data")
    parser.add_argument("--user-code", default=RUNTIME.default_user_code, help="local SQLite user_id")
    parser.add_argument("--interval", type=float, default=3.0, help="poll interval seconds")
    parser.add_argument("--bars", type=int, default=500, help="history bars for indicator calculation")
    args = parser.parse_args()

    import xtquant.xtdata as xtdata

    init_database_and_tables()
    conn = connect_db()
    user_id, codes = get_user_and_watchlist(conn, args.user_code)
    if not codes:
        raise RuntimeError(f"watchlist is empty for user={args.user_code}")

    print(f"[qmt] connect session={RUNTIME.qmt_session_path}")
    xtdata.connect()
    print(f"[qmt] user={args.user_code} codes={codes}")
    try:
        while True:
            raw = xtdata.get_full_tick(codes)
            pairs = parse_tick_payload(raw if isinstance(raw, dict) else {})
            now = dt.datetime.now().strftime("%H:%M:%S")
            for code, tick in pairs:
                persist_tick(conn, user_id=user_id, ts_code=code, tick=tick)
                hist = load_history(ts_code=code, bars=args.bars)
                if hist.empty:
                    print(f"[{now}] {code} no_local_history")
                    continue
                merged = merge_realtime_bar(hist, ts_code=code, tick=tick)
                calc_df = compute_indicators(merged)
                last = calc_df.iloc[-1]
                persist_snapshot(conn, user_id=user_id, ts_code=code, last_row=last)
                trend, summary = summarize_signal(last)
                print(f"[{now}] {code} last={_to_float(last.get('close', 0)):.3f} trend={trend} summary={summary}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("[qmt] stopped by user")
    finally:
        try:
            xtdata.disconnect()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
