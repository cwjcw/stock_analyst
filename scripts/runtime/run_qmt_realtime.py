from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from stock_analyst.config import RUNTIME
from stock_analyst.db import connect_db
from stock_analyst.indicators import compute_indicators, row_to_json, summarize_signal


def _to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def get_user_and_watchlist(conn, user_code: str) -> tuple[int, list[str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE user_code=%s AND is_active=1", (user_code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"user not found or inactive: {user_code}")
        user_id = int(row[0])
        cur.execute(
            """
            SELECT ts_code FROM user_watchlist
            WHERE user_id=%s AND is_active=1
            ORDER BY ts_code
            """,
            (user_id,),
        )
        codes = [r[0] for r in cur.fetchall()]
    return user_id, codes


def load_history(conn, ts_code: str, bars: int = 500) -> pd.DataFrame:
    sql = """
    SELECT
      q.ts_code,
      q.trade_date,
      q.open, q.high, q.low, q.close, q.pre_close, q.vol, q.amount,
      b.turnover_rate, b.volume_ratio, b.pe_ttm, b.pb, b.total_mv, b.circ_mv,
      l.up_limit, l.down_limit
    FROM daily_quotes q
    LEFT JOIN daily_basic b ON b.ts_code=q.ts_code AND b.trade_date=q.trade_date
    LEFT JOIN stk_limit l ON l.ts_code=q.ts_code AND l.trade_date=q.trade_date
    WHERE q.ts_code=%s
    ORDER BY q.trade_date DESC
    LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ts_code, bars))
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    if not rows:
        return pd.DataFrame(columns=[
            "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount",
            "turnover_rate", "volume_ratio", "pe_ttm", "pb", "total_mv", "circ_mv", "up_limit", "down_limit",
        ])
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    return df.sort_values("trade_date").reset_index(drop=True)


def merge_realtime_bar(hist_df: pd.DataFrame, ts_code: str, tick: dict) -> pd.DataFrame:
    if hist_df.empty:
        return hist_df

    today = dt.date.today()
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
        "turnover_rate": None,
        "volume_ratio": None,
        "pe_ttm": None,
        "pb": None,
        "total_mv": None,
        "circ_mv": None,
        "up_limit": None,
        "down_limit": None,
    }
    out = hist_df.copy()
    if len(out) > 0 and pd.to_datetime(out.iloc[-1]["trade_date"]).date() == today:
        for k, v in rt_row.items():
            out.at[out.index[-1], k] = v
    else:
        out = pd.concat([out, pd.DataFrame([rt_row])], ignore_index=True)
    return out


def persist_tick(conn, user_id: int, ts_code: str, tick: dict) -> None:
    bid_prices = tick.get("bidPrice", [])
    ask_prices = tick.get("askPrice", [])
    bid_vols = tick.get("bidVol", [])
    ask_vols = tick.get("askVol", [])
    tick_time = tick.get("timetag")
    if tick_time:
        tick_dt = dt.datetime.strptime(tick_time, "%Y%m%d %H:%M:%S")
    elif tick.get("time"):
        tick_dt = dt.datetime.fromtimestamp(int(tick["time"]) / 1000)
    else:
        tick_dt = dt.datetime.now()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO realtime_ticks (
              user_id, ts_code, tick_time, last_price, open, high, low, last_close, amount, volume,
              bid1, ask1, bid1_vol, ask1_vol
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                user_id,
                ts_code,
                tick_dt,
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


def persist_snapshot(conn, user_id: int, ts_code: str, last_row: pd.Series) -> None:
    trend, summary = summarize_signal(last_row)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO indicator_snapshot (user_id, ts_code, calc_time, last_price, trend_direction, signal_summary, indicators_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              calc_time=VALUES(calc_time),
              last_price=VALUES(last_price),
              trend_direction=VALUES(trend_direction),
              signal_summary=VALUES(signal_summary),
              indicators_json=VALUES(indicators_json)
            """,
            (
                user_id,
                ts_code,
                dt.datetime.now(),
                _to_float(last_row.get("close", 0)),
                trend,
                summary,
                row_to_json(last_row),
            ),
        )
    conn.commit()


def parse_tick_payload(raw: dict) -> list[tuple[str, dict]]:
    out = []
    for code, tick in raw.items():
        if isinstance(tick, dict):
            out.append((code, tick))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Read QMT realtime ticks and compute indicators by user watchlist")
    parser.add_argument("--user-code", default=RUNTIME.default_user_code)
    parser.add_argument("--interval", type=float, default=3.0, help="poll interval seconds")
    parser.add_argument("--bars", type=int, default=500, help="history bars for indicator calculation")
    args = parser.parse_args()

    import xtquant.xtdata as xtdata

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
                hist = load_history(conn, ts_code=code, bars=args.bars)
                if hist.empty:
                    print(f"[{now}] {code} no_history")
                    continue
                merged = merge_realtime_bar(hist, ts_code=code, tick=tick)
                calc_df = compute_indicators(merged)
                last = calc_df.iloc[-1]
                persist_snapshot(conn, user_id=user_id, ts_code=code, last_row=last)
                trend, summary = summarize_signal(last)
                print(
                    f"[{now}] {code} last={_to_float(last.get('close', 0)):.3f} trend={trend} summary={summary}"
                )
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
