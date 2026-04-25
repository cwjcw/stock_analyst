from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import tushare as ts
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.storage import ensure_dir


def load_token() -> str:
    token = (os.getenv("TUSHARE_TOKEN") or "").strip().strip('"').strip("'")
    if token:
        return token
    env = ROOT / ".env"
    if env.exists():
        vals = dotenv_values(env)
        return (vals.get("TUSHARE_TOKEN") or "").strip().strip('"').strip("'")
    return ""


def is_trade_day(day: dt.date) -> bool:
    token = load_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN")
    pro = ts.pro_api(token)
    df = pro.trade_cal(exchange="SSE", start_date=day.strftime("%Y%m%d"), end_date=day.strftime("%Y%m%d"))
    if df is None or df.empty:
        return False
    return str(df.iloc[0].get("is_open", "0")) == "1"


def ts_ms_to_dt(ms: int | float | str | None) -> dt.datetime | None:
    if ms in (None, "", 0, "0"):
        return None
    try:
        return dt.datetime.fromtimestamp(int(ms) / 1000)
    except Exception:
        return None


def ymd_hms(value: dt.datetime) -> str:
    return value.strftime("%Y%m%d%H%M%S")


def today_dir(base: Path) -> Path:
    p = base / dt.date.today().strftime("%Y%m%d")
    ensure_dir(p)
    return p


def parquet_upsert(df_new: pd.DataFrame, path: Path, key_cols: list[str], sort_col: str) -> tuple[int, int]:
    ensure_dir(path.parent)
    old_len = 0
    if path.exists():
        df_old = pd.read_parquet(path)
        old_len = len(df_old)
        merged = pd.concat([df_old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    for col in key_cols:
        if col in merged.columns:
            merged[col] = merged[col].astype(str).str.replace(".0", "", regex=False)
    if sort_col in merged.columns:
        merged[sort_col] = merged[sort_col].astype(str).str.replace(".0", "", regex=False)
    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    if sort_col in merged.columns:
        merged = merged.sort_values(sort_col).reset_index(drop=True)
    merged.to_parquet(path, index=False, compression="zstd")
    return old_len, len(merged)


def session_open(now: dt.datetime) -> bool:
    t = now.time()
    morning = dt.time(9, 30) <= t < dt.time(11, 30)
    afternoon = dt.time(13, 0) <= t < dt.time(15, 0)
    return morning or afternoon


def floor_10m_slot(value: dt.datetime) -> dt.datetime | None:
    t = value.time()
    if dt.time(9, 30) <= t < dt.time(11, 30):
        return value.replace(minute=(value.minute // 10) * 10, second=0, microsecond=0)
    if dt.time(13, 0) <= t < dt.time(15, 0):
        return value.replace(minute=(value.minute // 10) * 10, second=0, microsecond=0)
    return None


def slot_end(slot_start: dt.datetime) -> dt.datetime:
    return slot_start + dt.timedelta(minutes=10)


class Aggregator:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.lock = threading.Lock()
        self.latest_quotes: dict[str, dict] = {}
        self.bars: dict[tuple[str, str], dict] = {}
        self.auction_done: set[str] = set()
        self.bars_path = today_dir(out_dir) / "qmt_10m_live.parquet"
        self.auction_path = today_dir(out_dir) / "qmt_auction_snapshots.parquet"

    def on_quotes(self, datas: dict) -> None:
        now = dt.datetime.now()
        with self.lock:
            for code, quote in datas.items():
                if not isinstance(quote, dict):
                    continue
                qt = dict(quote)
                qt["fetch_time"] = ymd_hms(now)
                self.latest_quotes[code] = qt

                tick_dt = ts_ms_to_dt(qt.get("time")) or now
                slot_start = floor_10m_slot(tick_dt)
                if slot_start is None:
                    continue

                key = (code, ymd_hms(slot_start))
                price = float(qt.get("lastPrice", qt.get("open", 0)) or 0)
                volume = float(qt.get("volume", 0) or 0)
                amount = float(qt.get("amount", 0) or 0)
                bar = self.bars.get(key)
                if bar is None:
                    self.bars[key] = {
                        "ts_code": code,
                        "bar_start": ymd_hms(slot_start),
                        "bar_end": ymd_hms(slot_end(slot_start)),
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "start_volume": volume,
                        "end_volume": volume,
                        "start_amount": amount,
                        "end_amount": amount,
                        "fetch_time": qt["fetch_time"],
                    }
                else:
                    bar["high"] = max(bar["high"], price)
                    bar["low"] = min(bar["low"], price)
                    bar["close"] = price
                    bar["end_volume"] = volume
                    bar["end_amount"] = amount
                    bar["fetch_time"] = qt["fetch_time"]

    def maybe_capture_auction(self) -> None:
        now = dt.datetime.now()
        slots = [
            ("091500", dt.time(9, 15)),
            ("092000", dt.time(9, 20)),
            ("092500", dt.time(9, 25)),
        ]
        capture_rows = []
        with self.lock:
            for slot_id, target in slots:
                if slot_id in self.auction_done:
                    continue
                if now.time() < target:
                    continue
                for code, quote in self.latest_quotes.items():
                    row = {
                        "ts_code": code,
                        "slot_time": dt.date.today().strftime("%Y%m%d") + slot_id,
                        "fetch_time": quote.get("fetch_time", ymd_hms(now)),
                        "last_price": float(quote.get("lastPrice", quote.get("open", 0)) or 0),
                        "open": float(quote.get("open", 0) or 0),
                        "high": float(quote.get("high", 0) or 0),
                        "low": float(quote.get("low", 0) or 0),
                        "last_close": float(quote.get("lastClose", 0) or 0),
                        "amount": float(quote.get("amount", 0) or 0),
                        "volume": float(quote.get("volume", 0) or 0),
                    }
                    capture_rows.append(row)
                self.auction_done.add(slot_id)

        if capture_rows:
            df = pd.DataFrame(capture_rows)
            old_len, new_len = parquet_upsert(df, self.auction_path, ["ts_code", "slot_time"], "slot_time")
            print(
                f"[realtime] auction capture rows={len(df)} "
                f"file_rows={new_len} path={self.auction_path.as_posix()}"
            )

    def flush_completed_bars(self) -> None:
        now = dt.datetime.now()
        completed = []
        with self.lock:
            done_keys = []
            for key, bar in self.bars.items():
                bar_end_dt = dt.datetime.strptime(bar["bar_end"], "%Y%m%d%H%M%S")
                if now >= bar_end_dt:
                    volume = max(0.0, float(bar["end_volume"]) - float(bar["start_volume"]))
                    amount = max(0.0, float(bar["end_amount"]) - float(bar["start_amount"]))
                    completed.append(
                        {
                            "ts_code": bar["ts_code"],
                            "bar_start": bar["bar_start"],
                            "bar_end": bar["bar_end"],
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": volume,
                            "amount": amount,
                            "fetch_time": bar["fetch_time"],
                        }
                    )
                    done_keys.append(key)
            for key in done_keys:
                self.bars.pop(key, None)

        if completed:
            df = pd.DataFrame(completed)
            old_len, new_len = parquet_upsert(df, self.bars_path, ["ts_code", "bar_end"], "bar_end")
            print(
                f"[realtime] flush bars={len(df)} file_rows={new_len} "
                f"path={self.bars_path.as_posix()}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Subscribe QMT whole quotes, capture auction snapshots, and build realtime 10m bars."
    )
    parser.add_argument("--out-dir", default="data/qmt_realtime")
    parser.add_argument("--markets", nargs="*", default=["SH", "SZ"])
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--until", default="15:00")
    args = parser.parse_args()

    today = dt.date.today()
    if not is_trade_day(today):
        print(f"[realtime] {today.isoformat()} is not a trade day, exit.")
        return

    until_h, until_m = [int(x) for x in args.until.split(":")]
    until_dt = dt.datetime.combine(today, dt.time(until_h, until_m))

    import xtquant.xtdata as xtdata

    state = Aggregator(Path(args.out_dir))

    def on_data(datas):
        state.on_quotes(datas if isinstance(datas, dict) else {})

    xtdata.connect()
    seq = None
    try:
        seq = xtdata.subscribe_whole_quote(args.markets, on_data)
        print(f"[realtime] subscribe seq={seq} markets={args.markets}")
        while dt.datetime.now() <= until_dt:
            state.maybe_capture_auction()
            state.flush_completed_bars()
            time.sleep(max(0.2, args.poll_seconds))
        state.maybe_capture_auction()
        state.flush_completed_bars()
        print("[realtime] done")
    except KeyboardInterrupt:
        print("[realtime] stopped by user")
    finally:
        try:
            if seq is not None:
                xtdata.unsubscribe_quote(seq)
        except Exception:
            pass
        try:
            xtdata.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()

