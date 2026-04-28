from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.storage import ensure_dir


def today_dir(base: Path) -> Path:
    path = base / dt.date.today().strftime("%Y%m%d")
    ensure_dir(path)
    return path


def parquet_append(
    df_new: pd.DataFrame,
    path: Path,
    key_cols: list[str],
    sort_cols: list[str],
    min_bar_date: str | None = None,
) -> tuple[int, int]:
    ensure_dir(path.parent)
    old_len = 0
    if path.exists():
        df_old = pd.read_parquet(path)
        old_len = len(df_old)
        merged = pd.concat([df_old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    if "close" in merged.columns:
        merged = merged[pd.to_numeric(merged["close"], errors="coerce").notna()].copy()
    if "suspendFlag" in merged.columns:
        merged = merged[merged["suspendFlag"].astype(str) != "1"].copy()
    if min_bar_date and "bar_time" in merged.columns:
        bar_dates = merged["bar_time"].astype(str).str.replace(".0", "", regex=False).str.slice(0, 8)
        merged = merged[bar_dates >= min_bar_date].copy()
    for col in key_cols + sort_cols:
        if col in merged.columns:
            merged[col] = merged[col].astype(str).str.replace(".0", "", regex=False)
    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)
    merged.to_parquet(path, index=False, compression="zstd")
    return old_len, len(merged)


def load_codes_from_db(db_path: Path, user_id: str | None = None) -> list[str]:
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        sql = "SELECT DISTINCT stock_id FROM stocks"
        params: tuple[object, ...] = ()
        if user_id:
            sql += " WHERE user_id = ?"
            params = (user_id,)
        sql += " ORDER BY stock_id"
        rows = conn.execute(sql, params).fetchall()
        return [str(r[0]).upper() for r in rows if r and r[0]]
    finally:
        conn.close()


def chunked(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def latest_rows_from_batch(
    xtdata,
    codes: list[str],
    period: str,
    fetch_time: str,
    count: int,
    min_bar_date: str | None = None,
) -> tuple[list[pd.Series], list[str]]:
    rows: list[pd.Series] = []
    missing: list[str] = []
    data = xtdata.get_market_data_ex([], codes, period, "", "", max(1, int(count)), "none", True)
    for code in codes:
        df = data.get(code, pd.DataFrame())
        if df is None or df.empty:
            missing.append(code)
            continue
        tail = df.copy().reset_index().rename(columns={"index": "bar_time"})
        price_cols = [col for col in ["open", "high", "low", "close"] if col in tail.columns]
        if price_cols:
            valid_prices = tail[price_cols].apply(pd.to_numeric, errors="coerce").notna().all(axis=1)
            tail = tail[valid_prices].copy()
            if tail.empty:
                missing.append(code)
                continue
        if "suspendFlag" in tail.columns:
            tail = tail[tail["suspendFlag"].astype(str) != "1"].copy()
            if tail.empty:
                missing.append(code)
                continue
        if min_bar_date and "bar_time" in tail.columns:
            bar_dates = tail["bar_time"].astype(str).str.replace(".0", "", regex=False).str.slice(0, 8)
            tail = tail[bar_dates >= min_bar_date].copy()
            if tail.empty:
                missing.append(code)
                continue
        tail.insert(0, "ts_code", code)
        tail["fetch_time"] = fetch_time
        rows.extend([row for _, row in tail.iterrows()])
    return rows, missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch the latest minute bar for selected stocks. "
            "Default stock source is stock_analyst.db -> stocks.stock_id."
        )
    )
    parser.add_argument("--period", default="1m", choices=["1m", "5m", "10m"], help="recommended default is 1m")
    parser.add_argument("--ts-code", nargs="*", help="explicit stock codes, e.g. 000001.SZ 600519.SH")
    parser.add_argument("--db-path", default="stock_analyst.db")
    parser.add_argument("--user-id", help="optional filter on stocks.user_id in stock_analyst.db")
    parser.add_argument("--chunk-size", type=int, default=200, help="stock list chunk size")
    parser.add_argument("--count", type=int, default=120, help="recent minute bars per stock")
    parser.add_argument("--out-dir", default="data/qmt_selected_minutes")
    parser.add_argument(
        "--repair-missing",
        action="store_true",
        help="download history only for missing codes, then refetch the latest bar",
    )
    parser.add_argument(
        "--download-history",
        action="store_true",
        help="download history for all selected codes before fetch; slower but more stable",
    )
    args = parser.parse_args()

    codes = [c.upper() for c in (args.ts_code or []) if c]
    if not codes:
        codes = load_codes_from_db(Path(args.db_path), args.user_id)
    if not codes:
        raise SystemExit("No stock codes found from args or stock_analyst.db")

    fetch_time = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    out_path = today_dir(Path(args.out_dir)) / f"qmt_latest_{args.period}.parquet"

    import xtquant.xtdata as xtdata

    t0 = time.perf_counter()
    rows: list[pd.Series] = []
    missing_all: list[str] = []
    min_bar_date = dt.date.today().strftime("%Y%m%d") if args.repair_missing and dt.date.today().weekday() < 5 else None

    xtdata.connect()
    try:
        if args.download_history:
            start_s = (dt.date.today() - dt.timedelta(days=2)).strftime("%Y%m%d")
            end_s = dt.date.today().strftime("%Y%m%d")
            for i, code in enumerate(codes, 1):
                xtdata.download_history_data(code, args.period, start_s, end_s)
                if i % 100 == 0 or i == len(codes):
                    print(f"[qmt-latest] preload {i}/{len(codes)}")

        for idx, group in enumerate(chunked(codes, max(1, int(args.chunk_size))), 1):
            batch_rows, batch_missing = latest_rows_from_batch(
                xtdata,
                group,
                args.period,
                fetch_time,
                count=args.count,
                min_bar_date=min_bar_date,
            )
            rows.extend(batch_rows)
            missing_all.extend(batch_missing)
            print(f"[qmt-latest] chunk={idx} done codes={min(idx * int(args.chunk_size), len(codes))}/{len(codes)}")

        if args.repair_missing and missing_all:
            print(f"[qmt-latest] repair start missing={len(missing_all)}")
            start_s = (dt.date.today() - dt.timedelta(days=2)).strftime("%Y%m%d")
            end_s = dt.date.today().strftime("%Y%m%d")
            for i, code in enumerate(missing_all, 1):
                xtdata.download_history_data(code, args.period, start_s, end_s)
                if i % 100 == 0 or i == len(missing_all):
                    print(f"[qmt-latest] repair preload {i}/{len(missing_all)}")

            repaired_rows: list[pd.Series] = []
            still_missing = 0
            for idx, group in enumerate(chunked(missing_all, max(1, int(args.chunk_size))), 1):
                batch_rows, batch_missing = latest_rows_from_batch(
                    xtdata,
                    group,
                    args.period,
                    fetch_time,
                    count=args.count,
                    min_bar_date=min_bar_date,
                )
                repaired_rows.extend(batch_rows)
                still_missing += len(batch_missing)
                print(
                    f"[qmt-latest] repair chunk={idx} done "
                    f"codes={min(idx * int(args.chunk_size), len(missing_all))}/{len(missing_all)}"
                )
            rows.extend(repaired_rows)
            print(f"[qmt-latest] repair recovered={len(repaired_rows)} still_missing={still_missing}")
    finally:
        try:
            xtdata.disconnect()
        except Exception:
            pass

    snapshot = pd.DataFrame(rows)
    hit_codes = set()
    if not snapshot.empty:
        snapshot["ts_code"] = snapshot["ts_code"].astype(str)
        hit_codes = set(snapshot["ts_code"].str.upper())
        snapshot["bar_time"] = snapshot["bar_time"].astype(str).str.replace(".0", "", regex=False)
        snapshot["fetch_time"] = snapshot["fetch_time"].astype(str)
        old_len, new_len = parquet_append(
            snapshot,
            out_path,
            ["ts_code", "bar_time"],
            ["fetch_time", "ts_code", "bar_time"],
            min_bar_date=min_bar_date,
        )
    else:
        old_len, new_len = (0, 0)

    elapsed = time.perf_counter() - t0
    print(
        f"[qmt-latest] done period={args.period} total_codes={len(codes)} "
        f"hit_codes={len(hit_codes)} miss_codes={len(codes) - len(hit_codes)} hit_rows={len(snapshot)} "
        f"file_rows={new_len} elapsed_sec={elapsed:.2f} path={out_path.as_posix()}"
    )


if __name__ == "__main__":
    main()
