from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import pandas as pd
import tushare as ts
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.storage import ensure_dir


DEFAULT_PRICE_DAYS = 365
DEFAULT_FINANCE_DAYS = 365 * 4
DEFAULT_EVENT_DAYS = 365
DEFAULT_WORKERS = 3
RATE_LIMIT_INTERVAL = 0.35


STOCK_DATASETS = {
    "daily_basic": {
        "method": "daily_basic",
        "date_mode": "price",
        "keys": ["ts_code", "trade_date"],
        "sort": "trade_date",
    },
    "bak_daily": {
        "method": "bak_daily",
        "date_mode": "price",
        "keys": ["ts_code", "trade_date"],
        "sort": "trade_date",
    },
    "fina_indicator": {
        "method": "fina_indicator",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date"],
        "sort": "end_date",
    },
    "income": {
        "method": "income",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date", "report_type"],
        "sort": "end_date",
    },
    "balancesheet": {
        "method": "balancesheet",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date", "report_type"],
        "sort": "end_date",
    },
    "cashflow": {
        "method": "cashflow",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date", "report_type"],
        "sort": "end_date",
    },
    "top10_holders": {
        "method": "top10_holders",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "holder_name"],
        "sort": "end_date",
    },
    "top10_floatholders": {
        "method": "top10_floatholders",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "holder_name"],
        "sort": "end_date",
    },
    "forecast": {
        "method": "forecast",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date"],
        "sort": "ann_date",
    },
    "express": {
        "method": "express",
        "date_mode": "finance",
        "keys": ["ts_code", "end_date", "ann_date"],
        "sort": "ann_date",
    },
    "hsgt_top10": {
        "method": "hsgt_top10",
        "date_mode": "event",
        "keys": ["ts_code", "trade_date", "market_type"],
        "sort": "trade_date",
    },
    "limit_list_d": {
        "method": "limit_list_d",
        "date_mode": "event",
        "keys": ["ts_code", "trade_date"],
        "sort": "trade_date",
    },
}


def load_token() -> str:
    token = (os.getenv("TUSHARE_TOKEN") or "").strip().strip('"').strip("'")
    if token:
        return token
    env = ROOT / ".env"
    if env.exists():
        vals = dotenv_values(env)
        tok = vals.get("TUSHARE_TOKEN") or vals.get("\ufeffTUSHARE_TOKEN") or ""
        return str(tok).strip().strip('"').strip("'")
    return ""


def get_pro() -> ts.pro_api:
    token = load_token()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured in environment or .env")
    return ts.pro_api(token)


def ymd(day: dt.date) -> str:
    return day.strftime("%Y%m%d")


def parse_ymd(value: str) -> dt.date:
    return dt.datetime.strptime(str(value)[:8], "%Y%m%d").date()


def normalize_ts_code(value: str) -> str:
    code = str(value or "").strip().upper().replace(" ", "")
    if code.startswith(("SZ", "SH")):
        return f"{code[2:].zfill(6)}.{code[:2]}"
    if "." in code:
        symbol, market = code.split(".", 1)
        return f"{symbol.zfill(6)}.{market}"
    return f"{code.zfill(6)}.{('SH' if code.startswith(('5', '6', '9')) else 'SZ')}"


def watchlist_codes(user_id: str) -> list[str]:
    db_path = ROOT / "stock_analyst.db"
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT stock_id FROM stocks WHERE user_id=? ORDER BY stock_id",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()
    return [normalize_ts_code(row[0]) for row in rows]


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("trade_date", "ann_date", "end_date", "start_date"):
        if col in out.columns:
            out[col] = out[col].dropna().astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    return out


def filter_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = normalize_dates(df)
    date_cols = [col for col in ("trade_date", "ann_date", "end_date") if col in out.columns]
    if not date_cols:
        return out
    masks = []
    for col in date_cols:
        vals = out[col].astype(str).str.slice(0, 8)
        masks.append(vals.between(start_date, end_date))
    if not masks:
        return out
    mask = masks[0]
    for item in masks[1:]:
        mask = mask | item
    return out.loc[mask].copy()


def read_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_parquet(path))
    except Exception:
        return 0


def upsert_parquet(df_new: pd.DataFrame, path: Path, key_cols: list[str], sort_col: str) -> tuple[int, int]:
    ensure_dir(path.parent)
    df_new = normalize_dates(df_new)
    old_len = 0
    if path.exists():
        try:
            df_old = pd.read_parquet(path)
            old_len = len(df_old)
            merged = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            merged = df_new.copy()
    else:
        merged = df_new.copy()

    if merged.empty:
        return old_len, old_len

    for col in key_cols:
        if col in merged.columns:
            merged[col] = merged[col].astype(str).str.replace(".0", "", regex=False)
    existing_keys = [col for col in key_cols if col in merged.columns]
    if existing_keys:
        merged = merged.drop_duplicates(subset=existing_keys, keep="last")
    if sort_col in merged.columns:
        merged[sort_col] = merged[sort_col].astype(str).str.replace(".0", "", regex=False)
        merged = merged.sort_values(sort_col).reset_index(drop=True)
    merged.to_parquet(path, index=False, compression="zstd")
    return old_len, len(merged)


def request_with_retry(label: str, fn: Callable[[], pd.DataFrame], retries: int = 2) -> pd.DataFrame:
    for attempt in range(retries + 1):
        try:
            df = fn()
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as exc:
            if attempt >= retries:
                print(f"[ERR] {label}: {exc}")
                return pd.DataFrame()
            sleep_s = 1.5 * (attempt + 1)
            print(f"[WARN] {label}: {exc}; retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return pd.DataFrame()


def call_stock_api(pro: ts.pro_api, method: str, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    api = getattr(pro, method)
    attempts = [
        lambda: api(ts_code=ts_code, start_date=start_date, end_date=end_date),
        lambda: api(ts_code=ts_code, ann_date="", start_date=start_date, end_date=end_date),
        lambda: api(ts_code=ts_code),
    ]
    for fn in attempts:
        df = request_with_retry(f"{method} {ts_code}", fn, retries=1)
        if not df.empty:
            return df
    return pd.DataFrame()


def fetch_stock_dataset(
    pro: ts.pro_api,
    ts_code: str,
    dataset: str,
    root_out_dir: Path,
    ranges: dict[str, tuple[str, str]],
) -> dict:
    spec = STOCK_DATASETS[dataset]
    start_date, end_date = ranges[spec["date_mode"]]
    path = root_out_dir / ts_code / f"{ts_code}_tushare_{dataset}.parquet"
    df = call_stock_api(pro, spec["method"], ts_code, start_date, end_date)
    df = filter_by_date_range(df, start_date, end_date)
    if df.empty:
        rows = read_row_count(path)
        return {"dataset": dataset, "path": str(path), "rows": rows, "added": 0, "status": "empty"}
    old_len, new_len = upsert_parquet(df, path, spec["keys"], spec["sort"])
    return {"dataset": dataset, "path": str(path), "rows": new_len, "added": new_len - old_len, "status": "ok"}


def fetch_one_stock(ts_code: str, datasets: list[str], root_out_dir: Path, ranges: dict[str, tuple[str, str]]) -> dict:
    pro = get_pro()
    ts_code = normalize_ts_code(ts_code)
    results = []
    for dataset in datasets:
        results.append(fetch_stock_dataset(pro, ts_code, dataset, root_out_dir, ranges))
        time.sleep(RATE_LIMIT_INTERVAL)
    return {"ts_code": ts_code, "datasets": results}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch extended Tushare factors for stock analysis reports.")
    parser.add_argument("--user-id", default="cwjcw", help="Read stock list from local SQLite user watchlist.")
    parser.add_argument("--ts-code", action="append", help="Stock code. Can be repeated.")
    parser.add_argument("--all-watchlist", action="store_true", help="Fetch all stocks in --user-id watchlist.")
    parser.add_argument("--price-days", type=int, default=DEFAULT_PRICE_DAYS)
    parser.add_argument("--finance-days", type=int, default=DEFAULT_FINANCE_DAYS)
    parser.add_argument("--event-days", type=int, default=DEFAULT_EVENT_DAYS)
    parser.add_argument("--start-date", help="Override all start dates in YYYYMMDD.")
    parser.add_argument("--end-date", default=ymd(dt.date.today()))
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--out-dir", default="data/market_store")
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=list(STOCK_DATASETS.keys()),
        choices=sorted(STOCK_DATASETS.keys()),
        help="Datasets to fetch.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.all_watchlist and not args.ts_code:
        raise SystemExit("Use --all-watchlist or --ts-code CODE")

    end_date = args.end_date.replace("-", "")
    if args.start_date:
        start = args.start_date.replace("-", "")
        ranges = {"price": (start, end_date), "finance": (start, end_date), "event": (start, end_date)}
    else:
        end = parse_ymd(end_date)
        ranges = {
            "price": (ymd(end - dt.timedelta(days=args.price_days)), end_date),
            "finance": (ymd(end - dt.timedelta(days=args.finance_days)), end_date),
            "event": (ymd(end - dt.timedelta(days=args.event_days)), end_date),
        }

    codes = [normalize_ts_code(code) for code in (args.ts_code or [])]
    if args.all_watchlist:
        for code in watchlist_codes(args.user_id):
            if code not in codes:
                codes.append(code)
    if not codes:
        raise SystemExit("No stock codes found.")

    root_out_dir = ROOT / args.out_dir
    ensure_dir(root_out_dir)
    print(f"[range] price={ranges['price']} finance={ranges['finance']} event={ranges['event']}")
    print(f"[stocks] total={len(codes)} workers={args.workers} datasets={','.join(args.datasets)}")

    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(fetch_one_stock, code, args.datasets, root_out_dir, ranges): code
            for code in codes
        }
        for idx, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            try:
                result = future.result()
                ok += 1
                parts = [f"{item['dataset']}:{item['rows']}({item['added']:+})" for item in result["datasets"]]
                print(f"[{idx}/{len(codes)}] OK {code} " + " ".join(parts))
            except Exception as exc:
                fail += 1
                print(f"[{idx}/{len(codes)}] FAIL {code}: {exc}")

    print(f"[summary] ok={ok} fail={fail} total={len(codes)}")
    if fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
