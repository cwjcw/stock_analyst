from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        tok = vals.get("TUSHARE_TOKEN") or vals.get("\ufeffTUSHARE_TOKEN") or ""
        return str(tok).strip().strip('"').strip("'")
    return ""


def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def parse_ymd(s: str) -> dt.date:
    return dt.datetime.strptime(str(s)[:8], "%Y%m%d").date()


def shift_ymd(s: str, days: int) -> str:
    return ymd(parse_ymd(s) + dt.timedelta(days=days))


def stock_paths(root_out_dir: Path, ts_code: str) -> dict[str, Path]:
    stock_dir = root_out_dir / ts_code
    ensure_dir(stock_dir)
    return {
        "tushare_daily": stock_dir / f"{ts_code}_tushare_daily.parquet",
        "tushare_moneyflow_dc": stock_dir / f"{ts_code}_tushare_moneyflow_dc.parquet",
        "qmt_10min": stock_dir / f"{ts_code}_qmt_10min.parquet",
    }


def read_last_ymd(path: Path, date_col: str) -> str | None:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df.empty or date_col not in df.columns:
        return None
    vals = df[date_col].dropna().astype(str)
    if vals.empty:
        return None
    vals = vals.str.replace("-", "", regex=False).str.slice(0, 8)
    vals = vals[vals.str.fullmatch(r"\d{8}")]
    if vals.empty:
        return None
    return vals.max()


def upsert_parquet(df_new: pd.DataFrame, path: Path, key_cols: list[str], sort_col: str) -> tuple[int, int]:
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
    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    if sort_col in merged.columns:
        merged[sort_col] = merged[sort_col].astype(str).str.replace(".0", "", regex=False)
        merged = merged.sort_values(sort_col).reset_index(drop=True)
    merged.to_parquet(path, index=False, compression="zstd")
    new_len = len(merged)
    return old_len, new_len


def list_all_ts_codes() -> list[str]:
    token = load_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN")
    pro = ts.pro_api(token)
    for i in range(5):
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code")
        if df is not None and not df.empty and "ts_code" in df.columns:
            return sorted(df["ts_code"].dropna().astype(str).str.upper().unique().tolist())
        time.sleep(1.5 + i)
    return []


def export_one_stock(
    ts_code: str,
    root_out_dir: Path,
    daily_days: int,
    minute_days: int,
    overlap_days: int,
    full_refresh: bool,
    qmt_lock: threading.Lock,
) -> dict:
    token = load_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN")
    pro = ts.pro_api(token)

    end_d = dt.date.today()
    default_daily_start = ymd(end_d - dt.timedelta(days=int(daily_days * 1.6)))
    default_minute_start = ymd(end_d - dt.timedelta(days=minute_days))
    end_s = ymd(end_d)

    paths = stock_paths(root_out_dir, ts_code)
    result: dict[str, dict] = {}

    # 1) Tushare daily incremental
    last_daily = read_last_ymd(paths["tushare_daily"], "trade_date")
    start_daily = default_daily_start
    if (not full_refresh) and last_daily:
        start_daily = max(default_daily_start, shift_ymd(last_daily, -abs(overlap_days)))
    df_daily = pro.daily(
        ts_code=ts_code,
        start_date=start_daily,
        end_date=end_s,
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
    )
    if df_daily is None or df_daily.empty or "trade_date" not in df_daily.columns:
        ts.set_token(token)
        df_daily = ts.pro_bar(
            ts_code=ts_code,
            start_date=start_daily,
            end_date=end_s,
            freq="D",
            asset="E",
            adj=None,
        )
    if df_daily is None or df_daily.empty:
        df_daily = pd.DataFrame(columns=["ts_code", "trade_date"])
    old_len, new_len = upsert_parquet(df_daily, paths["tushare_daily"], ["ts_code", "trade_date"], "trade_date")
    result["tushare_daily"] = {
        "path": paths["tushare_daily"].as_posix(),
        "old_rows": old_len,
        "new_rows": new_len,
        "added_rows": new_len - old_len,
        "start": start_daily,
        "end": end_s,
    }

    # 2) Tushare moneyflow_dc incremental (Eastmoney historical flow)
    last_mf = read_last_ymd(paths["tushare_moneyflow_dc"], "trade_date")
    start_mf = default_minute_start
    if (not full_refresh) and last_mf:
        start_mf = max(default_minute_start, shift_ymd(last_mf, -abs(overlap_days)))
    # moneyflow_dc starts from 20230911
    start_mf = max(start_mf, "20230911")
    df_mf = pro.moneyflow_dc(ts_code=ts_code, start_date=start_mf, end_date=end_s)
    if df_mf is None or df_mf.empty:
        df_mf = pd.DataFrame(columns=["ts_code", "trade_date"])
    old_len, new_len = upsert_parquet(df_mf, paths["tushare_moneyflow_dc"], ["ts_code", "trade_date"], "trade_date")
    result["tushare_moneyflow_dc"] = {
        "path": paths["tushare_moneyflow_dc"].as_posix(),
        "old_rows": old_len,
        "new_rows": new_len,
        "added_rows": new_len - old_len,
        "start": start_mf,
        "end": end_s,
    }

    # 3) QMT 10m incremental
    last_qmt = read_last_ymd(paths["qmt_10min"], "trade_time")
    start_qmt = default_minute_start
    if (not full_refresh) and last_qmt:
        start_qmt = max(default_minute_start, shift_ymd(last_qmt, -abs(overlap_days)))

    import xtquant.xtdata as xtdata

    with qmt_lock:
        xtdata.connect()
        try:
            xtdata.download_history_data(ts_code, "10m", start_qmt, end_s)
            data = xtdata.get_market_data_ex([], [ts_code], "10m", start_qmt, end_s, -1, "none", True)
        finally:
            try:
                xtdata.disconnect()
            except Exception:
                pass
    df_qmt = data.get(ts_code, pd.DataFrame()).reset_index().rename(columns={"index": "trade_time"})
    if df_qmt is None or df_qmt.empty:
        df_qmt = pd.DataFrame(columns=["ts_code", "trade_time"])
    else:
        if "ts_code" not in df_qmt.columns:
            df_qmt.insert(0, "ts_code", ts_code)
    old_len, new_len = upsert_parquet(df_qmt, paths["qmt_10min"], ["ts_code", "trade_time"], "trade_time")
    result["qmt_10min"] = {
        "path": paths["qmt_10min"].as_posix(),
        "old_rows": old_len,
        "new_rows": new_len,
        "added_rows": new_len - old_len,
        "start": start_qmt,
        "end": end_s,
    }

    return result


def print_one(ts_code: str, res: dict) -> None:
    for name, info in res.items():
        print(
            f"[export] {ts_code} {name}: {info['path']} "
            f"rows={info['new_rows']} added={info['added_rows']} window={info['start']}-{info['end']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Parquet data: Tushare(daily,moneyflow) + QMT(10min). "
        "Use full run first, then incremental updates."
    )
    parser.add_argument("--ts-code", help="single stock, e.g. 000099.SZ")
    parser.add_argument("--all", action="store_true", help="all listed stocks")
    parser.add_argument("--daily-days", type=int, default=500, help="daily history window for first full run")
    parser.add_argument("--minute-days", type=int, default=90, help="moneyflow/10min window for first full run")
    parser.add_argument("--overlap-days", type=int, default=2, help="incremental overlap days to avoid missing bars")
    parser.add_argument("--full-refresh", action="store_true", help="force full refresh by window, ignore existing tail")
    parser.add_argument("--workers", type=int, default=8, help="thread workers in --all mode")
    parser.add_argument("--start-index", type=int, default=0, help="start index in --all mode")
    parser.add_argument("--limit", type=int, default=0, help="max stocks in --all mode (0 means all)")
    parser.add_argument("--out-dir", default="data/market_store")
    args = parser.parse_args()

    if not args.all and not args.ts_code:
        raise SystemExit("Use --ts-code <CODE> or --all")

    root_out_dir = Path(args.out_dir)
    ensure_dir(root_out_dir)
    qmt_lock = threading.Lock()

    if not args.all:
        ts_code = args.ts_code.upper()
        res = export_one_stock(
            ts_code=ts_code,
            root_out_dir=root_out_dir,
            daily_days=args.daily_days,
            minute_days=args.minute_days,
            overlap_days=args.overlap_days,
            full_refresh=args.full_refresh,
            qmt_lock=qmt_lock,
        )
        print("[export] done")
        print_one(ts_code, res)
        return

    codes = list_all_ts_codes()
    if not codes:
        raise SystemExit("No ts_code from Tushare stock_basic")
    start = max(0, int(args.start_index))
    end = len(codes) if int(args.limit) <= 0 else min(len(codes), start + int(args.limit))
    targets = codes[start:end]
    total = len(targets)
    print(f"[export] mode=all total={total} workers={args.workers} full_refresh={args.full_refresh}")

    futures = {}
    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as ex:
        for code in targets:
            fut = ex.submit(
                export_one_stock,
                code,
                root_out_dir,
                int(args.daily_days),
                int(args.minute_days),
                int(args.overlap_days),
                bool(args.full_refresh),
                qmt_lock,
            )
            futures[fut] = code

        for i, fut in enumerate(as_completed(futures), 1):
            code = futures[fut]
            try:
                res = fut.result()
                ok += 1
                print(f"[{i}/{total}] ok {code}")
                print_one(code, res)
            except Exception as e:
                fail += 1
                print(f"[{i}/{total}] fail {code}: {e}")

    print(f"[export] all done ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
