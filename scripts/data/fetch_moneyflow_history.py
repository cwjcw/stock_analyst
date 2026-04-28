"""
Fetch Tushare money-flow datasets for stock analysis.

Default scope is the latest 365 calendar days:

1. moneyflow_dc: Eastmoney individual stock money flow.
2. moneyflow_ind_dc: Eastmoney industry and concept board money flow.
3. moneyflow_mkt_dc: Eastmoney broad-market money flow.
4. moneyflow_hsgt: Shanghai/Shenzhen/Hong Kong Stock Connect money flow.

Output layout follows the existing market store:

data/market_store/
  {ts_code}/{ts_code}_tushare_moneyflow_dc.parquet
  _market/moneyflow_ind_dc.parquet
  _market/moneyflow_mkt_dc.parquet
  _market/moneyflow_hsgt.parquet
  _market/moneyflow_manifest.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
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


DEFAULT_DAYS = 365
DEFAULT_WORKERS = 4
DEFAULT_OVERLAP_DAYS = 3
RATE_LIMIT_INTERVAL = 0.35
MARKET_DIR_NAME = "_market"
MONEYFLOW_DC_MIN_START = "20230911"
BOARD_CONTENT_TYPES = ("行业", "概念")


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


def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def parse_ymd(value: str) -> dt.date:
    return dt.datetime.strptime(str(value)[:8], "%Y%m%d").date()


def shift_ymd(value: str, days: int) -> str:
    return ymd(parse_ymd(value) + dt.timedelta(days=days))


def normalize_ts_code(value: str) -> str:
    code = value.strip().upper()
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def default_start_date(days: int) -> str:
    return ymd(dt.date.today() - dt.timedelta(days=int(days)))


def read_last_ymd(path: Path, date_col: str = "trade_date") -> str | None:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df.empty or date_col not in df.columns:
        return None
    vals = df[date_col].dropna().astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    vals = vals[vals.str.fullmatch(r"\d{8}")]
    return vals.max() if not vals.empty else None


def read_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_parquet(path))
    except Exception:
        return 0


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("trade_date", "ann_date", "end_date"):
        if col in out.columns:
            out[col] = out[col].dropna().astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    return out


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


def fetch_by_window(
    label: str,
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str, str], pd.DataFrame],
    window_days: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    cur_start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    while cur_start <= end:
        cur_end = min(end, cur_start + dt.timedelta(days=window_days - 1))
        start_s = ymd(cur_start)
        end_s = ymd(cur_end)
        df = request_with_retry(f"{label} {start_s}-{end_s}", lambda: fetch_fn(start_s, end_s))
        if not df.empty:
            frames.append(df)
            print(f"  {label} {start_s}-{end_s}: +{len(df)}")
        else:
            print(f"  {label} {start_s}-{end_s}: empty")
        time.sleep(RATE_LIMIT_INTERVAL)
        cur_start = cur_end + dt.timedelta(days=1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def list_all_ts_codes(pro: ts.pro_api) -> list[str]:
    df = request_with_retry(
        "stock_basic",
        lambda: pro.stock_basic(exchange="", list_status="L", fields="ts_code"),
        retries=4,
    )
    if df.empty or "ts_code" not in df.columns:
        return []
    return sorted(df["ts_code"].dropna().astype(str).str.upper().unique().tolist())


def export_stock_moneyflow_dc(
    ts_code: str,
    root_out_dir: Path,
    start_date: str,
    end_date: str,
    full_refresh: bool,
    overlap_days: int,
) -> dict:
    pro = get_pro()
    ts_code = normalize_ts_code(ts_code)
    stock_dir = root_out_dir / ts_code
    ensure_dir(stock_dir)

    path = stock_dir / f"{ts_code}_tushare_moneyflow_dc.parquet"
    base_start = max(start_date, MONEYFLOW_DC_MIN_START)
    if full_refresh:
        fetch_start = base_start
    else:
        last = read_last_ymd(path)
        fetch_start = max(base_start, shift_ymd(last, -abs(overlap_days))) if last else base_start

    if fetch_start > end_date:
        rows = read_row_count(path)
        return {"dataset": "moneyflow_dc", "path": str(path), "old": rows, "new": rows, "added": 0, "window": ""}

    df = request_with_retry(
        f"moneyflow_dc {ts_code}",
        lambda: pro.moneyflow_dc(ts_code=ts_code, start_date=fetch_start, end_date=end_date),
    )
    if df.empty:
        rows = read_row_count(path)
        return {"dataset": "moneyflow_dc", "path": str(path), "old": rows, "new": rows, "added": 0, "window": f"{fetch_start}-{end_date}"}

    old_len, new_len = upsert_parquet(df, path, ["ts_code", "trade_date"], "trade_date")
    return {
        "dataset": "moneyflow_dc",
        "path": str(path),
        "old": old_len,
        "new": new_len,
        "added": new_len - old_len,
        "window": f"{fetch_start}-{end_date}",
    }


def export_market_dataset(
    pro: ts.pro_api,
    path: Path,
    dataset: str,
    fetch_fn: Callable[[str, str], pd.DataFrame],
    key_cols: list[str],
    start_date: str,
    end_date: str,
    full_refresh: bool,
    overlap_days: int,
    window_days: int,
) -> dict:
    if full_refresh:
        fetch_start = start_date
    else:
        last = read_last_ymd(path)
        fetch_start = max(start_date, shift_ymd(last, -abs(overlap_days))) if last else start_date
    if fetch_start > end_date:
        rows = read_row_count(path)
        return {"dataset": dataset, "path": str(path), "old": rows, "new": rows, "added": 0, "window": ""}

    print(f"[market] {dataset} {fetch_start} -> {end_date}")
    df = fetch_by_window(dataset, fetch_start, end_date, fetch_fn, window_days)
    if df.empty:
        rows = read_row_count(path)
        return {"dataset": dataset, "path": str(path), "old": rows, "new": rows, "added": 0, "window": f"{fetch_start}-{end_date}"}
    old_len, new_len = upsert_parquet(df, path, key_cols, "trade_date")
    return {
        "dataset": dataset,
        "path": str(path),
        "old": old_len,
        "new": new_len,
        "added": new_len - old_len,
        "window": f"{fetch_start}-{end_date}",
    }


def fetch_moneyflow_ind_dc(pro: ts.pro_api, start_date: str, end_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for content_type in BOARD_CONTENT_TYPES:
        def fetch_one(start_s: str, end_s: str, ctype: str = content_type) -> pd.DataFrame:
            return pro.moneyflow_ind_dc(start_date=start_s, end_date=end_s, content_type=ctype)

        df = fetch_by_window(f"moneyflow_ind_dc[{content_type}]", start_date, end_date, fetch_one, 31)
        if not df.empty:
            df["content_type"] = content_type
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_moneyflow_hsgt(pro: ts.pro_api, start_date: str, end_date: str) -> pd.DataFrame:
    # 300 rows per request is enough for a year, but monthly chunks keep the function reusable.
    return fetch_by_window(
        "moneyflow_hsgt",
        start_date,
        end_date,
        lambda start_s, end_s: pro.moneyflow_hsgt(start_date=start_s, end_date=end_s),
        31,
    )


def export_market_moneyflows(
    root_out_dir: Path,
    start_date: str,
    end_date: str,
    full_refresh: bool,
    overlap_days: int,
) -> list[dict]:
    pro = get_pro()
    market_dir = root_out_dir / MARKET_DIR_NAME
    ensure_dir(market_dir)

    results: list[dict] = []
    results.append(
        export_market_dataset(
            pro=pro,
            path=market_dir / "moneyflow_mkt_dc.parquet",
            dataset="moneyflow_mkt_dc",
            fetch_fn=lambda start_s, end_s: pro.moneyflow_mkt_dc(start_date=start_s, end_date=end_s),
            key_cols=["trade_date"],
            start_date=start_date,
            end_date=end_date,
            full_refresh=full_refresh,
            overlap_days=overlap_days,
            window_days=180,
        )
    )
    results.append(
        export_market_dataset(
            pro=pro,
            path=market_dir / "moneyflow_ind_dc.parquet",
            dataset="moneyflow_ind_dc",
            fetch_fn=lambda start_s, end_s: fetch_moneyflow_ind_dc(pro, start_s, end_s),
            key_cols=["trade_date", "ts_code", "name", "content_type"],
            start_date=start_date,
            end_date=end_date,
            full_refresh=full_refresh,
            overlap_days=overlap_days,
            window_days=180,
        )
    )
    results.append(
        export_market_dataset(
            pro=pro,
            path=market_dir / "moneyflow_hsgt.parquet",
            dataset="moneyflow_hsgt",
            fetch_fn=lambda start_s, end_s: fetch_moneyflow_hsgt(pro, start_s, end_s),
            key_cols=["trade_date"],
            start_date=start_date,
            end_date=end_date,
            full_refresh=full_refresh,
            overlap_days=overlap_days,
            window_days=180,
        )
    )
    write_manifest(market_dir, start_date, end_date, results)
    return results


def write_manifest(market_dir: Path, start_date: str, end_date: str, results: list[dict]) -> None:
    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "datasets": results,
        "notes": [
            "moneyflow_dc starts from 20230911 in Tushare.",
            "moneyflow_ind_dc currently fetches Eastmoney industry and concept boards.",
            "Rows are upserted by date and dataset-specific keys.",
        ],
    }
    (market_dir / "moneyflow_manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def print_result(prefix: str, info: dict) -> None:
    print(
        f"{prefix} {info['dataset']}: rows={info['new']} added={info['added']} "
        f"window={info.get('window', '')} path={info['path']}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch one-year Tushare money-flow data for stock analysis."
    )
    parser.add_argument("--ts-code", action="append", help="Stock code, e.g. 000001.SZ. Can be repeated.")
    parser.add_argument("--all", action="store_true", help="Fetch moneyflow_dc for all listed A shares.")
    parser.add_argument("--market-only", action="store_true", help="Only fetch market, board, and HSGT datasets.")
    parser.add_argument("--stock-only", action="store_true", help="Only fetch stock-level moneyflow_dc.")
    parser.add_argument("--start-date", help="Start date in YYYYMMDD. Defaults to today minus --days.")
    parser.add_argument("--end-date", default=ymd(dt.date.today()), help="End date in YYYYMMDD.")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"Default lookback days ({DEFAULT_DAYS}).")
    parser.add_argument("--full-refresh", action="store_true", help="Ignore existing tails inside the chosen window.")
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--start-index", type=int, default=0, help="Start offset in --all mode.")
    parser.add_argument("--limit", type=int, default=0, help="Max stocks in --all mode; 0 means no limit.")
    parser.add_argument("--out-dir", default="data/market_store")
    args = parser.parse_args()

    if not args.all and not args.ts_code and not args.market_only:
        parser.error("Choose --all, --ts-code CODE, or --market-only")
    if args.market_only and args.stock_only:
        parser.error("--market-only and --stock-only cannot be used together")

    start_date = (args.start_date or default_start_date(args.days)).replace("-", "")
    end_date = args.end_date.replace("-", "")
    if parse_ymd(start_date) > parse_ymd(end_date):
        parser.error("--start-date must be <= --end-date")

    root_out_dir = ROOT / args.out_dir
    ensure_dir(root_out_dir)
    print(f"[range] {start_date} -> {end_date}")
    print(f"[out] {root_out_dir}")

    if not args.stock_only:
        market_results = export_market_moneyflows(
            root_out_dir=root_out_dir,
            start_date=start_date,
            end_date=end_date,
            full_refresh=args.full_refresh,
            overlap_days=args.overlap_days,
        )
        for info in market_results:
            print_result("[done]", info)

    if args.market_only:
        return

    pro = get_pro()
    if args.all:
        codes = list_all_ts_codes(pro)
        if not codes:
            raise SystemExit("No listed stock codes returned by Tushare stock_basic.")
        start_idx = max(0, int(args.start_index))
        end_idx = len(codes) if int(args.limit) <= 0 else min(len(codes), start_idx + int(args.limit))
        targets = codes[start_idx:end_idx]
    else:
        targets = [normalize_ts_code(code) for code in args.ts_code or []]

    total = len(targets)
    print(f"[stocks] total={total} workers={args.workers}")
    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                export_stock_moneyflow_dc,
                code,
                root_out_dir,
                start_date,
                end_date,
                args.full_refresh,
                args.overlap_days,
            ): code
            for code in targets
        }
        for idx, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            try:
                info = future.result()
                ok += 1
                print_result(f"[{idx}/{total}] {code}", info)
            except Exception as exc:
                fail += 1
                print(f"[{idx}/{total}] {code} failed: {exc}")
    print(f"[summary] stocks ok={ok} fail={fail} total={total}")


if __name__ == "__main__":
    main()
