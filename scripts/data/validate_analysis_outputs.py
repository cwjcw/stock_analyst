from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.indicator_chain import compute_daily_chain, load_moneyflow_parquet


def normalize_ts_code(value: str) -> str:
    code = value.strip().upper()
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def read_parquet_status(path: Path, required_cols: list[str]) -> dict:
    status = {
        "path": str(path),
        "exists": path.exists(),
        "rows": 0,
        "columns": [],
        "missing_columns": required_cols,
        "ok": False,
        "error": "",
    }
    if not path.exists():
        return status
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        status["error"] = str(exc)
        return status

    status["rows"] = len(df)
    status["columns"] = list(df.columns)
    status["missing_columns"] = [col for col in required_cols if col not in df.columns]
    status["ok"] = bool(status["rows"] > 0 and not status["missing_columns"])
    return status


def print_status(label: str, status: dict) -> None:
    marker = "OK" if status["ok"] else "FAIL"
    detail = f"rows={status['rows']} missing={','.join(status['missing_columns']) or '-'}"
    if status["error"]:
        detail += f" error={status['error']}"
    print(f"[{marker}] {label}: {detail} path={status['path']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate local analysis parquet outputs.")
    parser.add_argument("--ts-code", default="000001.SZ", help="Stock code to validate.")
    parser.add_argument("--out-dir", default="data/market_store", help="Market store directory.")
    parser.add_argument(
        "--strict-qmt",
        action="store_true",
        help="Treat missing QMT 10-minute data as a failure instead of a warning.",
    )
    args = parser.parse_args()

    ts_code = normalize_ts_code(args.ts_code)
    root = ROOT / args.out_dir
    stock_dir = root / ts_code
    market_dir = root / "_market"

    checks = [
        (
            "tushare_daily",
            stock_dir / f"{ts_code}_tushare_daily.parquet",
            ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
            True,
        ),
        (
            "tushare_moneyflow",
            stock_dir / f"{ts_code}_tushare_moneyflow.parquet",
            ["ts_code", "trade_date"],
            True,
        ),
        (
            "tushare_moneyflow_dc",
            stock_dir / f"{ts_code}_tushare_moneyflow_dc.parquet",
            ["ts_code", "trade_date"],
            True,
        ),
        (
            "qmt_10min",
            stock_dir / f"{ts_code}_qmt_10min.parquet",
            ["ts_code"],
            bool(args.strict_qmt),
        ),
        (
            "moneyflow_mkt_dc",
            market_dir / "moneyflow_mkt_dc.parquet",
            ["trade_date"],
            True,
        ),
        (
            "moneyflow_ind_dc",
            market_dir / "moneyflow_ind_dc.parquet",
            ["trade_date", "content_type"],
            True,
        ),
        (
            "moneyflow_hsgt",
            market_dir / "moneyflow_hsgt.parquet",
            ["trade_date"],
            True,
        ),
    ]

    failures = 0
    for label, path, required_cols, required in checks:
        status = read_parquet_status(path, required_cols)
        if not required and not status["ok"]:
            status = {**status, "ok": True}
            print_status(f"{label} (optional)", status)
            continue
        print_status(label, status)
        if not status["ok"]:
            failures += 1

    daily = compute_daily_chain(ts_code)
    daily_ok = not daily["calc"].empty and not daily["last"].empty
    print(f"[{'OK' if daily_ok else 'FAIL'}] daily_indicator_chain: rows={len(daily['calc'])} source={daily['source']}")
    if not daily_ok:
        failures += 1

    moneyflow_df, moneyflow_source = load_moneyflow_parquet(ts_code)
    moneyflow_ok = not moneyflow_df.empty
    print(f"[{'OK' if moneyflow_ok else 'FAIL'}] moneyflow_loader: rows={len(moneyflow_df)} source={moneyflow_source}")
    if not moneyflow_ok:
        failures += 1

    if failures:
        raise SystemExit(f"validation failed: {failures} check(s) failed")
    print("[summary] validation passed")


if __name__ == "__main__":
    main()
