from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "output"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv_utf8_sig(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def normalize_trade_date(df: pd.DataFrame, col: str = "trade_date") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = out[col].astype(str).str.replace("-", "", regex=False)
    return out


def upsert_std_csv(df_new: pd.DataFrame, std_path: Path, key_cols: list[str]) -> pd.DataFrame:
    ensure_dir(std_path.parent)
    if std_path.exists():
        df_old = pd.read_csv(std_path)
        merged = pd.concat([df_old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    if "trade_date" in merged.columns:
        merged = merged.sort_values("trade_date")
    write_csv_utf8_sig(merged, std_path)
    return merged


def raw_path_tushare(dataset: str, ts_code: str, start: str, end: str, fetch_ts: str | None = None) -> Path:
    if fetch_ts is None:
        fetch_ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    run_day = dt.date.today().strftime("%Y%m%d")
    filename = f"{dataset}_{ts_code}_{start}_{end}_{fetch_ts}.csv"
    return OUTPUT / "raw" / "tushare" / dataset / run_day / filename


def std_path_daily(ts_code: str) -> Path:
    return OUTPUT / "std" / "daily" / f"{ts_code}_daily.csv"


def std_path_moneyflow(ts_code: str) -> Path:
    return OUTPUT / "std" / "moneyflow" / f"{ts_code}_moneyflow.csv"


def signal_report_path(ts_code: str) -> Path:
    today = dt.date.today().strftime("%Y%m%d")
    return ROOT / "reports" / f"{ts_code.replace('.', '_')}_report_{today}.md"


def raw_path_qmt(dataset: str, ts_code: str, start: str, end: str, fetch_ts: str | None = None) -> Path:
    if fetch_ts is None:
        fetch_ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    run_day = dt.date.today().strftime("%Y%m%d")
    filename = f"{dataset}_{ts_code}_{start}_{end}_{fetch_ts}.csv"
    return OUTPUT / "raw" / "qmt" / dataset / run_day / filename


def std_path_qmt_5m(ts_code: str) -> Path:
    return OUTPUT / "std" / "qmt_5m" / f"{ts_code}_5m.csv"
