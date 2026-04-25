from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .indicators import compute_indicators, summarize_signal
from .storage import ensure_dir


ROOT = Path(__file__).resolve().parents[2]


def market_store_dir(ts_code: str) -> Path:
    return ROOT / "data" / "market_store" / ts_code


def selected_minutes_dir() -> Path:
    return ROOT / "data" / "qmt_selected_minutes"


def report_path(ts_code: str) -> Path:
    today = dt.date.today().strftime("%Y%m%d")
    return ROOT / "reports" / f"{ts_code.replace('.', '_')}_indicator_report_{today}.md"


def load_daily_parquet(ts_code: str) -> tuple[pd.DataFrame, str]:
    path = market_store_dir(ts_code) / f"{ts_code}_tushare_daily.parquet"
    if not path.exists():
        return pd.DataFrame(), path.as_posix()
    df = pd.read_parquet(path)
    if df.empty:
        return df, path.as_posix()
    df["trade_date"] = df["trade_date"].astype(str)
    return df.sort_values("trade_date").reset_index(drop=True), path.as_posix()


def load_moneyflow_parquet(ts_code: str) -> tuple[pd.DataFrame, str]:
    path = market_store_dir(ts_code) / f"{ts_code}_tushare_moneyflow.parquet"
    if not path.exists():
        return pd.DataFrame(), path.as_posix()
    df = pd.read_parquet(path)
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.sort_values("trade_date").reset_index(drop=True)
    return df, path.as_posix()


def load_minute_sequence(ts_code: str, period: str = "1m", lookback_days: int = 10) -> tuple[pd.DataFrame, str]:
    base = selected_minutes_dir()
    if not base.exists():
        return pd.DataFrame(), base.as_posix()
    day_dirs = [p for p in base.iterdir() if p.is_dir() and p.name.isdigit()]
    day_dirs = sorted(day_dirs, key=lambda p: p.name)[-max(1, int(lookback_days)) :]
    frames = []
    used_files = []
    for day_dir in day_dirs:
        path = day_dir / f"qmt_latest_{period}.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        if df.empty or "ts_code" not in df.columns:
            continue
        df = df[df["ts_code"].astype(str).str.upper() == ts_code.upper()].copy()
        if df.empty:
            continue
        used_files.append(path.as_posix())
        frames.append(df)
    if not frames:
        return pd.DataFrame(), "not_found"

    merged = pd.concat(frames, ignore_index=True)
    merged["ts_code"] = merged["ts_code"].astype(str)
    merged["bar_time"] = merged["bar_time"].astype(str).str.replace(".0", "", regex=False)
    if "fetch_time" in merged.columns:
        merged["fetch_time"] = merged["fetch_time"].astype(str).str.replace(".0", "", regex=False)
    sort_cols = ["bar_time"]
    if "fetch_time" in merged.columns:
        sort_cols = ["bar_time", "fetch_time"]
    merged = merged.sort_values(sort_cols).drop_duplicates(subset=["ts_code", "bar_time"], keep="last")
    merged = merged.sort_values("bar_time").reset_index(drop=True)
    return merged, "; ".join(used_files)


def minute_to_indicator_input(min_df: pd.DataFrame) -> pd.DataFrame:
    if min_df.empty:
        return pd.DataFrame()
    out = min_df.copy()
    out["trade_date"] = out["bar_time"].astype(str)
    out["vol"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["amount"] = pd.to_numeric(out.get("amount"), errors="coerce")
    out["pre_close"] = pd.to_numeric(out.get("preClose"), errors="coerce")
    numeric_cols = ["open", "high", "low", "close", "vol", "amount", "pre_close"]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    return out[["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount"]]


def compute_daily_chain(ts_code: str) -> dict:
    df, source = load_daily_parquet(ts_code)
    if df.empty:
        return {"source": source, "raw": df, "calc": pd.DataFrame(), "last": pd.Series(dtype=object), "trend": "-", "summary": "-"}
    calc = compute_indicators(df)
    last = calc.iloc[-1]
    trend, summary = summarize_signal(last)
    return {"source": source, "raw": df, "calc": calc, "last": last, "trend": trend, "summary": summary}


def compute_minute_chain(ts_code: str, period: str = "1m", lookback_days: int = 10) -> dict:
    raw, source = load_minute_sequence(ts_code, period=period, lookback_days=lookback_days)
    if raw.empty:
        return {"source": source, "raw": raw, "calc": pd.DataFrame(), "last": pd.Series(dtype=object), "trend": "-", "summary": "-"}
    inp = minute_to_indicator_input(raw)
    calc = compute_indicators(inp)
    last = calc.iloc[-1]
    trend, summary = summarize_signal(last)
    return {"source": source, "raw": raw, "calc": calc, "last": last, "trend": trend, "summary": summary}

