from __future__ import annotations

import json

import numpy as np
import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values("trade_date").reset_index(drop=True)

    out["MA5"] = out["close"].rolling(5).mean()
    out["MA10"] = out["close"].rolling(10).mean()
    out["MA20"] = out["close"].rolling(20).mean()
    out["MA60"] = out["close"].rolling(60).mean()

    ema12 = ema(out["close"], 12)
    ema26 = ema(out["close"], 26)
    out["DIF"] = ema12 - ema26
    out["DEA"] = ema(out["DIF"], 9)
    out["MACD"] = 2 * (out["DIF"] - out["DEA"])

    high = out["high"]
    low = out["low"]
    close = out["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=out.index)
    minus_dm = pd.Series(minus_dm, index=out.index)
    tr14 = tr.rolling(14).sum()
    plus_dm14 = plus_dm.rolling(14).sum()
    minus_dm14 = minus_dm.rolling(14).sum()
    out["PDI"] = 100 * plus_dm14 / tr14.replace(0, np.nan)
    out["MDI"] = 100 * minus_dm14 / tr14.replace(0, np.nan)
    dx = 100 * (out["PDI"] - out["MDI"]).abs() / (out["PDI"] + out["MDI"]).replace(0, np.nan)
    out["ADX"] = dx.rolling(14).mean()
    out["ADXR"] = (out["ADX"] + out["ADX"].shift(6)) / 2

    low_n = out["low"].rolling(9).min()
    high_n = out["high"].rolling(9).max()
    rsv = 100 * (out["close"] - low_n) / (high_n - low_n).replace(0, np.nan)
    out["K"] = rsv.ewm(com=2, adjust=False).mean()
    out["D"] = out["K"].ewm(com=2, adjust=False).mean()
    out["J"] = 3 * out["K"] - 2 * out["D"]

    delta = out["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    for n in (6, 12, 24):
        avg_gain = gain.rolling(n).mean()
        avg_loss = loss.rolling(n).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        out[f"RSI{n}"] = 100 - 100 / (1 + rs)

    for n in (6, 10):
        hh = out["high"].rolling(n).max()
        ll = out["low"].rolling(n).min()
        out[f"WR{n}"] = 100 * (hh - out["close"]) / (hh - ll).replace(0, np.nan)

    for n in (6, 12, 24):
        ma = out["close"].rolling(n).mean()
        out[f"BIAS{n}"] = 100 * (out["close"] - ma) / ma.replace(0, np.nan)

    out["ROC"] = 100 * (out["close"] - out["close"].shift(12)) / out["close"].shift(12)
    out["MAROC"] = out["ROC"].rolling(6).mean()

    tp = (out["high"] + out["low"] + out["close"]) / 3
    sma_tp = tp.rolling(14).mean()
    md = tp.rolling(14).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    out["CCI"] = (tp - sma_tp) / (0.015 * md.replace(0, np.nan))

    close_diff = out["close"].diff().fillna(0)
    obv_step = np.where(close_diff > 0, out["vol"], np.where(close_diff < 0, -out["vol"], 0))
    out["OBV"] = pd.Series(obv_step, index=out.index).cumsum()
    out["MAOBV"] = out["OBV"].rolling(30).mean()
    out["QRR"] = out["vol"] / out["vol"].shift(1).rolling(5).mean()
    if "volume_ratio" in out.columns:
        out["QRR"] = out["volume_ratio"].fillna(out["QRR"])
    return out


def summarize_signal(last_row: pd.Series) -> tuple[str, str]:
    trend = "neutral"
    if (
        pd.notna(last_row.get("MA20"))
        and pd.notna(last_row.get("MA60"))
        and pd.notna(last_row.get("DIF"))
        and pd.notna(last_row.get("DEA"))
    ):
        if last_row["close"] > last_row["MA20"] > last_row["MA60"] and last_row["DIF"] > last_row["DEA"]:
            trend = "bullish"
        elif last_row["close"] < last_row["MA20"] < last_row["MA60"] and last_row["DIF"] < last_row["DEA"]:
            trend = "bearish"

    parts = [f"trend={trend}"]
    rsi6 = last_row.get("RSI6")
    if pd.notna(rsi6):
        if rsi6 >= 80:
            parts.append("RSI6=overbought")
        elif rsi6 <= 20:
            parts.append("RSI6=oversold")
    qrr = last_row.get("QRR")
    if pd.notna(qrr):
        if qrr >= 1.5:
            parts.append("volume=active")
        elif qrr <= 0.7:
            parts.append("volume=weak")

    return trend, "; ".join(parts)


def row_to_json(last_row: pd.Series) -> str:
    payload = {}
    for k, v in last_row.items():
        if pd.isna(v):
            payload[k] = None
        elif isinstance(v, (np.floating, float, np.integer, int)):
            payload[k] = float(v)
        else:
            payload[k] = str(v)
    return json.dumps(payload, ensure_ascii=False)

