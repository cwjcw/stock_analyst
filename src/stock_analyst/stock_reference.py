from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT / "data" / "reference_cache"
SPOT_CACHE = CACHE_DIR / "a_spot.parquet"
CONCEPT_CACHE = CACHE_DIR / "concept_index.json"


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def normalize_ts_code(raw_code: str) -> str:
    code = str(raw_code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        return code
    if len(code) != 6 or not code.isdigit():
        return code
    if code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def code_to_symbol(raw_code: str) -> str:
    ts_code = normalize_ts_code(raw_code)
    return ts_code.split(".")[0] if "." in ts_code else ts_code


def _load_akshare():
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]:
        os.environ[key] = ""
    os.environ["NO_PROXY"] = "*"
    import akshare as ak

    return ak


def _refresh_spot_cache() -> pd.DataFrame:
    _ensure_cache_dir()
    ak = _load_akshare()
    df = ak.stock_zh_a_spot_em()
    keep_cols = [col for col in df.columns if col in {"代码", "名称"}]
    df = df[keep_cols].copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df.to_parquet(SPOT_CACHE, index=False)
    return df


def get_spot_df(force_refresh: bool = False) -> pd.DataFrame:
    _ensure_cache_dir()
    if force_refresh:
        return _refresh_spot_cache()
    if not SPOT_CACHE.exists():
        return pd.DataFrame(columns=["代码", "名称"])
    try:
        return pd.read_parquet(SPOT_CACHE)
    except Exception:
        return pd.DataFrame(columns=["代码", "名称"])


def _refresh_concept_cache() -> dict[str, list[str]]:
    _ensure_cache_dir()
    ak = _load_akshare()
    concept_df = ak.stock_board_concept_name_em()
    concept_col = "板块名称" if "板块名称" in concept_df.columns else concept_df.columns[0]
    concept_map: dict[str, list[str]] = {}
    for board_name in concept_df[concept_col].dropna().astype(str).tolist():
        try:
            cons_df = ak.stock_board_concept_cons_em(symbol=board_name)
        except Exception:
            continue
        if "代码" not in cons_df.columns:
            continue
        for code in cons_df["代码"].dropna().astype(str).str.zfill(6):
            concept_map.setdefault(code, []).append(board_name)

    payload = {"concept_index": concept_map}
    CONCEPT_CACHE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return concept_map


def get_concept_index(force_refresh: bool = False) -> dict[str, list[str]]:
    _ensure_cache_dir()
    if force_refresh:
        return _refresh_concept_cache()
    if not CONCEPT_CACHE.exists():
        return {}
    try:
        payload = json.loads(CONCEPT_CACHE.read_text(encoding="utf-8"))
        concept_index = payload.get("concept_index") or {}
        if not isinstance(concept_index, dict):
            raise ValueError("invalid concept cache")
        return {str(k): [str(x) for x in v] for k, v in concept_index.items()}
    except Exception:
        return {}


@lru_cache(maxsize=2048)
def lookup_stock_reference(raw_code: str) -> dict:
    ts_code = normalize_ts_code(raw_code)
    symbol = code_to_symbol(ts_code)
    if len(symbol) != 6 or not symbol.isdigit():
        return {"input_code": raw_code, "ts_code": ts_code, "symbol": symbol, "stock_name": "", "concept_boards": []}

    try:
        spot_df = get_spot_df()
        row = spot_df.loc[spot_df["代码"].astype(str) == symbol] if not spot_df.empty else pd.DataFrame()
    except Exception:
        row = pd.DataFrame()

    stock_name = ""
    if not row.empty and "名称" in row.columns:
        stock_name = str(row.iloc[0]["名称"])

    concept_index = get_concept_index()
    concept_boards = concept_index.get(symbol, [])

    return {
        "input_code": raw_code,
        "ts_code": ts_code,
        "symbol": symbol,
        "stock_name": stock_name,
        "concept_boards": concept_boards,
    }
