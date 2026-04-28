from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.indicator_chain import compute_daily_chain, compute_minute_chain, load_moneyflow_parquet, report_path
from stock_analyst.stock_reference import lookup_stock_reference


def _fmt(value, digits: int = 4) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value) if value not in (None, "") else "-"


def _num(value) -> float | None:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return None if pd.isna(parsed) else float(parsed)
    except Exception:
        return None


def _first(row: pd.Series, names: list[str]):
    for name in names:
        if name in row.index and pd.notna(row.get(name)):
            return row.get(name)
    return None


def make_table(rows: list[dict], headers: list[str]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(header, "-")) for header in headers) + " |")
    return "\n".join(lines)


EXTENDED_DATASET_LABELS = {
    "daily_basic": "估值与交易活跃度 daily_basic",
    "bak_daily": "备用行情 bak_daily",
    "fina_indicator": "财务指标 fina_indicator",
    "income": "利润表 income",
    "balancesheet": "资产负债表 balancesheet",
    "cashflow": "现金流量表 cashflow",
    "hsgt_top10": "沪深港通活跃成交 hsgt_top10",
    "top10_holders": "前十大股东 top10_holders",
    "top10_floatholders": "前十大流通股东 top10_floatholders",
    "forecast": "业绩预告 forecast",
    "express": "业绩快报 express",
    "limit_list_d": "涨跌停/炸板记录 limit_list_d",
}


EXTENDED_DATASET_COLUMNS = {
    "daily_basic": ["trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps_ttm", "dv_ttm", "total_mv", "circ_mv"],
    "bak_daily": ["trade_date", "close", "pct_chg", "swing", "vol", "amount", "turnover_rate", "pe", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv"],
    "fina_indicator": ["end_date", "ann_date", "roe", "roe_dt", "grossprofit_margin", "netprofit_margin", "debt_to_assets", "current_ratio", "quick_ratio", "ocfps", "eps"],
    "income": ["end_date", "ann_date", "revenue", "total_revenue", "n_income_attr_p", "n_income", "operate_profit", "basic_eps"],
    "balancesheet": ["end_date", "ann_date", "total_assets", "total_liab", "total_hldr_eqy_exc_min_int", "money_cap", "inventories", "accounts_receiv"],
    "cashflow": ["end_date", "ann_date", "n_cashflow_act", "free_cashflow", "net_profit", "c_cash_equ_end", "c_fr_sale_sg"],
    "hsgt_top10": ["trade_date", "name", "close", "change", "rank", "amount", "net_amount", "buy", "sell"],
    "top10_holders": ["end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"],
    "top10_floatholders": ["end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"],
    "forecast": ["ann_date", "end_date", "type", "p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "summary"],
    "express": ["ann_date", "end_date", "revenue", "operate_profit", "n_income", "total_assets", "diluted_eps", "yoy_sales", "yoy_dedu_np"],
    "limit_list_d": ["trade_date", "name", "close", "pct_chg", "amp", "fc_ratio", "fl_ratio", "fd_amount", "first_time", "last_time", "open_times", "limit"],
}


def _md_cell(value) -> str:
    if pd.isna(value):
        return "-"
    text = str(value).replace("\r", " ").replace("\n", " ").replace("|", "/")
    return text.strip() or "-"


def dataframe_sample_table(df: pd.DataFrame, dataset: str, max_rows: int = 8) -> str:
    if df.empty:
        return "暂无数据。"
    preferred = [col for col in EXTENDED_DATASET_COLUMNS.get(dataset, []) if col in df.columns]
    columns = preferred or list(df.columns[:12])
    sample = df.tail(max_rows).copy()
    if columns:
        sample = sample[columns]
    rows = [{col: _md_cell(row.get(col)) for col in sample.columns} for _, row in sample.iterrows()]
    return make_table(rows, list(sample.columns))


def extended_data_sections(datasets: dict[str, tuple[pd.DataFrame, str]]) -> list[str]:
    sections: list[str] = []
    for dataset, (df, _source) in datasets.items():
        label = EXTENDED_DATASET_LABELS.get(dataset, dataset)
        sections.extend(
            [
                f"### {label}",
                "",
                f"- 记录数：{len(df)}",
                "",
                dataframe_sample_table(df, dataset),
                "",
            ]
        )
    return sections


def localize_trend_label(value: object) -> str:
    text = str(value or "-").strip()
    mapping = {
        "neutral": "中性",
        "bullish": "偏多",
        "bearish": "偏空",
        "up": "上升",
        "down": "下降",
        "sideways": "震荡",
    }
    return mapping.get(text.lower(), text)


def localize_summary(value: object) -> str:
    text = str(value or "-").strip()
    if not text:
        return "-"
    return (
        text.replace("trend=neutral", "趋势=中性")
        .replace("trend=bullish", "趋势=偏多")
        .replace("trend=bearish", "趋势=偏空")
        .replace("trend=up", "趋势=上升")
        .replace("trend=down", "趋势=下降")
        .replace("trend=sideways", "趋势=震荡")
        .replace("RSI6=overbought", "RSI6=超买")
        .replace("RSI6=oversold", "RSI6=超卖")
        .replace("volume=active", "量能=活跃")
        .replace("volume=weak", "量能=偏弱")
    )


def market_store_dir(ts_code: str) -> Path:
    return ROOT / "data" / "market_store" / ts_code


def parse_symbol_market(ts_code: str) -> tuple[str, str]:
    code = str(ts_code or "").strip().upper()
    if "." not in code:
        return code, "sz"
    symbol, market = code.split(".", 1)
    return symbol, ("sh" if market == "SH" else "sz")


def fetch_realtime_moneyflow_akshare(ts_code: str) -> tuple[dict, str]:
    symbol, _market = parse_symbol_market(ts_code)
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        return {}, f"akshare_unavailable: {exc}"

    results: dict[str, float | str] = {}
    try:
        # AkShare ranking API provides intraday/close snapshots by indicator.
        for indicator, key in (("今日", "today_main_net_inflow"), ("昨日", "yesterday_main_net_inflow")):
            df = ak.stock_individual_fund_flow_rank(indicator=indicator)
            if df is None or df.empty:
                continue
            df = df.copy()
            if "代码" not in df.columns:
                continue
            row_df = df[df["代码"].astype(str).str.zfill(6) == str(symbol).zfill(6)]
            if row_df.empty:
                continue
            row = row_df.iloc[0]
            # Typical column name: 主力净流入-净额
            candidate_cols = [col for col in df.columns if "主力净流入" in str(col) and "净额" in str(col)]
            if not candidate_cols:
                continue
            value = _num(row.get(candidate_cols[0]))
            if value is not None:
                results[key] = value
    except Exception as exc:
        return {}, f"akshare_error: {exc}"

    if results:
        results["fetch_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        store_path = market_store_dir(ts_code) / f"{ts_code}_akshare_realtime_moneyflow.parquet"
        store_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"ts_code": ts_code, **results}]).to_parquet(store_path, index=False, compression="zstd")
        return results, store_path.as_posix()
    return {}, "akshare_empty"


def load_factor(ts_code: str, dataset: str) -> tuple[pd.DataFrame, str]:
    path = market_store_dir(ts_code) / f"{ts_code}_tushare_{dataset}.parquet"
    if not path.exists():
        return pd.DataFrame(), path.as_posix()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame(), path.as_posix()
    for col in ("trade_date", "ann_date", "end_date"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    sort_cols = [col for col in ("trade_date", "ann_date", "end_date") if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df, path.as_posix()


def latest_row(df: pd.DataFrame, sort_cols: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=object)
    existing = [col for col in sort_cols if col in df.columns]
    if existing:
        return df.sort_values(existing).iloc[-1]
    return df.iloc[-1]


def recent_df(df: pd.DataFrame, years: int = 4) -> pd.DataFrame:
    if df.empty:
        return df
    cutoff = (dt.date.today() - dt.timedelta(days=365 * years)).strftime("%Y%m%d")
    masks = []
    for col in ("trade_date", "ann_date", "end_date"):
        if col in df.columns:
            masks.append(df[col].astype(str).str.slice(0, 8) >= cutoff)
    if not masks:
        return df
    mask = masks[0]
    for item in masks[1:]:
        mask = mask | item
    return df.loc[mask].copy()


def analyze_capital(moneyflow_df: pd.DataFrame, daily_last: pd.Series) -> dict:
    if moneyflow_df.empty:
        return {
            "status": "不好判断",
            "logic": "未找到资金流数据。",
            "suggestion": "先补齐资金流数据，再判断主力是否真正进场。",
        }

    recent = moneyflow_df.tail(3).copy()
    latest = recent.iloc[-1]
    net_amount = _num(latest.get("net_mf_amount")) or 0.0
    total_flow = 0.0
    for col in [
        "buy_sm_amount",
        "sell_sm_amount",
        "buy_md_amount",
        "sell_md_amount",
        "buy_lg_amount",
        "sell_lg_amount",
        "buy_elg_amount",
        "sell_elg_amount",
    ]:
        total_flow += _num(latest.get(col)) or 0.0
    net_pct = (net_amount / total_flow * 100.0) if total_flow else 0.0
    super_net = (_num(latest.get("buy_elg_amount")) or 0.0) - (_num(latest.get("sell_elg_amount")) or 0.0)
    large_net = (_num(latest.get("buy_lg_amount")) or 0.0) - (_num(latest.get("sell_lg_amount")) or 0.0)

    if "net_mf_amount" in recent.columns:
        net_series = pd.to_numeric(recent["net_mf_amount"], errors="coerce").fillna(0.0)
    else:
        net_series = pd.Series(dtype="float64")
    three_day_increase = len(net_series) == 3 and bool((net_series.diff().iloc[1:] > 0).all())
    pct_chg = _num(daily_last.get("pct_chg")) or 0.0

    if (net_amount > 0 and net_pct > 5) or (net_amount > 0 and three_day_increase):
        return {
            "status": "资金进入",
            "logic": (
                f"最新主力净流入 {net_amount:.2f}，净占比约 {net_pct:.2f}%，"
                f"超大单净额 {super_net:.2f}，大单净额 {large_net:.2f}。"
                + (" 最近 3 日净流入连续增加。" if three_day_increase else "")
            ),
            "suggestion": "主力开始控盘，可重点跟踪，优先观察是否继续放量走强。",
        }

    if (net_amount < 0 and net_pct < -5) or (pct_chg >= 0 and net_amount < 0):
        return {
            "status": "资金离开",
            "logic": (
                f"最新主力净流入 {net_amount:.2f}，净占比约 {net_pct:.2f}%，当日涨跌幅 {pct_chg:.2f}%。"
                + (" 股价上涨但主力净流出，存在拉高出货嫌疑。" if pct_chg >= 0 and net_amount < 0 else "")
            ),
            "suggestion": "不宜盲目追高，若已有仓位，优先考虑减仓和保护利润。",
        }

    return {
        "status": "不好判断",
        "logic": (
            f"最新主力净流入 {net_amount:.2f}，净占比约 {net_pct:.2f}%，"
            f"超大单净额 {super_net:.2f}，多空资金仍在对冲。"
        ),
        "suggestion": "资金面还未形成明显方向，先观察后续是否放量突破或继续回流。",
    }


def analyze_trend(daily_last: pd.Series, minute_last: pd.Series) -> dict:
    if daily_last.empty:
        return {"status": "不好判断", "logic": "缺少日线历史数据。", "suggestion": "先补齐日线数据。"}

    close_ref = _num(minute_last.get("close")) if not minute_last.empty else None
    if close_ref is None:
        close_ref = _num(daily_last.get("close")) or 0.0
    ma20 = _num(daily_last.get("MA20"))
    ma5 = _num(daily_last.get("MA5"))
    ma10 = _num(daily_last.get("MA10"))
    dif = _num(daily_last.get("DIF"))
    dea = _num(daily_last.get("DEA"))
    macd = _num(daily_last.get("MACD"))

    if (
        ma20 is not None
        and ma5 is not None
        and ma10 is not None
        and dif is not None
        and dea is not None
        and close_ref > ma20
        and ma5 > ma10
        and (dif > dea or (dif > 0 and dea > 0))
    ):
        return {
            "status": "向上趋势",
            "logic": (
                f"参考价格 {close_ref:.4f} 高于 MA20({ma20:.4f})，"
                f"MA5({ma5:.4f}) 高于 MA10({ma10:.4f})，"
                f"MACD 结构为 DIF({dif:.4f}) 与 DEA({dea:.4f})。"
            ),
            "suggestion": "趋势面配合良好，优先顺势看多，等待更好的盘中进场时机。",
        }

    if ma20 is not None and dif is not None and dea is not None and close_ref < ma20 and (
        dif < dea or (dif < 0 and (macd or 0.0) < 0)
    ):
        return {
            "status": "向下趋势",
            "logic": f"参考价格 {close_ref:.4f} 低于 MA20({ma20:.4f})，DIF({dif:.4f}) 低于 DEA({dea:.4f})。",
            "suggestion": "趋势仍在下行，除非出现明确止跌信号，否则先回避。",
        }

    return {
        "status": "不好判断",
        "logic": f"MA5={_fmt(ma5)}，MA10={_fmt(ma10)}，MA20={_fmt(ma20)}，趋势仍偏震荡。",
        "suggestion": "耐心等待放量突破或有效转弱。",
    }


def analyze_strength(daily_last: pd.Series, minute_last: pd.Series) -> dict:
    source = minute_last if not minute_last.empty else daily_last
    if source.empty:
        return {"status": "不好判断", "logic": "缺少 RSI 和 KDJ 数据。", "suggestion": "先补齐指标数据。"}

    rsi = _num(source.get("RSI6")) or _num(source.get("RSI12"))
    k = _num(source.get("K"))
    d_value = _num(source.get("D"))
    j = _num(source.get("J"))

    if rsi is not None and 50 <= rsi <= 70 and k is not None and d_value is not None and j is not None and k >= d_value and j < 80:
        return {
            "status": "强度健康",
            "logic": f"RSI 约为 {rsi:.2f}，K={k:.2f}，D={d_value:.2f}，J={j:.2f}，动能仍处于健康区间。",
            "suggestion": "强弱面健康，可继续跟踪上涨延续性。",
        }

    if rsi is not None and rsi > 80 and j is not None and j > 100:
        return {
            "status": "强度过热",
            "logic": f"RSI 约为 {rsi:.2f}，J 值约为 {j:.2f}，已经进入明显超买区。",
            "suggestion": "短线不宜追高，若已有浮盈，可优先考虑分批止盈。",
        }

    if rsi is not None and rsi < 20:
        return {
            "status": "强度极弱",
            "logic": f"RSI 约为 {rsi:.2f}，已接近极度超卖区。",
            "suggestion": "如果资金开始回流，可留意是否形成反弹；否则继续等待。",
        }

    return {
        "status": "不好判断",
        "logic": f"RSI={_fmt(rsi)}，K={_fmt(k)}，D={_fmt(d_value)}，J={_fmt(j)}，暂未进入极端强弱区。",
        "suggestion": "强弱面暂时中性，等待更明确的超买或超卖信号。",
    }


def synthesize_signal(capital: dict, trend: dict, strength: dict) -> dict:
    capital_status = capital["status"]
    trend_status = trend["status"]
    strength_status = strength["status"]

    if capital_status == "资金进入" and trend_status == "向上趋势" and strength_status == "强度健康":
        return {
            "signal": "强烈推荐",
            "logic": "资金进入 + 趋势向上 + 强度健康，三维共振最完整。",
            "action": "可重仓持有或顺势加仓，优先沿趋势交易。",
        }
    if (capital_status == "资金离开" and trend_status == "向上趋势") or (
        capital_status == "资金进入" and strength_status == "强度过热"
    ):
        return {
            "signal": "警惕背离",
            "logic": "趋势仍在，但资金或强弱面已经出现背离，短线容易冲高回落。",
            "action": "建议逢高减仓，不追涨，等待新的确认信号。",
        }
    if capital_status == "资金进入" and trend_status == "不好判断" and strength_status == "强度极弱":
        return {
            "signal": "底部埋伏",
            "logic": "资金开始尝试进入，但趋势尚未完全转强，强弱面处于极弱区。",
            "action": "可以小仓位试错，严格控制仓位，等待趋势确认后再加码。",
        }
    if capital_status == "资金离开" and trend_status == "向下趋势":
        return {
            "signal": "离场观望",
            "logic": "资金与趋势同时转弱，风险明显大于机会。",
            "action": "以观望为主，不因价格便宜而贸然抄底。",
        }
    return {
        "signal": "不明朗",
        "logic": "三维结果互相矛盾，当前还没有形成清晰共振。",
        "action": "优先等待市场给出方向，或切换到更强的标的。",
    }


def assess_trend(daily_last: pd.Series, minute_last: pd.Series) -> tuple[str, list[str]]:
    notes: list[str] = []
    if daily_last.empty:
        return "缺少日线趋势数据", ["未找到可用的历史日线数据。"]

    close = _num(daily_last.get("close"))
    ma20 = _num(daily_last.get("MA20"))
    ma60 = _num(daily_last.get("MA60"))
    dif = _num(daily_last.get("DIF"))
    dea = _num(daily_last.get("DEA"))

    label = "中性"
    if close is not None and ma20 is not None and ma60 is not None:
        if close > ma20 > ma60:
            label = "偏多趋势"
            notes.append("日线收盘价位于 MA20 和 MA60 上方。")
        elif close < ma20 < ma60:
            label = "偏空趋势"
            notes.append("日线收盘价位于 MA20 和 MA60 下方。")
        else:
            notes.append("日线均线关系仍然偏混合。")
    if dif is not None and dea is not None:
        notes.append("日线 MACD 结构偏强。" if dif > dea else "日线 MACD 结构偏弱。")

    if not minute_last.empty:
        minute_close = _num(minute_last.get("close"))
        if minute_close is not None and close is not None:
            notes.append("最新分钟收盘价高于最新日线收盘价，盘中表现偏强。" if minute_close > close else "最新分钟收盘价低于最新日线收盘价，盘中承压。")

    return label, notes


def assess_timing(minute_last: pd.Series) -> tuple[str, list[str]]:
    if minute_last.empty:
        return "缺少盘中时机数据", ["未找到可用的分钟线序列。"]

    notes: list[str] = []
    close = _num(minute_last.get("close"))
    ma5 = _num(minute_last.get("MA5"))
    ma10 = _num(minute_last.get("MA10"))
    dif = _num(minute_last.get("DIF"))
    dea = _num(minute_last.get("DEA"))
    rsi6 = _num(minute_last.get("RSI6"))

    label = "观望"
    if close is not None and ma5 is not None and close > ma5:
        label = "可关注买点"
        notes.append("最新分钟收盘价站上分钟 MA5。")
    if close is not None and ma10 is not None and close < ma10:
        notes.append("价格仍在分钟 MA10 下方，追高并不理想。")
    if dif is not None and dea is not None:
        notes.append("分钟 MACD 对短线时机有支撑。" if dif > dea else "分钟 MACD 仍然偏弱。")
    if rsi6 is not None:
        if rsi6 <= 20:
            notes.append("分钟 RSI6 已接近超卖，可能出现反弹时机。")
        elif rsi6 >= 80:
            notes.append("分钟 RSI6 已接近超买，短线风险较高。")

    if not notes:
        notes.append("当前分钟样本不强，时机判断仅供参考。")
    return label, notes


def daily_rows(last: pd.Series) -> list[dict]:
    if last.empty:
        return []
    return [
        {"指标": "收盘价", "数值": _fmt(last.get("close"))},
        {"指标": "MA5", "数值": _fmt(last.get("MA5"))},
        {"指标": "MA10", "数值": _fmt(last.get("MA10"))},
        {"指标": "MA20", "数值": _fmt(last.get("MA20"))},
        {"指标": "MA60", "数值": _fmt(last.get("MA60"))},
        {"指标": "DIF", "数值": _fmt(last.get("DIF"))},
        {"指标": "DEA", "数值": _fmt(last.get("DEA"))},
        {"指标": "MACD", "数值": _fmt(last.get("MACD"))},
        {"指标": "RSI6", "数值": _fmt(last.get("RSI6"))},
        {"指标": "RSI12", "数值": _fmt(last.get("RSI12"))},
        {"指标": "K", "数值": _fmt(last.get("K"))},
        {"指标": "D", "数值": _fmt(last.get("D"))},
        {"指标": "J", "数值": _fmt(last.get("J"))},
    ]


def minute_rows(last: pd.Series, raw_last: pd.Series) -> list[dict]:
    if last.empty:
        return []
    return [
        {"指标": "K线时间", "数值": raw_last.get("bar_time", "-")},
        {"指标": "抓取时间", "数值": raw_last.get("fetch_time", "-")},
        {"指标": "收盘价", "数值": _fmt(last.get("close"))},
        {"指标": "MA5", "数值": _fmt(last.get("MA5"))},
        {"指标": "MA10", "数值": _fmt(last.get("MA10"))},
        {"指标": "DIF", "数值": _fmt(last.get("DIF"))},
        {"指标": "DEA", "数值": _fmt(last.get("DEA"))},
        {"指标": "MACD", "数值": _fmt(last.get("MACD"))},
        {"指标": "RSI6", "数值": _fmt(last.get("RSI6"))},
        {"指标": "K", "数值": _fmt(last.get("K"))},
        {"指标": "D", "数值": _fmt(last.get("D"))},
        {"指标": "J", "数值": _fmt(last.get("J"))},
        {"指标": "成交量(手)", "数值": _fmt(raw_last.get("volume"), 0)},
        {"指标": "成交额(元)", "数值": _fmt(raw_last.get("amount"), 0)},
    ]


def analyze_extended_factors(ts_code: str) -> dict:
    datasets = {
        name: load_factor(ts_code, name)
        for name in [
            "daily_basic",
            "bak_daily",
            "fina_indicator",
            "income",
            "balancesheet",
            "cashflow",
            "hsgt_top10",
            "top10_holders",
            "top10_floatholders",
            "forecast",
            "express",
            "limit_list_d",
        ]
    }

    rows: list[dict] = []
    notes: list[str] = []
    coverage: list[dict] = []
    score = 0

    for name, (df, source) in datasets.items():
        coverage.append({"数据项": name, "记录数": len(df), "来源": source})

    daily_basic = latest_row(datasets["daily_basic"][0], ["trade_date"])
    if not daily_basic.empty:
        turnover = _num(_first(daily_basic, ["turnover_rate_f", "turnover_rate"]))
        volume_ratio = _num(daily_basic.get("volume_ratio"))
        pe = _num(_first(daily_basic, ["pe_ttm", "pe"]))
        pb = _num(daily_basic.get("pb"))
        dv = _num(_first(daily_basic, ["dv_ttm", "dv_ratio"]))
        mv = _num(daily_basic.get("circ_mv"))
        logic = f"换手率 {_fmt(turnover, 2)}%，量比 {_fmt(volume_ratio, 2)}，PE {_fmt(pe, 2)}，PB {_fmt(pb, 2)}，股息率 {_fmt(dv, 2)}%，流通市值 {_fmt(mv, 2)}。"
        if turnover is not None and turnover >= 3:
            score += 1
            logic += " 交易活跃度较高。"
        if pe is not None and pe > 60:
            score -= 1
            logic += " 估值偏高，需要更强业绩支撑。"
        if dv is not None and dv >= 3:
            score += 1
            logic += " 股息率具备一定防守属性。"
        rows.append({"维度": "估值与活跃度", "结论": logic})
    else:
        rows.append({"维度": "估值与活跃度", "结论": "未获取到 daily_basic 数据。"})

    bak_daily = latest_row(datasets["bak_daily"][0], ["trade_date"])
    if not bak_daily.empty:
        pct = _num(_first(bak_daily, ["pct_chg", "change_ratio"]))
        swing = _num(_first(bak_daily, ["swing", "amplitude"]))
        up_limit = _num(_first(bak_daily, ["up_limit", "limit_up"]))
        down_limit = _num(_first(bak_daily, ["down_limit", "limit_down"]))
        logic = f"最新涨跌幅 {_fmt(pct, 2)}%，振幅 {_fmt(swing, 2)}%，涨停价 {_fmt(up_limit)}，跌停价 {_fmt(down_limit)}。"
        if swing is not None and swing >= 8:
            score -= 1
            logic += " 日内振幅较大，追涨风险抬升。"
        rows.append({"维度": "备用行情风险", "结论": logic})
    else:
        rows.append({"维度": "备用行情风险", "结论": "未获取到 bak_daily 数据。"})

    fina = latest_row(datasets["fina_indicator"][0], ["end_date", "ann_date"])
    if not fina.empty:
        roe = _num(_first(fina, ["roe", "roe_dt"]))
        gross = _num(fina.get("grossprofit_margin"))
        net_margin = _num(fina.get("netprofit_margin"))
        debt = _num(fina.get("debt_to_assets"))
        ocfps = _num(fina.get("ocfps"))
        logic = f"ROE {_fmt(roe, 2)}%，毛利率 {_fmt(gross, 2)}%，净利率 {_fmt(net_margin, 2)}%，资产负债率 {_fmt(debt, 2)}%，每股经营现金流 {_fmt(ocfps)}。"
        if roe is not None and roe >= 10:
            score += 1
            logic += " 盈利质量有支撑。"
        if debt is not None and debt >= 75:
            score -= 1
            logic += " 资产负债率较高，需注意杠杆风险。"
        rows.append({"维度": "财务质量", "结论": logic})
    else:
        rows.append({"维度": "财务质量", "结论": "未获取到 fina_indicator 数据。"})

    income = latest_row(datasets["income"][0], ["end_date", "ann_date"])
    cashflow = latest_row(datasets["cashflow"][0], ["end_date", "ann_date"])
    balancesheet = latest_row(datasets["balancesheet"][0], ["end_date", "ann_date"])
    if not income.empty or not cashflow.empty or not balancesheet.empty:
        revenue = _num(_first(income, ["revenue", "total_revenue"]))
        profit = _num(_first(income, ["n_income_attr_p", "n_income", "net_profit"]))
        op_cash = _num(_first(cashflow, ["n_cashflow_act", "net_cash_flows_oper_act"]))
        assets = _num(balancesheet.get("total_assets"))
        liab = _num(balancesheet.get("total_liab"))
        cash_ratio = (op_cash / profit) if op_cash is not None and profit not in (None, 0) else None
        logic = f"营收 {_fmt(revenue, 2)}，归母/净利润 {_fmt(profit, 2)}，经营现金流 {_fmt(op_cash, 2)}，总资产 {_fmt(assets, 2)}，总负债 {_fmt(liab, 2)}。"
        if cash_ratio is not None:
            logic += f" 经营现金流/利润约 {_fmt(cash_ratio, 2)}。"
            if cash_ratio >= 1:
                score += 1
                logic += " 利润现金含量较好。"
            elif cash_ratio < 0:
                score -= 1
                logic += " 现金流与利润背离。"
        rows.append({"维度": "业绩与现金流", "结论": logic})
    else:
        rows.append({"维度": "业绩与现金流", "结论": "未获取到三张表数据。"})

    hsgt = datasets["hsgt_top10"][0]
    if not hsgt.empty:
        net_col = "net_amount" if "net_amount" in hsgt.columns else None
        recent = hsgt.tail(20)
        net_sum = pd.to_numeric(recent[net_col], errors="coerce").sum() if net_col else None
        logic = f"近 20 条沪深股通活跃记录数 {len(recent)}"
        if net_sum is not None:
            logic += f"，净买入合计 {_fmt(net_sum, 2)}。"
            score += 1 if net_sum > 0 else -1
        else:
            logic += "。"
        rows.append({"维度": "北向活跃股", "结论": logic})
    else:
        rows.append({"维度": "北向活跃股", "结论": "未进入或未获取 hsgt_top10 活跃成交数据。"})

    holder = latest_row(datasets["top10_holders"][0], ["end_date"])
    floatholder = latest_row(datasets["top10_floatholders"][0], ["end_date"])
    holder_logic = []
    if not holder.empty:
        holder_logic.append(f"前十大股东最新期末 {holder.get('end_date', '-')}，单项持股比例 {_fmt(_first(holder, ['hold_ratio', 'hold_ratio_pct']), 2)}%。")
    if not floatholder.empty:
        holder_logic.append(f"前十大流通股东最新期末 {floatholder.get('end_date', '-')}，单项持股比例 {_fmt(_first(floatholder, ['hold_ratio', 'hold_ratio_pct']), 2)}%。")
    rows.append({"维度": "股东集中度", "结论": " ".join(holder_logic) if holder_logic else "未获取到前十大股东/流通股东数据。"})

    forecast = latest_row(recent_df(datasets["forecast"][0], years=4), ["ann_date", "end_date"])
    express = latest_row(recent_df(datasets["express"][0], years=4), ["ann_date", "end_date"])
    event_logic = []
    if not forecast.empty:
        event_logic.append(
            f"最新业绩预告 {forecast.get('ann_date', '-')}，类型 {forecast.get('type', '-')}，变动幅度 {_fmt(_first(forecast, ['p_change_min', 'p_change']), 2)}% 至 {_fmt(forecast.get('p_change_max'), 2)}%。"
        )
    if not express.empty:
        event_logic.append(
            f"最新业绩快报 {express.get('ann_date', '-')}，营收 {_fmt(express.get('revenue'), 2)}，净利润 {_fmt(_first(express, ['n_income', 'operate_profit']), 2)}。"
        )
    rows.append({"维度": "业绩预期", "结论": " ".join(event_logic) if event_logic else "未获取到业绩预告/快报数据。"})

    limit_df = datasets["limit_list_d"][0]
    if not limit_df.empty:
        up_count = 0
        down_count = 0
        if "limit" in limit_df.columns:
            up_count = int(limit_df["limit"].astype(str).str.contains("U|涨", case=False, regex=True).sum())
            down_count = int(limit_df["limit"].astype(str).str.contains("D|跌", case=False, regex=True).sum())
        logic = f"区间内涨跌停/炸板记录 {len(limit_df)} 条，涨停相关 {up_count} 条，跌停相关 {down_count} 条。"
        if up_count > down_count and up_count > 0:
            score += 1
            logic += " 情绪面偏活跃。"
        if down_count > up_count:
            score -= 1
            logic += " 情绪面偏弱。"
        rows.append({"维度": "涨跌停情绪", "结论": logic})
    else:
        rows.append({"维度": "涨跌停情绪", "结论": "区间内未获取到 limit_list_d 记录。"})

    if score >= 3:
        summary = "扩展维度整体偏正面，可增强主信号可信度。"
    elif score <= -2:
        summary = "扩展维度整体偏谨慎，需降低主信号权重。"
    else:
        summary = "扩展维度整体中性，暂未显著强化或削弱主信号。"
    notes.append(f"扩展维度分数：{score}。{summary}")

    return {
        "rows": rows,
        "coverage": coverage,
        "summary": summary,
        "score": score,
        "notes": notes,
        "data_sections": extended_data_sections(datasets),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="基于历史日线、资金流、分钟线和扩展 Tushare 数据生成中文分析报告")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--stock-name", default="")
    parser.add_argument("--minute-period", default="1m", choices=["1m", "5m", "10m"])
    parser.add_argument("--minute-lookback-days", type=int, default=10)
    args = parser.parse_args()

    ts_code = args.ts_code.upper()
    stock_name = (args.stock_name or "").strip()
    if not stock_name:
        try:
            stock_name = str(lookup_stock_reference(ts_code).get("stock_name") or "").strip()
        except Exception:
            stock_name = ""
    daily = compute_daily_chain(ts_code)
    minute = compute_minute_chain(ts_code, period=args.minute_period, lookback_days=args.minute_lookback_days)
    moneyflow_df, moneyflow_source = load_moneyflow_parquet(ts_code)
    realtime_flow, realtime_flow_source = fetch_realtime_moneyflow_akshare(ts_code)
    extended = analyze_extended_factors(ts_code)

    daily_last = daily["last"]
    minute_last = minute["last"]
    minute_raw_last = minute["raw"].iloc[-1] if not minute["raw"].empty else pd.Series(dtype=object)

    moneyflow_sum = "-"
    if not moneyflow_df.empty and "net_mf_amount" in moneyflow_df.columns:
        moneyflow_sum = _fmt(pd.to_numeric(moneyflow_df["net_mf_amount"], errors="coerce").sum())
    realtime_today = _fmt(realtime_flow.get("today_main_net_inflow"), 2) if realtime_flow else "-"
    realtime_yesterday = _fmt(realtime_flow.get("yesterday_main_net_inflow"), 2) if realtime_flow else "-"

    trend_label, trend_notes = assess_trend(daily_last, minute_last)
    timing_label, timing_notes = assess_timing(minute_last)
    capital_dimension = analyze_capital(moneyflow_df, daily_last)
    trend_dimension = analyze_trend(daily_last, minute_last)
    strength_dimension = analyze_strength(daily_last, minute_last)
    final_signal = synthesize_signal(capital_dimension, trend_dimension, strength_dimension)

    lines = [
        f"# {ts_code} 技术指标报告",
        "",
        f"- 生成时间：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 日线数据来源：{daily['source']}",
        f"- 分钟数据来源：{minute['source']}",
        f"- 资金流来源：{moneyflow_source}",
        f"- 实时资金流来源：{realtime_flow_source}",
        f"- 分钟周期：{args.minute_period}",
        "",
        "## 概览",
        "",
        f"- 日线趋势：{localize_trend_label(daily['trend'])}",
        f"- 日线摘要：{localize_summary(daily['summary'])}",
        f"- 分钟趋势：{localize_trend_label(minute['trend'])}",
        f"- 分钟摘要：{localize_summary(minute['summary'])}",
        f"- 已加载分钟条数：{len(minute['raw'])}",
        f"- 资金流记录数：{len(moneyflow_df)}",
        f"- 资金流净额合计：{moneyflow_sum}",
        f"- AkShare 实时主力净流入(今日)：{realtime_today}",
        f"- AkShare 主力净流入(昨日)：{realtime_yesterday}",
        f"- 扩展数据结论：{extended['summary']}",
        "",
        "## 两层判断",
        "",
        f"- 趋势判断：{trend_label}",
        f"- 时机判断：{timing_label}",
        f"- 综合信号：{final_signal['signal']}",
        f"- 操作建议：{final_signal['action']}",
        "",
        "### 趋势说明",
        "",
    ]

    lines.extend([f"- {note}" for note in trend_notes])
    lines.extend(["", "### 时机说明", ""])
    lines.extend([f"- {note}" for note in timing_notes])
    lines.extend(["", "## 日线指标", ""])

    if daily_last.empty:
        lines.extend(["未找到日线历史数据。", ""])
    else:
        lines.extend([make_table(daily_rows(daily_last), ["指标", "数值"]), ""])

    lines.extend(["## 分钟指标", ""])
    if minute_last.empty:
        lines.extend(["未找到分钟线序列，请先执行最新分钟线抓取脚本。", ""])
    else:
        lines.extend([make_table(minute_rows(minute_last, minute_raw_last), ["指标", "数值"]), ""])

    lines.extend(
        [
            "## 三维判研",
            "",
            "### 1. 资金面",
            "",
            f"- 状态：{capital_dimension['status']}",
            f"- 判定逻辑：{capital_dimension['logic']}",
            f"- 建议操作：{capital_dimension['suggestion']}",
            "",
            "### 2. 趋势面",
            "",
            f"- 状态：{trend_dimension['status']}",
            f"- 判定逻辑：{trend_dimension['logic']}",
            f"- 建议操作：{trend_dimension['suggestion']}",
            "",
            "### 3. 强弱面",
            "",
            f"- 状态：{strength_dimension['status']}",
            f"- 判定逻辑：{strength_dimension['logic']}",
            f"- 建议操作：{strength_dimension['suggestion']}",
            "",
            "## 扩展分析汇总",
            "",
            f"- 扩展维度分数：{extended['score']}",
            f"- 汇总结论：{extended['summary']}",
            "",
            make_table(extended["rows"], ["维度", "结论"]),
            "",
            "## 扩展数据覆盖",
            "",
            make_table(extended["coverage"], ["数据项", "记录数"]),
            "",
            "## 综合信号",
            "",
            f"- 最终判定：{final_signal['signal']}",
            f"- 判定逻辑：{final_signal['logic']}",
            f"- 建议操作：{final_signal['action']}",
            "",
            "## 使用规则",
            "",
            "- 趋势判断基于完整历史日线，再结合最新分钟线做盘中修正。",
            "- 扩展分析汇总用于给主信号增加基本面、估值、情绪、北向和股东结构约束。",
            "- 若扩展维度偏谨慎，即使技术信号较强，也应降低仓位或等待更多确认。",
            "- 若扩展维度偏正面且技术/资金共振，信号可信度更高。",
            "",
        ]
    )

    lines.extend(["", "## 扩展原始数据明细", ""])
    lines.extend(extended["data_sections"])

    out_path = report_path(ts_code, stock_name=stock_name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8-sig")
    print(out_path.as_posix())


if __name__ == "__main__":
    main()
