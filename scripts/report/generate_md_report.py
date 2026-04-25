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


def _fmt(v, digits: int = 4) -> str:
    try:
        if pd.isna(v):
            return "-"
        return f"{float(v):.{digits}f}"
    except Exception:
        return str(v)


def make_table(rows: list[dict], headers: list[str]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(h, "-")) for h in headers) + " |")
    return "\n".join(lines)


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
    )


def _num(series_or_value) -> float | None:
    try:
        series = pd.Series([series_or_value])
        value = pd.to_numeric(series, errors="coerce").iloc[0]
        return None if pd.isna(value) else float(value)
    except Exception:
        return None


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

    net_series = pd.to_numeric(recent["net_mf_amount"], errors="coerce").fillna(0.0)
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
                f"最新主力净流入 {net_amount:.2f}，净占比约 {net_pct:.2f}%，"
                f"当日涨跌幅 {pct_chg:.2f}%。"
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
        return {
            "status": "不好判断",
            "logic": "缺少日线历史数据，无法判断趋势面。",
            "suggestion": "先补齐日线数据后再分析趋势。",
        }

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

    if (
        ma20 is not None
        and dif is not None
        and dea is not None
        and close_ref < ma20
        and (dif < dea or (dif < 0 and (macd or 0.0) < 0))
    ):
        return {
            "status": "向下趋势",
            "logic": (
                f"参考价格 {close_ref:.4f} 低于 MA20({ma20:.4f})，"
                f"DIF({dif:.4f}) 低于 DEA({dea:.4f})，"
                f"MACD {0.0 if macd is None else macd:.4f}。"
            ),
            "suggestion": "趋势仍在下行，除非出现明确止跌信号，否则先回避。",
        }

    return {
        "status": "不好判断",
        "logic": (
            f"参考价格 {close_ref:.4f} 与均线距离有限，"
            f"MA5={_fmt(ma5)}、MA10={_fmt(ma10)}、MA20={_fmt(ma20)}，趋势仍偏震荡。"
        ),
        "suggestion": "趋势面尚未给出清晰方向，耐心等待放量突破或有效转弱。",
    }


def analyze_strength(daily_last: pd.Series, minute_last: pd.Series) -> dict:
    source = minute_last if not minute_last.empty else daily_last
    if source.empty:
        return {
            "status": "不好判断",
            "logic": "缺少 RSI 与 KDJ 数据。",
            "suggestion": "先补齐分钟线或日线指标数据。",
        }

    rsi = _num(source.get("RSI6")) or _num(source.get("RSI12"))
    k = _num(source.get("K"))
    d = _num(source.get("D"))
    j = _num(source.get("J"))
    prev_j = _num(source.get("J"))

    if rsi is not None and 50 <= rsi <= 70 and k is not None and d is not None and j is not None and k >= d and j < 80:
        return {
            "status": "强度健康",
            "logic": f"RSI 约为 {rsi:.2f}，K={k:.2f}，D={d:.2f}，J={j:.2f}，动能仍处于健康区间。",
            "suggestion": "强弱面健康，可继续跟踪上涨延续性。",
        }

    if rsi is not None and rsi > 80 and j is not None and j > 100:
        return {
            "status": "强度过热",
            "logic": f"RSI 约为 {rsi:.2f}，J 值约为 {j:.2f}，已经进入明显超买区。",
            "suggestion": "短线不宜追高，若已有浮盈，可优先考虑分批止盈。",
        }

    if rsi is not None and rsi < 20:
        extra = ""
        if k is not None and d is not None and k < d:
            extra = " 当前 KDJ 仍在低位死叉。"
        return {
            "status": "强度极弱",
            "logic": f"RSI 约为 {rsi:.2f}，已接近极度超卖区。{extra}".strip(),
            "suggestion": "如果资金开始回流，可留意是否形成“黄金坑”；否则继续耐心等待。",
        }

    return {
        "status": "不好判断",
        "logic": f"RSI={_fmt(rsi)}，K={_fmt(k)}，D={_fmt(d)}，J={_fmt(j)}，暂未进入极端强弱区。",
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
            "action": "坚决以观望为主，不因“便宜”而贸然抄底。",
        }
    return {
        "signal": "不明朗",
        "logic": "三维结果互相矛盾，当前还没有形成清晰共振。",
        "action": "优先等待市场给出方向，或切换到更强的标的。",
    }


def recommended_tushare_datasets() -> list[dict]:
    return [
        {
            "数据项": "daily_basic",
            "用途": "补充换手率、量比、市盈率、市净率、总市值、流通市值、股息率等。",
            "怎么用": "适合放到趋势判断前的基础画像里，用于判断估值高低、筹码活跃度和资金承接能力。",
        },
        {
            "数据项": "bak_daily",
            "用途": "补充涨停价、跌停价、振幅、委比、委差等更细的日度盘面特征。",
            "怎么用": "适合识别强势股是否有封板特征、振幅是否异常，以及是否存在追涨风险。",
        },
        {
            "数据项": "fina_indicator",
            "用途": "补充 ROE、毛利率、净利率、资产负债率、经营现金流质量等财务质量指标。",
            "怎么用": "适合加入基本面过滤层，避免只看技术面而忽略公司质量。",
        },
        {
            "数据项": "income / balancesheet / cashflow",
            "用途": "补充营业收入、净利润、负债结构、现金流等三张表核心字段。",
            "怎么用": "适合做季度趋势跟踪，判断业绩是否和技术面同步改善。",
        },
        {
            "数据项": "moneyflow_hsgt / hsgt_top10",
            "用途": "补充北向资金总量流向和重点持仓变化。",
            "怎么用": "适合验证外资是否持续加仓，增强资金面判断的可靠性。",
        },
        {
            "数据项": "top10_holders / top10_floatholders",
            "用途": "补充前十大股东和流通股东变化。",
            "怎么用": "适合观察机构持仓集中度是否提升，辅助判断筹码是否趋于稳定。",
        },
        {
            "数据项": "forecast / express",
            "用途": "补充业绩预告和业绩快报。",
            "怎么用": "适合在财报正式披露前做预期管理，避免踩到业绩雷。",
        },
        {
            "数据项": "limit_list_d",
            "用途": "补充涨停、跌停、连板和炸板信息。",
            "怎么用": "适合做情绪面观察，判断个股是否属于当前市场主线或短线热点。",
        },
    ]


def assess_trend(daily_last: pd.Series, minute_last: pd.Series) -> tuple[str, list[str]]:
    notes: list[str] = []
    if daily_last.empty:
        return "缺少日线趋势数据", ["未找到可用的历史日线数据。"]

    close = pd.to_numeric(pd.Series([daily_last.get("close")]), errors="coerce").iloc[0]
    ma20 = pd.to_numeric(pd.Series([daily_last.get("MA20")]), errors="coerce").iloc[0]
    ma60 = pd.to_numeric(pd.Series([daily_last.get("MA60")]), errors="coerce").iloc[0]
    dif = pd.to_numeric(pd.Series([daily_last.get("DIF")]), errors="coerce").iloc[0]
    dea = pd.to_numeric(pd.Series([daily_last.get("DEA")]), errors="coerce").iloc[0]

    label = "中性"
    if pd.notna(close) and pd.notna(ma20) and pd.notna(ma60):
        if close > ma20 > ma60:
            label = "偏多趋势"
            notes.append("日线收盘价位于 MA20 和 MA60 上方。")
        elif close < ma20 < ma60:
            label = "偏空趋势"
            notes.append("日线收盘价位于 MA20 和 MA60 下方。")
        else:
            notes.append("日线均线关系仍然偏混合。")
    if pd.notna(dif) and pd.notna(dea):
        if dif > dea:
            notes.append("日线 MACD 结构偏强。")
        elif dif < dea:
            notes.append("日线 MACD 结构偏弱。")

    if not minute_last.empty:
        minute_close = pd.to_numeric(pd.Series([minute_last.get("close")]), errors="coerce").iloc[0]
        if pd.notna(minute_close) and pd.notna(close):
            if minute_close > close:
                notes.append("最新分钟收盘价高于最新日线收盘价，盘中表现偏强。")
            elif minute_close < close:
                notes.append("最新分钟收盘价低于最新日线收盘价，盘中承压。")

    return label, notes


def assess_timing(minute_last: pd.Series) -> tuple[str, list[str]]:
    notes: list[str] = []
    if minute_last.empty:
        return "缺少盘中时机数据", ["未找到可用的分钟线序列。"]

    close = pd.to_numeric(pd.Series([minute_last.get("close")]), errors="coerce").iloc[0]
    ma5 = pd.to_numeric(pd.Series([minute_last.get("MA5")]), errors="coerce").iloc[0]
    ma10 = pd.to_numeric(pd.Series([minute_last.get("MA10")]), errors="coerce").iloc[0]
    dif = pd.to_numeric(pd.Series([minute_last.get("DIF")]), errors="coerce").iloc[0]
    dea = pd.to_numeric(pd.Series([minute_last.get("DEA")]), errors="coerce").iloc[0]
    rsi6 = pd.to_numeric(pd.Series([minute_last.get("RSI6")]), errors="coerce").iloc[0]

    label = "观望"
    if pd.notna(close) and pd.notna(ma5) and close > ma5:
        label = "可关注买点"
        notes.append("最新分钟收盘价站上分钟 MA5。")
    if pd.notna(close) and pd.notna(ma10) and close < ma10:
        notes.append("价格仍在分钟 MA10 下方，追高并不理想。")
    if pd.notna(dif) and pd.notna(dea):
        if dif > dea:
            notes.append("分钟 MACD 对短线时机有支撑。")
        elif dif < dea:
            notes.append("分钟 MACD 仍然偏弱。")
    if pd.notna(rsi6):
        if rsi6 <= 20:
            notes.append("分钟 RSI6 已接近超卖，可能出现反弹时机。")
        elif rsi6 >= 80:
            notes.append("分钟 RSI6 已接近超买，短线风险较高。")

    if not notes:
        notes.append("当前分钟样本还不够，时机判断仅供参考。")
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
        {"指标": "成交量", "数值": _fmt(raw_last.get("volume"), 0)},
        {"指标": "成交额", "数值": _fmt(raw_last.get("amount"), 0)},
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="基于历史日线和最新分钟线生成中文技术指标报告")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--minute-period", default="1m", choices=["1m", "5m", "10m"])
    parser.add_argument("--minute-lookback-days", type=int, default=10)
    args = parser.parse_args()

    ts_code = args.ts_code.upper()
    daily = compute_daily_chain(ts_code)
    minute = compute_minute_chain(ts_code, period=args.minute_period, lookback_days=args.minute_lookback_days)
    moneyflow_df, moneyflow_source = load_moneyflow_parquet(ts_code)

    daily_last = daily["last"]
    minute_last = minute["last"]
    minute_raw_last = minute["raw"].iloc[-1] if not minute["raw"].empty else pd.Series(dtype=object)

    moneyflow_sum = "-"
    if not moneyflow_df.empty and "net_mf_amount" in moneyflow_df.columns:
        moneyflow_sum = _fmt(pd.to_numeric(moneyflow_df["net_mf_amount"], errors="coerce").sum())

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
            "## 综合信号",
            "",
            f"- 最终判定：{final_signal['signal']}",
            f"- 判定逻辑：{final_signal['logic']}",
            f"- 建议操作：{final_signal['action']}",
            "",
            "## 建议继续关注的 Tushare 数据",
            "",
            "这些数据项尚未纳入当前自动抓取链路，但非常适合后续扩展：",
            "",
        ]
    )

    lines.extend(
        [
            make_table(recommended_tushare_datasets(), ["数据项", "用途", "怎么用"]),
            "",
        ]
    )

    lines.extend(
        [
            "## 使用规则",
            "",
            "- 趋势判断基于完整历史日线，再结合最新一分钟线做实时修正。",
            "- 时机判断基于分钟线序列以及最新一分钟线。",
            "- 先看趋势判断，再决定这只股票是否值得参与。",
            "- 再看时机判断，用来决定当天应买入、卖出还是观望。",
            "",
            "## 计算规则",
            "",
            "- 日线趋势由完整历史日线计算得到。",
            "- 实时分钟指标由已有分钟线序列加上最新抓取的一分钟线共同计算。",
            "- 如果新抓取的一分钟线与已有记录 `bar_time` 相同，则保留 `fetch_time` 更新的那一条。",
            "",
        ]
    )

    out_path = report_path(ts_code)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8-sig")
    print(out_path.as_posix())


if __name__ == "__main__":
    main()
