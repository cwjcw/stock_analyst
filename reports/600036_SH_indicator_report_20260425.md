# 600036.SH Indicator Report

- Generated At: 2026-04-25 07:44:36
- Daily Source: E:/code/stock_analyst/data/market_store/600036.SH/600036.SH_tushare_daily.parquet
- Minute Source: E:/code/stock_analyst/data/qmt_selected_minutes/20260425/qmt_latest_1m.parquet
- Moneyflow Source: E:/code/stock_analyst/data/market_store/600036.SH/600036.SH_tushare_moneyflow.parquet
- Minute Period: 1m

## Summary

- Daily Trend: neutral
- Daily Summary: trend=neutral
- Minute Trend: neutral
- Minute Summary: trend=neutral
- Minute Bars Loaded: 1
- Moneyflow Rows: 58
- Moneyflow Net Sum: -619646.0000

## Two-Level Decision

- Trend Decision: Neutral
- Timing Decision: Wait

### Trend Notes

- Daily moving averages are mixed.
- MACD structure is on the stronger side.

### Timing Notes

- Need more minute bars to judge timing.

## Daily Indicators

| Metric | Value |
| --- | --- |
| close | 39.3800 |
| MA5 | 39.7340 |
| MA10 | 39.6110 |
| MA20 | 39.5205 |
| MA60 | 39.1805 |
| DIF | 0.0781 |
| DEA | 0.0665 |
| MACD | 0.0231 |
| RSI6 | 30.6569 |
| RSI12 | 45.9184 |
| K | 57.9317 |
| D | 61.8215 |
| J | 50.1519 |

## Minute Indicators

| Metric | Value |
| --- | --- |
| bar_time | 20260424150000 |
| fetch_time | 20260425074431 |
| close | - |
| MA5 | - |
| MA10 | - |
| DIF | - |
| DEA | - |
| MACD | - |
| RSI6 | - |
| K | - |
| D | - |
| J | - |
| volume | 0 |
| amount | 0 |

## Usage Rule

- Trend decision is based on full daily history plus the latest minute bar as a realtime correction.
- Timing decision is based on the intraday minute sequence and the latest minute bar.
- Use trend to decide whether the stock is worth participating in.
- Use timing to decide when to buy, sell, or wait during the day.

## Calculation Rule

- Daily trend is computed from full daily history.
- Realtime minute indicators are computed from the existing minute sequence plus the latest fetched minute bar.
- If the latest fetched minute bar shares the same `bar_time`, the latest `fetch_time` version is kept.
