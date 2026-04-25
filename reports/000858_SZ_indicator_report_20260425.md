# 000858.SZ Indicator Report

- Generated At: 2026-04-25 07:44:35
- Daily Source: E:/code/stock_analyst/data/market_store/000858.SZ/000858.SZ_tushare_daily.parquet
- Minute Source: E:/code/stock_analyst/data/qmt_selected_minutes/20260425/qmt_latest_1m.parquet
- Moneyflow Source: E:/code/stock_analyst/data/market_store/000858.SZ/000858.SZ_tushare_moneyflow.parquet
- Minute Period: 1m

## Summary

- Daily Trend: bearish
- Daily Summary: trend=bearish; RSI6=oversold
- Minute Trend: neutral
- Minute Summary: trend=neutral
- Minute Bars Loaded: 1
- Moneyflow Rows: 58
- Moneyflow Net Sum: -576419.9300

## Two-Level Decision

- Trend Decision: Bearish trend
- Timing Decision: Wait

### Trend Notes

- Daily close is below MA20 and MA60.
- MACD structure is on the weaker side.

### Timing Notes

- Need more minute bars to judge timing.

## Daily Indicators

| Metric | Value |
| --- | --- |
| close | 101.2700 |
| MA5 | 101.1240 |
| MA10 | 101.9990 |
| MA20 | 102.7110 |
| MA60 | 103.5418 |
| DIF | -0.5850 |
| DEA | -0.4095 |
| MACD | -0.3509 |
| RSI6 | 18.3673 |
| RSI12 | 29.5518 |
| K | 20.8153 |
| D | 23.8147 |
| J | 14.8165 |

## Minute Indicators

| Metric | Value |
| --- | --- |
| bar_time | 20260424150000 |
| fetch_time | 20260425074431 |
| close | 101.2700 |
| MA5 | - |
| MA10 | - |
| DIF | 0.0000 |
| DEA | 0.0000 |
| MACD | 0.0000 |
| RSI6 | - |
| K | - |
| D | - |
| J | - |
| volume | 1387 |
| amount | 14051617 |

## Usage Rule

- Trend decision is based on full daily history plus the latest minute bar as a realtime correction.
- Timing decision is based on the intraday minute sequence and the latest minute bar.
- Use trend to decide whether the stock is worth participating in.
- Use timing to decide when to buy, sell, or wait during the day.

## Calculation Rule

- Daily trend is computed from full daily history.
- Realtime minute indicators are computed from the existing minute sequence plus the latest fetched minute bar.
- If the latest fetched minute bar shares the same `bar_time`, the latest `fetch_time` version is kept.
