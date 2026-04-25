# 600519.SH Indicator Report

- Generated At: 2026-04-25 07:44:37
- Daily Source: E:/code/stock_analyst/data/market_store/600519.SH/600519.SH_tushare_daily.parquet
- Minute Source: E:/code/stock_analyst/data/qmt_selected_minutes/20260425/qmt_latest_1m.parquet
- Moneyflow Source: E:/code/stock_analyst/data/market_store/600519.SH/600519.SH_tushare_moneyflow.parquet
- Minute Period: 1m

## Summary

- Daily Trend: neutral
- Daily Summary: trend=neutral
- Minute Trend: neutral
- Minute Summary: trend=neutral
- Minute Bars Loaded: 1
- Moneyflow Rows: 58
- Moneyflow Net Sum: 266898.8800

## Two-Level Decision

- Trend Decision: Bullish trend
- Timing Decision: Wait

### Trend Notes

- Daily close is above MA20 and MA60.
- MACD structure is on the weaker side.

### Timing Notes

- Need more minute bars to judge timing.

## Daily Indicators

| Metric | Value |
| --- | --- |
| close | 1458.4900 |
| MA5 | 1421.9780 |
| MA10 | 1433.7680 |
| MA20 | 1441.1255 |
| MA60 | 1437.3557 |
| DIF | -2.2573 |
| DEA | -0.5856 |
| MACD | -3.3434 |
| RSI6 | 48.0558 |
| RSI12 | 47.9899 |
| K | 41.0463 |
| D | 34.9008 |
| J | 53.3375 |

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
