# stock_analyst 架构设计（V1）

## 目标

1. 用 Tushare 抓取全市场历史数据，入库 MariaDB（`192.168.1.251:3307`，库名 `stock`）。
2. 用 QMT 抓取实时数据，默认按指定用户的自选股列表运行。
3. 融合历史 + 实时数据计算技术指标与趋势方向，用于买卖决策参考。
4. 预留前端管理能力（用户管理、自选股输入、触发计算、页面展示结果）。

## 数据流

1. `scripts/init_mariadb.py`
   - 初始化数据库与表结构。
2. `scripts/sync_tushare_market.py`
   - 按交易日抓取全市场：`stock_basic`、`daily_quotes`、`daily_basic`、`stk_limit`。
3. `scripts/manage_watchlist.py`
   - 管理用户与自选股。
4. `scripts/run_qmt_realtime.py`
   - 读取用户自选股，轮询 QMT `get_full_tick`，写入 `realtime_ticks`。
   - 使用历史+实时合成最新K线，计算指标，写入 `indicator_snapshot`。

## 数据表

- `users`: 用户信息。
- `user_watchlist`: 用户自选股。
- `stock_basic`: 股票静态基础信息。
- `daily_quotes`: 日线行情。
- `daily_basic`: 日线基础指标（换手率、量比、估值、市值）。
- `stk_limit`: 涨跌停价。
- `realtime_ticks`: 实时 tick 数据。
- `indicator_snapshot`: 每个用户每只股票最新指标快照（趋势+信号+json详情）。

## 实时指标计算逻辑

- 指标范围：MA/MACD/DMI/KDJ/RSI/WR/BIAS/ROC/CCI/OBV/QRR。
- 趋势判定（简化版）：
  - `bullish`: `close > MA20 > MA60` 且 `DIF > DEA`
  - `bearish`: `close < MA20 < MA60` 且 `DIF < DEA`
  - 否则 `neutral`
- 震荡信号：
  - `RSI6 >= 80` -> overbought
  - `RSI6 <= 20` -> oversold
- 量能信号：
  - `QRR >= 1.5` -> active
  - `QRR <= 0.7` -> weak

## 前端预留（本期仅设计）

- 前端目录：`frontend/`
- 后续建议接口：
  - `POST /api/users`
  - `POST /api/users/{user_code}/watchlist`
  - `DELETE /api/users/{user_code}/watchlist/{ts_code}`
  - `POST /api/users/{user_code}/compute`
  - `GET /api/users/{user_code}/signals`
  - `GET /api/users/{user_code}/ticks`

## 启动顺序建议

1. 初始化库表
2. 同步 Tushare 历史
3. 维护用户和自选股
4. 启动 QMT 实时计算进程

