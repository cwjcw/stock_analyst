# stock_analyst

A 股数据采集与技术指标分析项目（Tushare + QMT）。

## 当前数据口径（仅 Parquet）
每只股票基础数据固定 3 个 Parquet 文件：
1. `Tushare` 日线
2. `Tushare` 资金流
3. `QMT` 10 分钟线

扩展资金流数据：
- 个股：`{ts_code}_tushare_moneyflow_dc.parquet`
- 市场：`data/market_store/_market/moneyflow_mkt_dc.parquet`
- 板块：`data/market_store/_market/moneyflow_ind_dc.parquet`
- 沪深港通：`data/market_store/_market/moneyflow_hsgt.parquet`

特点：
- 首次全量，后续自动增量
- 增量时回退 2 天重抓并去重（幂等）
- 多线程并发导出

## 项目结构
```text
stock_analyst/
├─ src/stock_analyst/            # 核心模块
├─ scripts/
│  ├─ data/
│  │  └─ export_tushare_qmt_all.py   # 全量/增量导出（Parquet）
│  ├─ report/
│  │  └─ generate_md_report.py       # 生成个股分析报告（MD）
│  └─ runtime/
│     ├─ run_qmt_realtime.py         # QMT 实时行情抓取
│     └─ manage_watchlist.py         # 自选股管理
├─ web/                          # Web 控制台（Flask）
├─ frontend/                     # 前端预留目录
├─ docs/                         # 架构文档
├─ data/market_store/            # 股票数据目录（不入 Git）
│  └─ {ts_code}/
│     ├─ {ts_code}_tushare_daily.parquet
│     ├─ {ts_code}_tushare_moneyflow.parquet
│     └─ {ts_code}_qmt_10min.parquet
├─ reports/                      # Markdown 报告
├─ .env.example
└─ README.md
```

## 环境准备
1. 进入虚拟环境（Windows）
```powershell
.\.venv\Scripts\activate
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 配置 `.env`（至少包含 `TUSHARE_TOKEN`）

邮件相关配置（注册欢迎邮件、找回密码邮件）也统一放在 `.env`：
```bash
SMTP_HOST=smtp.example.com
SMTP_PORT=465
SMTP_USERNAME=your_email@example.com
SMTP_PASSWORD=your_smtp_password_or_app_password
SMTP_FROM=your_email@example.com
```

## 数据导出命令
脚本：`scripts/data/export_tushare_qmt_all.py`

### 1) 首次全量导出
```bash
python scripts/data/export_tushare_qmt_all.py --all --daily-days 500 --minute-days 90 --workers 8 --out-dir data/market_store
```

### 2) 后续增量更新（每天跑）
```bash
python scripts/data/export_tushare_qmt_all.py --all --workers 8 --out-dir data/market_store
```

这是日常默认命令：每天只跑这一条即可（自动增量 + 自动发现新股）。

### 3) 强制全量刷新
```bash
python scripts/data/export_tushare_qmt_all.py --all --full-refresh --daily-days 500 --minute-days 90 --workers 8 --out-dir data/market_store
```

### 4) 单只股票调试
```bash
python scripts/data/export_tushare_qmt_all.py --ts-code 000099.SZ --daily-days 500 --minute-days 90 --out-dir data/market_store
```

## 扩展资金流导出

脚本：`scripts/data/fetch_moneyflow_history.py`

默认抓取过去一年，包含个股资金流向（DC）、东财行业/概念板块资金流向（DC）、大盘资金流向（DC）、沪深港通资金流向。

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --all --workers 4
```

只更新板块、大盘、沪深港通：
```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --market-only
```

完整数据清单见：`docs/tushare_stock_analysis_data.md`

## 扩展分析指标导出

脚本：`scripts/data/fetch_analysis_factors.py`

用于补充报告中的估值、活跃度、备用行情、财务质量、三张表、北向活跃股、股东结构、业绩预告/快报、涨跌停情绪等数据。

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_analysis_factors.py --all-watchlist --user-id cwjcw --workers 3
```

单只股票调试：
```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_analysis_factors.py --ts-code 601398.SH --workers 1
```

## 数据输出验证

脚本：`scripts/data/validate_analysis_outputs.py`

检查指定股票的日线、传统资金流、东财个股资金流、QMT 10 分钟线（默认可选）、大盘资金流、板块资金流、沪深港通资金流是否能成功读取，并验证日线指标链能否计算。

```powershell
.\.venv\Scripts\python.exe scripts\data\validate_analysis_outputs.py --ts-code 000001.SZ
```

## Web 控制台
```bash
python web/app.py
```
访问：`http://127.0.0.1:8080`

当前 Web 端结构：
- 未登录：注册、登录、找回密码
- 已登录：总览、股票关注、分析结果、用户中心、使用说明

当前 Web 端能力：
- 注册时必须填写用户名、邮箱、密码（至少 8 位）
- 注册成功后发送欢迎邮件
- 忘记密码时按用户名重置，并将随机 8 位密码发到注册邮箱
- 登录后按分组管理自选股票
- 多选股票后触发分析，直接查看中文结果
- 支持导出 Markdown 分析报告

## 数据与 Git
`data/market_store/` 已在 `.gitignore` 中忽略，不会同步到 Git。

## QMT 指定股票最新分钟线抓取
```bash
python scripts/data/qmt_fetch_all_kline_once.py --period 1m --repair-missing --out-dir data/qmt_selected_minutes
```

说明：
- 默认股票来源为 `stock_analyst.db` 里的 `stocks.stock_id`
- 推荐默认周期为 `1m`
- 若有缺失股票，再只对缺失股票补拉历史并二次抓取
- 也可以显式指定股票：
```bash
python scripts/data/qmt_fetch_all_kline_once.py --period 1m --ts-code 000001.SZ 600519.SH
```

## QMT 盘中实时 10 分钟聚合（推荐）
脚本：
`scripts/runtime/qmt_realtime_10m_aggregator.py`

逻辑：
- 订阅全市场实时行情（`SH` + `SZ`）
- 在 `09:15`、`09:20`、`09:25` 记录集合竞价快照
- 从 `09:30` 开始，根据实时行情在内存里聚合 10 分钟 OHLC
- 自动跳过午休 `11:30-13:00`
- 落盘文件：
  - `data/qmt_realtime/YYYYMMDD/qmt_auction_snapshots.parquet`
  - `data/qmt_realtime/YYYYMMDD/qmt_10m_live.parquet`

运行示例：
```bash
python scripts/runtime/qmt_realtime_10m_aggregator.py --out-dir data/qmt_realtime --until 15:00
```

## QMT 交易日定时抓取脚本
脚本：
`scripts/runtime/schedule_qmt_full_kline.py`

规则：
- 先判断今天是否交易日（SSE 交易日历）
- 若是交易日：
  - 集合竞价阶段在 09:15、09:20、09:25 各抓一次
  - 从 09:30 开始，每 10 分钟抓一次“指定股票最新一分钟线”（默认到 15:00）
  - 自动跳过午休时段 11:30-13:00

运行示例：
```bash
python scripts/runtime/schedule_qmt_full_kline.py --period 1m --out-dir data/qmt_selected_minutes --until 15:00
```
