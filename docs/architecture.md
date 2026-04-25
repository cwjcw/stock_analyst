# stock_analyst 架构说明

## 目标

`stock_analyst` 是一个 A 股个股分析项目，用本地 Parquet 数据层承接 Tushare、QMT 和少量参考数据，再生成中文指标报告和 Web 控制台分析结果。

核心目标：

- 沉淀可增量更新的个股行情、资金流和分钟线数据。
- 将趋势、资金、强弱、板块和市场资金环境合并到同一套分析上下文。
- 保持脚本可单独运行，方便日常任务、调试和后续定时化。

## 数据层

基础个股数据放在：

```text
data/market_store/{ts_code}/
```

主要文件：

- `{ts_code}_tushare_daily.parquet`：Tushare 日线行情。
- `{ts_code}_tushare_moneyflow.parquet`：Tushare 个股资金流。
- `{ts_code}_tushare_moneyflow_dc.parquet`：东财个股资金流向。
- `{ts_code}_qmt_10min.parquet`：QMT 10 分钟线。

市场和板块数据放在：

```text
data/market_store/_market/
```

主要文件：

- `moneyflow_mkt_dc.parquet`：东财大盘资金流。
- `moneyflow_ind_dc.parquet`：东财行业和概念板块资金流。
- `moneyflow_hsgt.parquet`：沪深港通资金流向。
- `moneyflow_manifest.json`：资金流任务元信息。

## 脚本入口

### 基础行情和 QMT

[export_tushare_qmt_all.py](../scripts/data/export_tushare_qmt_all.py)

负责 Tushare 日线、Tushare 原始资金流、QMT 10 分钟线的全量或增量导出。

```powershell
.\.venv\Scripts\python.exe scripts\data\export_tushare_qmt_all.py --ts-code 000001.SZ
```

### 扩展资金流

[fetch_moneyflow_history.py](../scripts/data/fetch_moneyflow_history.py)

负责东财个股资金流、行业/概念板块资金流、大盘资金流、沪深港通资金流。默认窗口为过去一年。

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --all --workers 4
```

### 输出验证

[validate_analysis_outputs.py](../scripts/data/validate_analysis_outputs.py)

检查本地分析所需数据是否可读、是否为空、关键列是否存在，并验证日线指标链能否计算。

```powershell
.\.venv\Scripts\python.exe scripts\data\validate_analysis_outputs.py --ts-code 000001.SZ
```

### 报告生成

[generate_md_report.py](../scripts/report/generate_md_report.py)

读取本地日线、资金流和分钟线数据，生成 Markdown 分析报告。

```powershell
.\.venv\Scripts\python.exe scripts\report\generate_md_report.py --ts-code 000001.SZ --minute-period 10m
```

## 核心模块

- [indicators.py](../src/stock_analyst/indicators.py)：MA、MACD、DMI、KDJ、RSI、WR、BIAS、ROC、CCI、OBV、QRR 等指标计算。
- [indicator_chain.py](../src/stock_analyst/indicator_chain.py)：读取本地 Parquet 并生成日线/分钟线分析链。
- [stock_reference.py](../src/stock_analyst/stock_reference.py)：股票代码、名称和概念板块参考信息。
- [storage.py](../src/stock_analyst/storage.py)：通用路径和写入辅助函数。

## Web 控制台

[web/app.py](../web/app.py) 提供 Flask 控制台：

- 用户注册、登录、找回密码。
- 自选股分组管理。
- 股票名称和概念板块补全。
- 多选股票后触发本地分析。
- 导出 Markdown 分析报告。

启动：

```powershell
.\.venv\Scripts\python.exe web\app.py
```

访问：

```text
http://127.0.0.1:8080
```

## 日常推荐流程

1. 更新基础行情：

```powershell
.\.venv\Scripts\python.exe scripts\data\export_tushare_qmt_all.py --all --workers 8
```

2. 更新扩展资金流：

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --all --workers 4
```

3. 验证样本输出：

```powershell
.\.venv\Scripts\python.exe scripts\data\validate_analysis_outputs.py --ts-code 000001.SZ
```

4. 生成报告：

```powershell
.\.venv\Scripts\python.exe scripts\report\generate_md_report.py --ts-code 000001.SZ --minute-period 10m
```
