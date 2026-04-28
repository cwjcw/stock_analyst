# Tushare 个股分析数据清单

目标：围绕个股趋势、买卖点、资金、题材、情绪、基本面和风险事件，沉淀可复用的本地数据层。

## 已有数据

当前 `data/market_store/{ts_code}/` 已经包含：

- `{ts_code}_tushare_daily.parquet`：日线 OHLCV，用于趋势、波动、突破、均线、量价。
- `{ts_code}_tushare_moneyflow.parquet`：Tushare 个股资金流，用于主力/大小单资金拆分。
- `{ts_code}_qmt_10min.parquet`：10 分钟线，用于盘中节奏、短周期买卖点。

## 本次补充的资金流数据

脚本：[fetch_moneyflow_history.py](../scripts/data/fetch_moneyflow_history.py)

默认时间窗口为过去一年：

- `moneyflow_dc`：东财个股资金流，落盘到 `{ts_code}_tushare_moneyflow_dc.parquet`。
- `moneyflow_ind_dc`：东财行业和概念板块资金流，落盘到 `_market/moneyflow_ind_dc.parquet`。
- `moneyflow_mkt_dc`：东财大盘资金流，落盘到 `_market/moneyflow_mkt_dc.parquet`。
- `moneyflow_hsgt`：沪深港通资金流向，落盘到 `_market/moneyflow_hsgt.parquet`。

推荐日常命令：

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --all --workers 4
```

仅更新市场、板块、沪深港通：

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --market-only
```

单股调试：

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_moneyflow_history.py --ts-code 000001.SZ
```

## 建议继续纳入的个股分析数据

以下数据已由 [fetch_analysis_factors.py](../scripts/data/fetch_analysis_factors.py) 纳入本地采集，并由 [generate_md_report.py](../scripts/report/generate_md_report.py) 汇总写入 Markdown：

- `daily_basic`
- `bak_daily`
- `fina_indicator`
- `income`
- `balancesheet`
- `cashflow`
- `hsgt_top10`
- `top10_holders`
- `top10_floatholders`
- `forecast`
- `express`
- `limit_list_d`

运行命令：

```powershell
.\.venv\Scripts\python.exe scripts\data\fetch_analysis_factors.py --all-watchlist --user-id cwjcw --workers 3
```

### 趋势和量价

- `daily` / `pro_bar`：日线行情。
- `weekly` / `monthly`：周线、月线，用于中期趋势确认。
- `daily_basic`：换手率、量比、市值、PE/PB/PS，用于估值和活跃度。
- `adj_factor`：复权因子，用于还原真实走势。
- `stk_factor`：Tushare 技术因子，如均线、MACD、KDJ、RSI 等。

### 资金和交易行为

- `moneyflow`：Tushare 个股资金流，已存在。
- `moneyflow_dc`：东财个股资金流，本次补充。
- `moneyflow_ind_dc`：东财行业/概念板块资金流，本次补充。
- `moneyflow_mkt_dc`：大盘资金流，本次补充。
- `moneyflow_hsgt`：沪深港通资金流，本次补充。
- `hsgt_top10`：沪深港通成交活跃股，用于北向偏好识别。
- `top_list` / `top_inst`：龙虎榜和机构席位，用于异动、游资和机构行为。

### 题材和板块

- `dc_index` / `dc_member`：东财行业、概念、地域板块及成分股。
- `ths_index` / `ths_member`：同花顺概念和成分股。
- `index_classify` / `index_member_all`：指数分类和成分股。
- `sw_daily`：申万行业行情，用于稳定行业轮动口径。

### 情绪和强弱

- `limit_list_d`：涨跌停明细。
- `limit_step`：连板梯队。
- `kpl_list`：开盘啦题材和涨停数据。
- `dc_hot` / `ths_hot`：东财、同花顺热榜。

### 基本面和业绩

- `income`：利润表，重点看营收、净利润趋势。
- `balancesheet`：资产负债表，重点看杠杆和资产结构。
- `cashflow`：现金流量表，重点看经营现金流质量。
- `fina_indicator`：ROE、毛利率、净利率、负债率等核心指标。
- `forecast` / `express`：业绩预告和业绩快报。
- `disclosure_date`：财报披露计划。

### 事件和风险

- `anns_d`：公告。
- `news` / `major_news`：新闻和重大新闻。
- `research_report`：研报。
- `stock_st`：ST 状态。
- `pledge_stat` / `pledge_detail`：股权质押。
- `repurchase`：回购。
- `share_float`：限售解禁。
- `holder_num`：股东户数。
- `stk_holdertrade`：股东增减持。

## 落盘建议

个股级数据放在：

```text
data/market_store/{ts_code}/{ts_code}_tushare_{dataset}.parquet
```

市场和板块级数据放在：

```text
data/market_store/_market/{dataset}.parquet
```

每个批量任务同时写入 manifest，记录接口、时间窗口、行数和失败片段，方便后续排查权限、限流和空数据问题。
