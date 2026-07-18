# 申万行业成分表名
INDUSTRY_CONS_TABLE_NAME = "ts_idx_industry_cons"

# 申万行业成分表DDL
INDUSTRY_CONS_TABLE_DDL = """
CREATE TABLE ts_idx_industry_cons (
  l1_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '一级行业代码',
  l1_name varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '一级行业名称',
  l2_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '二级行业代码',
  l2_name varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '二级行业名称',
  l3_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '三级行业代码',
  l3_name varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '三级行业名称',
  ts_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '成分股票代码',
  ts_name varchar(40) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '成分股票名称',
  in_date varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '纳入日期',
  out_date varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '剔除日期',
  is_new varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '是否最新: Y是N否',
  UNIQUE KEY uni_index_code_ts_code (l3_code,ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='申万行业成分构成(分级)' |
"""

# 申万行业成分表字段映射
INDUSTRY_CONS_COLUMN_MAP = {
    "L1代码": "l1_code",
    "L1名称": "l1_name",
    "L2代码": "l2_code",
    "L2名称": "l2_name",
    "L3代码": "l3_code",
    "L3名称": "l3_name",
    "成分股票代码": "ts_code",
    "成分股票名称": "ts_name",
    "纳入日期": "in_date",
    "剔除日期": "out_date",
    "是否最新Y是N否": "is_new",
}

# 指数成分表名
INDEX_CONS_TABLE_NAME = "ts_idx_index_cons"

# 指数成分表DDL
INDEX_CONS_TABLE_DDL = """
CREATE TABLE ts_idx_index_cons (
  index_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指数代码',
  ts_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '成分股代码',
  trade_date varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易日期',
  UNIQUE KEY uni_index_code_ts_code_trade_date (index_code,ts_code,trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='指数成分'
"""

# 指数成分表字段映射
INDEX_CONS_COLUMN_MAP = {
    "指数代码": "index_code",
    "证券代码": "ts_code",
    "交易日期": "trade_date",
}

# 指数日行情表名
INDEX_DAILY_TABLE_NAME = "ths_idx_index_daily"

# 指数日行情表DDL
INDEX_DAILY_TABLE_DDL = """
CREATE TABLE ths_idx_index_daily (
  index_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指数代码',
  trade_date varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易日期',

  pre_close decimal(20,6) DEFAULT NULL COMMENT '前收盘价',
  open decimal(20,6) DEFAULT NULL COMMENT '开盘价',
  high decimal(20,6) DEFAULT NULL COMMENT '最高价',
  low decimal(20,6) DEFAULT NULL COMMENT '最低价',
  close decimal(20,6) DEFAULT NULL COMMENT '收盘价',
  pct_chg decimal(20,10) DEFAULT NULL COMMENT '涨跌幅',

  vol bigint DEFAULT NULL COMMENT '成交量',
  amount decimal(30,6) DEFAULT NULL COMMENT '成交金额',
  turnover_rate decimal(20,10) DEFAULT NULL COMMENT '换手率',
  turnover_rate_f decimal(20,10) DEFAULT NULL COMMENT '换手率（自由流通股本）',
  swing decimal(20,10) DEFAULT NULL COMMENT '振幅',

  up_days int DEFAULT NULL COMMENT '连涨天数',
  down_days int DEFAULT NULL COMMENT '连跌天数',

  constituent_raise_number int DEFAULT NULL COMMENT '指数成份上涨数量',
  constituent_fall_number int DEFAULT NULL COMMENT '指数成份下跌数量',
  constituent_up_number int DEFAULT NULL COMMENT '指数成份涨停数量',
  constituent_dl_number int DEFAULT NULL COMMENT '指数成份跌停数量',

  constituent_chg_ratio_aa decimal(20,10) DEFAULT NULL COMMENT '指数成份平均涨跌幅',
  constituent_chg_ratio_m decimal(20,10) DEFAULT NULL COMMENT '指数成份涨跌幅中位数',

  new_high_num int DEFAULT NULL COMMENT '指数成份新高家数',
  new_low_num int DEFAULT NULL COMMENT '指数成份新低家数',

  up_num_ratio decimal(20,10) DEFAULT NULL COMMENT '涨停成份数量占比',
  over250_avgclose_num_ratio decimal(20,10) DEFAULT NULL COMMENT '站上250日均线的成份数量占比',
  raise_num_ratio decimal(20,10) DEFAULT NULL COMMENT '上涨成份数量占比',

  UNIQUE KEY uni_index_code_trade_date (index_code, trade_date),
  KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='指数日行情及技术指标'
"""


# 指数日行情表字段映射
INDEX_DAILY_COLUMN_MAP = {
    "thscode": "index_code",
    "time": "trade_date",
    "ths_pre_close_index": "pre_close",
    "ths_open_price_index": "open",
    "ths_high_price_index": "high",
    "ths_low_index": "low",
    "ths_close_price_index": "close",
    "ths_chg_ratio_index": "pct_chg",
    "ths_vol_index": "vol",
    "ths_trans_amt_index": "amount",
    "ths_turnover_ratio_index": "turnover_rate",
    "ths_free_turnover_ratio_index": "turnover_rate_f",
    "ths_swing_index": "swing",
    "ths_up_days_index": "up_days",
    "ths_down_days_index": "down_days",
    "ths_constituent_raise_number_index": "constituent_raise_number",
    "ths_constituent_fall_number_index": "constituent_fall_number",
    "ths_constituent_up_number_index": "constituent_up_number",
    "ths_constituent_dl_number_index": "constituent_dl_number",
    "ths_constituent_chg_ratio_aa_index": "constituent_chg_ratio_aa",
    "ths_constituent_chg_ratio_m_index": "constituent_chg_ratio_m",
    "ths_new_high_num_index": "new_high_num",
    "ths_new_low_num_index": "new_low_num",
    "ths_up_num_ratio_index": "up_num_ratio",
    "ths_over250_avgclose_num_ratio_hb_index": "over250_avgclose_num_ratio",
    "ths_raise_num_ratio_index": "raise_num_ratio",
}
