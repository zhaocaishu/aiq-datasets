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

# 股票日内特征表名
QUOTATION_INTRADAY_TABLE_NAME = "ts_quotation_intraday_daily"

# 股票日内特征表DDL
QUOTATION_INTRADAY_TABLE_DDL = """
CREATE TABLE ts_quotation_intraday_daily (
  ts_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '股票代码',
  trade_date varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易日期',
  tail_ratio float DEFAULT NULL COMMENT '尾盘成交占比',
  returns_skewness float DEFAULT NULL COMMENT '收益率偏度',
  price_vol_corr float DEFAULT NULL COMMENT '量价相关度',
  downside_ratio float DEFAULT NULL COMMENT '下行波动占比',
  UNIQUE KEY uni_ts_code_trade_date (ts_code,trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票日内特征'
"""

# 股票日内特征字段映射
QUOTATION_INTRADAY_COLUMN_MAP = {
    "ts_code": "ts_code",
    "trade_date": "trade_date",
    "tail_ratio": "tail_ratio",
    "returns_skewness": "returns_skewness",
    "price_vol_corr": "price_vol_corr",
    "downside_ratio": "downside_ratio",
}
