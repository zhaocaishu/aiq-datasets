# 申万行业成员表名
INDUSTRY_MEMBER_TABLE_NAME = "ts_idx_index_member_all_sw"

# 申万行业成员表DDL
INDUSTRY_MEMBER_TABLE_DDL = """
CREATE TABLE ts_idx_index_member_all_sw (
  ts_code varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '成分股票代码',
  l1_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '一级行业名称',
  l2_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '二级行业名称',
  l3_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '三级行业名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='申万行业成分'
"""

# 申万行业成员表字段映射
INDUSTRY_MEMBER_COLUMN_MAP = {
    "股票代码": "ts_code",
    "新版一级行业": "l1_name",
    "新版二级行业": "l2_name",
    "新版三级行业": "l3_name",
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

# 指数日线行情表名
INDEX_DAILY_TABLE_NAME = "ts_idx_index_daily_choice"

# 指数日线行情表DDL
INDEX_DAILY_TABLE_DDL = """
CREATE TABLE ts_idx_index_daily_choice (
  index_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指数代码',
  trade_date varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易日',
  close float DEFAULT NULL COMMENT '收盘点位',
  open float DEFAULT NULL COMMENT '开盘点位',
  high float DEFAULT NULL COMMENT '最高点位',
  low float DEFAULT NULL COMMENT '最低点位',
  `change` float DEFAULT NULL COMMENT '涨跌点',
  pct_chg float DEFAULT NULL COMMENT '涨跌幅（%）',
  vol float DEFAULT NULL COMMENT '成交量（万股）',
  amount float DEFAULT NULL COMMENT '成交额（万元）',
  turnover float DEFAULT NULL COMMENT '换手率（%）',
  UNIQUE KEY uni_index_code_trade_date (index_code,trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='指数日线行情'
"""

# 指数日线表字段映射
INDEX_DAILY_COLUMN_MAP = {
    "指数代码": "index_code",
    "交易日期": "trade_date",
    "收盘价": "close",
    "开盘价": "open",
    "最高价": "high",
    "最低价": "low",
    "涨跌": "change",
    "涨跌幅(%)": "pct_chg",
    "成交量(万股)": "vol",
    "成交额(万元)": "amount",
    "换手率(%)": "turnover",
}

# 股票日内特征表名
QUOTATION_INTRADAY_TABLE_NAME = "ts_quotation_intraday_daily"

# 股票日内特征表DDL
QUOTATION_INTRADAY_TABLE_DDL = """
CREATE TABLE ts_quotation_intraday_daily (
  ts_code varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '股票代码',
  trade_date varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易日期',
  tail_ratio float DEFAULT NULL COMMENT '尾盘成交占比',
  vwap float DEFAULT NULL COMMENT '平均价格',
  UNIQUE KEY uni_ts_code_trade_date (ts_code,trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票日内特征'
"""

# 股票日内特征字段映射
QUOTATION_INTRADAY_COLUMN_MAP = {
    "ts_code": "ts_code",
    "trade_date": "trade_date",
    "tail_ratio": "tail_ratio",
    "vwap": "vwap",
}