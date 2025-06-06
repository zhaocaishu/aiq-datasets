import pandas as pd
from sqlalchemy import create_engine

# 配置常量
DB_URI = "mysql+pymysql://zcs:2025zcsdaydayup@146.56.231.251/stock_info"
SOURCE_FILE = "/Users/didi/Downloads/SwClass/最新个股申万行业分类(完整版-截至7月末).xlsx"
TABLE_NAME = "ts_idx_index_member_all_sw"

# 字段映射
COLUMN_MAP = {
    "股票代码": "ts_code",
    "新版一级行业": "l1_name",
    "新版二级行业": "l2_name",
    "新版三级行业": "l3_name",
}

def load_stock_industry(filepath):
    """读取行业分类Excel文件并重命名字段"""
    df = pd.read_excel(filepath, usecols=COLUMN_MAP.keys())
    return df.rename(columns=COLUMN_MAP)

def save_to_mysql(df, table_name, db_uri):
    """将数据写入MySQL数据库表"""
    engine = create_engine(db_uri)
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    finally:
        engine.dispose()

if __name__ == "__main__":
    df = load_stock_industry(SOURCE_FILE)
    save_to_mysql(df, TABLE_NAME, DB_URI)
