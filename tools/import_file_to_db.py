import argparse
import pandas as pd
from sqlalchemy import create_engine

from helpers.db_constants import QUOTATION_INTRADAY_COLUMN_MAP, QUOTATION_INTRADAY_TABLE_NAME

# 配置常量
DB_URI = "mysql+pymysql://zcs:2025zcsdaydayup@146.56.231.251/stock_info"


def load_local_file(filepath, column_map):
    """读取行业分类Excel/CSV文件并重命名字段"""
    if filepath.endswith(".csv"):
        df = pd.read_csv(filepath, usecols=column_map.keys())
    else:
        df = pd.read_excel(filepath, usecols=column_map.keys())
    return df.rename(columns=column_map)


def save_to_table(df, table_name, db_uri):
    """将数据写入MySQL数据库表"""
    engine = create_engine(db_uri)
    try:
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    finally:
        engine.dispose()


if __name__ == "__main__":
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description="上传文件到数据库表")
    parser.add_argument("--src_file", required=True, type=str, help="input file")
    args = parser.parse_args()
    df = load_local_file(args.src_file, QUOTATION_INTRADAY_COLUMN_MAP)
    save_to_table(df, QUOTATION_INTRADAY_TABLE_NAME, DB_URI)
