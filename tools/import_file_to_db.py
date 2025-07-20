import argparse
import pandas as pd
from sqlalchemy import create_engine

from helpers.db_constants import (
    INDEX_CONS_COLUMN_MAP,
    INDEX_CONS_TABLE_NAME,
)

# 配置常量
DB_URI = "mysql+pymysql://zcs:2025zcsdaydayup@146.56.231.251/stock_info"


def load_local_file(filepath: str, column_map: dict = None) -> pd.DataFrame:
    # 判断文件格式
    is_csv = filepath.lower().endswith(".csv")
    read_func = pd.read_csv if is_csv else pd.read_excel

    # 确定是否指定列
    read_kwargs = {}
    if column_map:
        read_kwargs["usecols"] = list(column_map.keys())

    df = read_func(filepath, **read_kwargs)

    # 重命名列名（如果需要）
    if column_map:
        df = df.rename(columns=column_map)

    return df


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
    df = load_local_file(args.src_file, INDEX_CONS_COLUMN_MAP)
    save_to_table(df, INDEX_CONS_TABLE_NAME, DB_URI)
