import argparse
import pandas as pd
from sqlalchemy import create_engine


# 配置常量
DB_URI = "mysql+pymysql://zcs:2025zcsdaydayup@146.56.231.251/stock_info"


def export_table_to_file(table_name: str, db_uri: str, output_file: str) -> None:
    """将数据库表导出到本地CSV或Excel文件"""
    engine = create_engine(db_uri)
    try:
        df = pd.read_sql_table(table_name, con=engine)
    finally:
        engine.dispose()

    # 根据文件后缀决定导出格式
    if output_file.lower().endswith(".csv"):
        df.to_csv(output_file, index=False)
    else:
        df.to_excel(output_file, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导出数据库表数据到本地文件")
    parser.add_argument(
        "--table_name",
        required=True,
        type=str,
        help="输出的表名",
    )
    parser.add_argument(
        "--export_file",
        required=True,
        type=str,
        help="输出文件路径，用于从数据库导出",
    )
    args = parser.parse_args()

    export_table_to_file(args.table_name, DB_URI, args.export_file)
