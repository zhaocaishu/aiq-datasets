# -*- coding: utf-8 -*-
import os
import argparse
import csv
import pandas as pd

from sqlalchemy import create_engine

HEADER = [
    "Instrument",
    "Date",
    "Close",
    "Open",
    "High",
    "Low",
    "Change",
    "Pct_Chg",
    "Volume",
    "AMount",
    "Turnover_rate"
]


class ExportCodeData(object):
    def __init__(self, args):
        self.__init_db(args.host, args.user, args.passwd)

    def __init_db(self, host, user, passwd):
        self.engine = create_engine(
            "mysql+pymysql://zcs:2025zcsdaydayup@127.0.0.1/stock_info"
        )

    def export_data(self, save_dir):
        """导出数据到文件
        Args:
            save_dir: 导出到目录
        """
        # 创建目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 上市的全部指数代码
        codes = ["000300.SH", "000903.SH", "000905.SH", "000906.SH"]

        # 从数据库导出数据
        for code in codes:
            # 查询数据
            print("Exporting %s" % code)

            with self.engine.connect() as conn:
                df = pd.read_sql(
                    "SELECT * FROM ts_idx_index_daily_choice WHERE index_code = %s",
                    conn,
                    params=(code,),
                )

            with open("%s/%s.csv" % (save_dir, code), "w", newline="") as csvfile:
                writer = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                writer.writerow(HEADER)

                for _, row in df.iterrows():
                    list_row = list(row)
                    writer.writerow(list_row)

    def close(self):
        """关闭数据库连接"""
        self.engine.dispose()


if __name__ == "__main__":
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description="查询并保存指数日线数据")
    parser.add_argument(
        "-save_dir", required=True, type=str, help="Directory of the saved files"
    )
    parser.add_argument(
        "-host", type=str, default="127.0.0.1", help="The address of database"
    )
    parser.add_argument(
        "-user", type=str, default="zcs", help="The user name of database"
    )
    parser.add_argument(
        "-passwd", type=str, default="2025zcsdaydayup", help="The password of database"
    )

    args = parser.parse_args()

    # 解析命令行中的参数，得到需要爬取的数据、日期范围
    print("Begin export data, save directory: %s" % args.save_dir)

    export = ExportCodeData(args)
    try:
        export.export_data(os.path.join(args.save_dir, "features"))
    finally:
        export.close()  # 确保连接被关闭

    print("End export data")
