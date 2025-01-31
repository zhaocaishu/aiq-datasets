# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

HEADER = [
    "Instrument",
    "Date",
    "Close",
    "Open",
    "High",
    "Low",
    "Pre_Close",
    "Change",
    "Pct_Chg",
    "Volume",
    "AMount",
]


class ExportCodeData(object):
    def __init__(self, args):
        self.__init_db(args.host, args.user, args.passwd)

    def __init_db(self, host, user, passwd):
        """init db, and show tables"""
        self.connection = mysql.connector.connect(
            host=host, user=user, passwd=passwd, database="stock_info"
        )

        with self.connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")

            for table in cursor:
                print(table)

    def export_data(self, save_dir):
        """导出数据到文件
        Args:
            save_dir: 导出到目录
        """
        # 创建目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 上市的全部指数代码
        codes = ["000300.SH", "000903.SH", "000905.SH"]

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            for code in codes:
                # 查询数据
                query = (
                    "SELECT daily.* FROM ts_idx_index_daily daily"
                    "WHERE daily.ts_code='%s' LIMIT 50000" % code
                )

                print(query)

                cursor.execute(query)

                with open("%s/%s.csv" % (save_dir, code), "w", newline="") as csvfile:
                    writer = csv.writer(
                        csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                    )
                    writer.writerow(HEADER)

                    for row in cursor:
                        list_row = list(row)
                        t_date = list_row[1]
                        list_row[1] = (
                            t_date[0:4] + "-" + t_date[4:6] + "-" + t_date[6:8]
                        )
                        writer.writerow(list_row)


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
    export.export_data(os.path.join(args.save_dir, "features"))

    print("End export data")
