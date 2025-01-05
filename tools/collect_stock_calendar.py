# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

HEADER = ["Exchange", "Trade_date"]


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

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询数据
            query = (
                "SELECT DISTINCT exchange, date_format(cal_date, '%Y-%m-%d') "
                "FROM ts_basic_trade_cal WHERE is_open=1"
            )

            print(query)

            cursor.execute(query)

            with open(
                "%s/calendars/calendar.csv" % save_dir, "w", newline=""
            ) as csvfile:
                writer = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                writer.writerow(HEADER)

                for row in cursor:
                    list_row = list(row)
                    exchange = list_row[0]
                    trade_date = list_row[1]
                    writer.writerow([exchange, trade_date])


if __name__ == "__main__":
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description="获取全部交易日期")
    parser.add_argument(
        "-save_dir", required=True, type=str, help="Directory of the files"
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

    # 解析命令行中的参数，得到全部交易日期
    print("Begin export data, save_dir: %s" % args.save_dir)

    export = ExportCodeData(args)
    export.export_data(args.save_dir)

    print("End export data")
