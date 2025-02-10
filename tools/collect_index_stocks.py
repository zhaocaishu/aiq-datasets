# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

HEADER = ["Instrument", "Date"]


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

    def export_data(self, save_dir, index_name):
        """导出数据到文件
        Args:
            save_dir: 导出到目录
            index_name: 指数名称
        """
        # 创建目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询数据
            if index_name == "csi100":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='000903.SH'"
                )
            elif index_name == "csi300":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='000300.SH'"
                )
            elif index_name == "csi500":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='000905.SH'"
                )
            elif index_name == "csi1000":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='000852.SH'"
                )
            elif index_name == "sh300":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='000300.SH'"
                )
            elif index_name == "szzs":
                query = (
                    "SELECT DISTINCT ts_code, trade_date FROM ts_idx_index_weight "
                    "WHERE index_code='399001.SZ'"
                )
            else:
                raise ValueError("Unknown index name")

            print(query)

            cursor.execute(query)

            with open("%s/%s.csv" % (save_dir, index_name), "w", newline="") as csvfile:
                writer = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                writer.writerow(HEADER)

                for row in cursor:
                    list_row = list(row)
                    t_date = list_row[1]
                    list_row[1] = t_date[0:4] + "-" + t_date[4:6] + "-" + t_date[6:8]
                    writer.writerow(list_row)


if __name__ == "__main__":
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description="获取指数里的每支股票")
    parser.add_argument(
        "-save_dir", required=True, type=str, help="Directory of the files"
    )
    parser.add_argument("-index_name", type=str, help="index name")
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

    # 解析命令行中的参数
    print(
        "Begin export data, save_dir: %s, index_name: %s"
        % (args.save_dir, args.index_name)
    )

    export = ExportCodeData(args)
    export.export_data(os.path.join(args.save_dir, "instruments"), args.index_name)

    print("End export data")
