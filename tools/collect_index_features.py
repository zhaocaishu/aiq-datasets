# -*- coding: utf-8 -*-
import os
import argparse
import csv
import pandas as pd

import pymysql
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
    "UPratio",
]

QUERY_SQL = """
WITH up_stats AS (
    SELECT w.index_code,
           w.trade_date,
           AVG(CASE WHEN q.pct_chg > 0 THEN 1.0 ELSE 0.0 END) AS up_ratio
    FROM ts_idx_index_weight_daily AS w
    JOIN ts_quotation_daily AS q
    ON w.ts_code = q.ts_code
    AND w.trade_date = q.trade_date
    WHERE w.index_code = '%s'
    GROUP BY w.index_code, w.trade_date
)

-- 2. 将占比结果与指数日度行情特征合并
SELECT d.*,
       u.up_ratio AS up_ratio
FROM ts_idx_index_daily AS d
JOIN up_stats AS u
ON d.index_code = u.index_code
AND d.trade_date = u.trade_date
WHERE d.index_code = '%s'
"""


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

    def _preprocess_index_weight(self, code):
        # 1. 获取数据
        conn = pymysql.connect(host='127.0.0.1', user='zcs', password='2025zcsdaydayup', database='stock_info')
        df_weight = pd.read_sql(f"SELECT * FROM ts_idx_index_weight WHERE index_code = '%s' % code", conn)
        df_cal = pd.read_sql("SELECT cal_date FROM ts_basic_trade_cal WHERE is_open = 1", conn)
        
        # 2. 构建完整的 trade_date 序列
        calendar = df_cal['cal_date'].sort_values().drop_duplicates()
        
        # 3. 对每组 index_code 和 ts_code 做前向填充
        filled_list = []
        for (index_code, ts_code), group in df_weight.groupby(['index_code', 'ts_code']):
            group = group.sort_values('trade_date')
            group = group.set_index('trade_date')
            group = group.reindex(calendar, method='ffill')  # 前向填充
            group['index_code'] = index_code
            group['ts_code'] = ts_code
            group = group.reset_index().rename(columns={'index': 'trade_date'})
            filled_list.append(group)
        
        # 4. 合并结果
        df_filled = pd.concat(filled_list, ignore_index=True)
        df_filled = df_filled[['index_code', 'ts_code', 'trade_date', 'weight']]
        
        # 可选择写回数据库
        df_filled.to_sql('ts_idx_index_weight_daily', conn, index=False)

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
                print("Exporting %s" % code)

                # 将指数权重按日补充完整
                self._preprocess_index_weight(code)

                cursor.execute(QUERY_SQL, (code, code))

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

    def close(self):
        """关闭数据库连接"""
        self.connection.close()


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
