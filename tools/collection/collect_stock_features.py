# -*- coding: utf-8 -*-
import os
import argparse
import csv

import mysql.connector

from common.utils import fetch_listed_stocks
from common.industry import IndustryL1, IndustryL2

HEADER = [
    "Instrument",
    "Date",
    "Open",
    "Close",
    "High",
    "Low",
    "Pre_Close",
    "Change",
    "Pct_Chg",
    "Volume",
    "AMount",
    "Turnover_rate",
    "Turnover_rate_f",
    "Volume_ratio",
    "Pe",
    "Pe_ttm",
    "Pb",
    "Ps",
    "Ps_ttm",
    "Dv_ratio",
    "Dv_ttm",
    "Total_share",
    "Float_share",
    "Free_share",
    "Total_mv",
    "Circ_mv",
    "Adj_factor",
    "Up_limit",
    "Down_limit",
    "Mfd_inflow_vol_ratio",
    "Mfd_large_amount_ratio",
    "Mkt_class",
    "Ind_class_l1",
    "Ind_class_l2",
    "List_date",
    "Q_dt_roe",
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

        # 获取上市的全部股票代码
        stocks = fetch_listed_stocks(self.connection)

        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            for code in stocks:
                # 查询数据
                query = (
                    "SELECT daily.*, daily_basic.turnover_rate, daily_basic.turnover_rate_f, daily_basic.volume_ratio, daily_basic.pe, daily_basic.pe_ttm, "
                    "daily_basic.pb, daily_basic.ps, daily_basic.ps_ttm, daily_basic.dv_ratio, daily_basic.dv_ttm, daily_basic.total_share, daily_basic.float_share, "
                    "daily_basic.free_share, daily_basic.total_mv, daily_basic.circ_mv, factor.adj_factor, stk_limit.up_limit, stk_limit.down_limit, "
                    "((moneyflow.buy_sm_vol + moneyflow.buy_md_vol + moneyflow.buy_lg_vol + moneyflow.buy_elg_vol) - (moneyflow.sell_sm_vol + moneyflow.sell_md_vol + moneyflow.sell_lg_vol + moneyflow.sell_elg_vol)) / (moneyflow.buy_sm_vol + moneyflow.buy_md_vol + moneyflow.buy_lg_vol + moneyflow.buy_elg_vol + moneyflow.sell_sm_vol + moneyflow.sell_md_vol + moneyflow.sell_lg_vol + moneyflow.sell_elg_vol), "
                    "((moneyflow.buy_lg_amount + moneyflow.buy_elg_amount) - (moneyflow.sell_lg_amount + moneyflow.sell_elg_amount)) / (moneyflow.buy_lg_amount + moneyflow.buy_elg_amount + moneyflow.sell_lg_amount + moneyflow.sell_elg_amount), "
                    "fina.q_dt_roe "
                    "FROM ts_quotation_daily daily "
                    "JOIN ts_quotation_daily_basic daily_basic ON "
                    "daily.ts_code=daily_basic.ts_code AND "
                    "daily.trade_date=daily_basic.trade_date "
                    "JOIN ts_quotation_adj_factor factor ON "
                    "daily.ts_code=factor.ts_code AND "
                    "daily.trade_date=factor.trade_date "
                    "JOIN ts_quotation_stk_limit stk_limit ON "
                    "daily.ts_code=stk_limit.ts_code AND "
                    "daily.trade_date=stk_limit.trade_date "
                    "JOIN ts_quotation_moneyflow moneyflow ON "
                    "daily.ts_code=moneyflow.ts_code AND "
                    "daily.trade_date=moneyflow.trade_date "
                    "JOIN ts_financial_fina_indicator fina ON "
                    "daily.ts_code = fina.ts_code "
                    "AND fina.update_flag = 1 "
                    "AND fina.ann_date = ( "
                    "    SELECT MAX(f2.ann_date) "
                    "    FROM ts_financial_fina_indicator f2 "
                    "    WHERE f2.ts_code = daily.ts_code "
                    "    AND f2.update_flag = 1 "
                    "    AND f2.ann_date <= daily.trade_date "
                    ") "
                    "AND fina.end_date = ( "
                    "    SELECT MAX(f3.end_date) "
                    "    FROM ts_financial_fina_indicator f3 "
                    "    WHERE f3.ts_code = daily.ts_code "
                    "    AND f3.ann_date = fina.ann_date "
                    "    AND f3.update_flag = 1 "
                    ") "
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

                        # market class
                        market_name = stocks[code][0]
                        if market_name in ("科创板", "创业板"):
                            market_class = 0
                        elif market_name in ("主板", "中小板"):
                            market_class = 1
                        else:
                            raise ValueError(f"Unknown market name: {market_name}")
                        list_row.append(market_class)

                        # industry class
                        ind_class_l1 = getattr(IndustryL1, stocks[code][1], None)
                        ind_class_l2 = getattr(IndustryL2, stocks[code][2], None)

                        assert ind_class_l1 is not None
                        assert ind_class_l2 is not None

                        list_row.append(ind_class_l1.value)
                        list_row.append(ind_class_l2.value)

                        # list date
                        list_date = stocks[code][3]
                        list_row.append(list_date)

                        writer.writerow(list_row)

    def close(self):
        """关闭数据库连接"""
        self.connection.close()


if __name__ == "__main__":
    """主程序，解析参数，并执行相关的命令"""
    parser = argparse.ArgumentParser(description="查询并保存全部股票数据")
    parser.add_argument(
        "--save_dir", required=True, type=str, help="Directory of the saved files"
    )
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="The address of database"
    )
    parser.add_argument(
        "--user", type=str, default="zcs", help="The user name of database"
    )
    parser.add_argument(
        "--passwd", type=str, default="2025zcsdaydayup", help="The password of database"
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
