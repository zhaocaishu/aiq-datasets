"""
验证个股数据日期与交易日历对齐的脚本
适用于中国A股市场
"""

import os
import glob
import argparse
import pandas as pd


def get_trading_calendar(data_dir, start_date, end_date):
    """生成指定日期范围内的理论交易日历"""
    df = pd.read_csv(os.path.join(data_dir, "calendars", "day.csv"))
    df = df[df["Trade_date"] >= start_date]
    df = df[df["Trade_date"] <= end_date]

    trade_dates = df[df["Exchange"] == "SSE"]["Trade_date"].tolist()
    return set(trade_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="验证个股数据日期与交易日历对齐")
    parser.add_argument(
        "-save_dir", required=True, type=str, help="Directory of the files"
    )
    args = parser.parse_args()

    miss_stock_cnt = 0
    miss_trading_days = 0
    total_trading_days = 0
    stock_files = glob.glob(
        os.path.join(os.path.join(args.save_dir, "features"), "*.csv")
    )
    for stock_file in stock_files:
        # 加载个股数据
        df = pd.read_csv(stock_file)
        data_dates = set(df["Date"].tolist())

        # 生成理论交易日历
        start_date = df["Date"].min()
        end_date = df["Date"].max()
        trading_days = get_trading_calendar(args.save_dir, start_date, end_date)

        # 找出差异
        missing_dates = trading_days - data_dates  # 数据缺失的交易日
        miss_stock_cnt += len(missing_dates) > 0
        miss_trading_days += len(missing_dates)
        total_trading_days += len(trading_days)

        extra_dates = data_dates - trading_days  # 数据中多余的非交易日
        assert len(extra_dates) == 0

        # 输出结果
        print("------------------------------------\n")
        print(f"股票名称: {os.path.basename(stock_file)}")
        print(f"日期范围: {start_date} 至 {end_date}")
        print(f"理论交易日总数: {len(trading_days)}")
        print(f"数据中包含日期总数: {len(data_dates)}")
        print("\n差异检测结果:")
        print(f"数据缺失的交易日数量: {len(missing_dates)}")
        print(f"数据缺失的交易日量: {sorted(missing_dates)}")
        print(f"数据中多余的非交易日数量: {len(extra_dates)}")
        print("------------------------------------\n")

    print(f"数据中有缺失的股票占比: {miss_stock_cnt / len(stock_files)}")
    print(f"数据中有缺失的日期占比: {miss_trading_days / total_trading_days}")
