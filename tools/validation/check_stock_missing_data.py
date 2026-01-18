"""
Data Quality Checker for A-Share Stock Data

功能：
1. 校验个股数据是否与交易日历对齐
2. 结合停复牌信息，停牌日不计为缺失
3. 输出单股票明细 + 全市场汇总指标

适用市场：中国 A 股
"""

import os
import glob
import argparse
from typing import Dict, Set, List

import pandas as pd


class DataQualityChecker:
    """
    A 股个股数据质量校验器
    """

    def __init__(
        self,
        data_dir: str,
        exchange: str = "SSE",
        calendar_file: str = "calendars/day.csv",
        suspend_file: str = "suspend/suspend.csv",
        feature_dir: str = "features",
        start_date: str = None,
        end_date: str = None,
    ):
        self.data_dir = data_dir
        self.exchange = exchange

        self.calendar_path = os.path.join(data_dir, calendar_file)
        self.suspend_path = os.path.join(data_dir, suspend_file)
        self.feature_dir = os.path.join(data_dir, feature_dir)

        # 校验区间（全局）
        self.start_date = start_date
        self.end_date = end_date

        self.calendar_df = self._load_trading_calendar()
        self.suspend_df = self._load_suspend_data()

    def _load_trading_calendar(self) -> pd.DataFrame:
        if not os.path.exists(self.calendar_path):
            raise FileNotFoundError(f"交易日历不存在: {self.calendar_path}")

        df = pd.read_csv(self.calendar_path, dtype={"Date": str})
        df = df[df["Exchange"] == self.exchange]
        return df

    def get_trading_days(self, start_date: str, end_date: str) -> Set[str]:
        df = self.calendar_df
        df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]
        return set(df["Date"].tolist())

    def _load_suspend_data(self) -> pd.DataFrame:
        if not os.path.exists(self.suspend_path):
            return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type"])

        df = pd.read_csv(self.suspend_path, dtype={"trade_date": str})
        df = df[df["suspend_type"] == "S"]
        return df

    def get_suspend_days(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Set[str]:
        df = self.suspend_df
        df = df[df["ts_code"] == ts_code]
        df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]

        trade_dates = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.strftime(
            "%Y-%m-%d"
        )

        return set(trade_dates.tolist())

    def check_single_stock(self, stock_file: str) -> Dict:
        df = pd.read_csv(stock_file, dtype={"Date": str})
        ts_code = os.path.basename(stock_file).replace(".csv", "")

        if "Date" not in df.columns:
            raise ValueError(f"{ts_code} 数据中缺少 Date 字段")

        data_dates = set(df["Date"].tolist())

        data_start = df["Date"].min()
        data_end = df["Date"].max()

        # 实际校验区间（取交集）
        start_date = max(data_start, self.start_date) if self.start_date else data_start
        end_date = min(data_end, self.end_date) if self.end_date else data_end

        if start_date > end_date:
            return {
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
                "trading_days": 0,
                "suspend_days": 0,
                "data_days": 0,
                "missing_cnt": 0,
                "missing_ratio": 0.0,
                "missing_days": [],
            }

        trading_days = self.get_trading_days(start_date, end_date)
        suspend_days = self.get_suspend_days(ts_code, start_date, end_date)

        # 仅保留区间内的数据日期
        data_dates = {d for d in data_dates if start_date <= d <= end_date}

        covered_days = data_dates | suspend_days
        missing_days = trading_days - covered_days

        return {
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
            "trading_days": len(trading_days),
            "suspend_days": len(suspend_days),
            "data_days": len(data_dates),
            "missing_cnt": len(missing_days),
            "missing_ratio": (
                len(missing_days) / len(trading_days) if trading_days else 0.0
            ),
            "missing_days": sorted(missing_days),
        }

    def check_all_stocks(self) -> List[Dict]:
        stock_files = glob.glob(os.path.join(self.feature_dir, "*.csv"))
        results = []

        for stock_file in stock_files:
            try:
                result = self.check_single_stock(stock_file)
                results.append(result)
            except Exception as e:
                print(f"[ERROR] {stock_file}: {e}")

        return results

    @staticmethod
    def summary(results: List[Dict]) -> Dict:
        total_stocks = len(results)
        stocks_with_missing = sum(r["missing_cnt"] > 0 for r in results)

        total_missing_days = sum(r["missing_cnt"] for r in results)
        total_trading_days = sum(r["trading_days"] for r in results)

        return {
            "total_stocks": total_stocks,
            "stocks_with_missing_ratio": (
                stocks_with_missing / total_stocks if total_stocks else 0.0
            ),
            "missing_day_ratio": (
                total_missing_days / total_trading_days if total_trading_days else 0.0
            ),
        }


def main():
    parser = argparse.ArgumentParser(
        description="A股个股数据质量校验（交易日 + 停复牌）"
    )
    parser.add_argument("--data_dir", required=True, type=str, help="数据根目录")
    parser.add_argument(
        "--start_date", type=str, default=None, help="开始日期（YYYY-MM-DD）"
    )
    parser.add_argument(
        "--end_date", type=str, default=None, help="结束日期（YYYY-MM-DD）"
    )
    parser.add_argument("--topk", default=10, type=int, help="输出缺失率最高的股票数量")
    args = parser.parse_args()

    checker = DataQualityChecker(
        data_dir=args.data_dir, start_date=args.start_date, end_date=args.end_date
    )

    results = checker.check_all_stocks()
    summary = checker.summary(results)

    print("\n====== 数据质量汇总 ======")
    for k, v in summary.items():
        print(f"{k}: {v}")

    print("\n====== 缺失率最高的股票 ======")
    bad_cases = sorted(results, key=lambda x: x["missing_ratio"], reverse=True)[
        : args.topk
    ]

    for r in bad_cases:
        print(
            f"{r['ts_code']} | "
            f"missing_ratio={r['missing_ratio']:.4f} | "
            f"missing_cnt={r['missing_cnt']} | "
            f"missing_days={r['missing_days'][:10]}"
        )


if __name__ == "__main__":
    main()
