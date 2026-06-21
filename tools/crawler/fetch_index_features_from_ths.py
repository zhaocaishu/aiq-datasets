# -*- coding: utf-8 -*-
import argparse
import os
import time
import pandas as pd
import requests


class ThsIndexFetcher:
    """同花顺 iFind 指数特征数据增量抓取器"""

    def __init__(self, token, codes, output_file="index_features.csv"):
        self.url = "https://quantapi.51ifind.com/api/v1/date_sequence"
        self.headers = {"Content-Type": "application/json", "access_token": token}
        self.codes = codes
        self.output_file = output_file

        # 固定的指标配置
        self.indipara = [
            {"indicator": "ths_pre_close_index", "indiparams": [""]},
            {"indicator": "ths_open_price_index", "indiparams": [""]},
            {"indicator": "ths_high_price_index", "indiparams": [""]},
            {"indicator": "ths_low_index", "indiparams": [""]},
            {"indicator": "ths_close_price_index", "indiparams": [""]},
            {"indicator": "ths_chg_ratio_index", "indiparams": [""]},
            {"indicator": "ths_vol_index", "indiparams": [""]},
            {"indicator": "ths_trans_amt_index", "indiparams": [""]},
            {"indicator": "ths_turnover_ratio_index", "indiparams": [""]},
            {"indicator": "ths_free_turnover_ratio_index", "indiparams": [""]},
            {"indicator": "ths_swing_index", "indiparams": [""]},
            {"indicator": "ths_up_days_index", "indiparams": [""]},
            {"indicator": "ths_down_days_index", "indiparams": [""]},
            {"indicator": "ths_constituent_raise_number_index", "indiparams": [""]},
            {"indicator": "ths_constituent_fall_number_index", "indiparams": [""]},
            {"indicator": "ths_constituent_up_number_index", "indiparams": [""]},
            {"indicator": "ths_constituent_dl_number_index", "indiparams": [""]},
            {"indicator": "ths_constituent_chg_ratio_aa_index", "indiparams": [""]},
            {"indicator": "ths_constituent_chg_ratio_m_index", "indiparams": [""]},
            {
                "indicator": "ths_new_high_num_index",
                "indiparams": ["", "1", "250", "1"],
            },
            {
                "indicator": "ths_new_low_num_index",
                "indiparams": ["", "1", "250", "1"],
            },
            {"indicator": "ths_up_num_ratio_index", "indiparams": [""]},
            {
                "indicator": "ths_over250_avgclose_num_ratio_hb_index",
                "indiparams": [""],
            },
            {"indicator": "ths_raise_num_ratio_index", "indiparams": [""]},
        ]

    def _get_fetched_months(self):
        """读取本地文件，获取已经抓取成功的月份集合"""
        fetched_months = set()
        if os.path.exists(self.output_file):
            try:
                print("正在读取本地缓存记录...")
                existing_df = pd.read_csv(self.output_file, usecols=["time"])
                if not existing_df.empty:
                    existing_df["time"] = pd.to_datetime(existing_df["time"])
                    fetched_months = set(
                        existing_df["time"].dt.strftime("%Y-%m").unique()
                    )
                print(f"已发现 {len(fetched_months)} 个月的缓存数据。")
            except Exception as e:
                print(f"缓存读取失败，将重新开始获取: {e}")
        return fetched_months

    def _fetch_single_month(self, start_date, end_date):
        """请求单个月份的数据"""
        payload = {
            "codes": self.codes,
            "startdate": start_date,
            "enddate": end_date,
            "indipara": self.indipara,
        }
        try:
            response = requests.post(
                url=self.url, json=payload, headers=self.headers, timeout=30
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"HTTP 请求失败，状态码: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
        return None

    def run(self, start_month, end_month):
        """执行增量抓取主逻辑"""
        fetched_months = self._get_fetched_months()
        # 生成月度时间区间序列
        periods = pd.period_range(start=start_month, end=end_month, freq="M")

        print("开始同步数据...")
        for period in periods:
            month_str = str(period)

            if month_str in fetched_months:
                print(f"[{month_str}] 数据已存在缓存中，跳过。")
                continue

            start_date = period.start_time.strftime("%Y-%m-%d")
            end_date = period.end_time.strftime("%Y-%m-%d")

            print(f"正在请求 [{month_str}] ({start_date} 至 {end_date})...")
            res_data = self._fetch_single_month(start_date, end_date)

            if res_data and res_data.get("errorcode") == 0:
                df_list = []
                for table_block in res_data.get("tables", []):
                    thscode = table_block.get("thscode")
                    time_seq = table_block.get("time")
                    indicators = table_block.get("table", {})

                    if not time_seq or not indicators:
                        continue

                    temp_df = pd.DataFrame(indicators)
                    temp_df.insert(0, "time", pd.to_datetime(time_seq))
                    temp_df.insert(0, "thscode", thscode)
                    df_list.append(temp_df)

                if df_list:
                    month_df = pd.concat(df_list, ignore_index=True)
                    write_header = not os.path.exists(self.output_file)
                    month_df.to_csv(
                        self.output_file,
                        mode="a",
                        index=False,
                        header=write_header,
                    )
                    print(f"[{month_str}] 成功保存 {len(month_df)} 条数据。")
                else:
                    print(f"[{month_str}] 警告: 未返回有效表格数据。")
            elif res_data:
                print(f"[{month_str}] API 业务错误: {res_data.get('errmsg')}")

            time.sleep(1.5)  # 频率控制

    def finalize_data(self):
        """全局清洗、去重与时序对齐"""
        print("-" * 30)
        print("正在进行全局数据清洗与时序对齐...")
        if os.path.exists(self.output_file):
            final_df = pd.read_csv(self.output_file)
            final_df["time"] = pd.to_datetime(final_df["time"])

            initial_len = len(final_df)
            # 依据业务主键进行去重
            final_df.drop_duplicates(
                subset=["time", "thscode"], keep="last", inplace=True
            )

            # 按时间与代码升序排列
            final_df.sort_values(by=["time", "thscode"], inplace=True)
            final_df.reset_index(drop=True, inplace=True)

            # 覆盖写入
            final_df.to_csv(self.output_file, index=False)
            print(
                f"数据清洗完毕！最终总条数: {len(final_df)} (移除了 {initial_len - len(final_df)} 条重复记录)。"
            )
        else:
            print("未发现本地数据文件，请检查抓取日志。")


def main():
    # 命令行参数解析
    parser = argparse.ArgumentParser(
        description="同花顺 iFind 历史指数特征数据异步按月增量同步工具"
    )
    parser.add_argument(
        "--start", type=str, default="2025-09", help="开始月份 (格式: YYYY-MM)"
    )
    parser.add_argument(
        "--end", type=str, default="2025-12", help="结束月份 (格式: YYYY-MM)"
    )
    parser.add_argument(
        "--output", type=str, default="/Users/darren/Downloads/ths-data/index_features.csv", help="输出的 CSV 文件名"
    )
    parser.add_argument(
        "--codes",
        type=str,
        default="000300.SH,000905.SH,000906.SH",
        help="待抓取的指数代码，英文逗号分隔",
    )
    parser.add_argument(
        "--token",
        type=str,
        default="f46d81e03125a1300d76e69a0aabb8c9488d5e31.signs_ODYxOTMxOTE4",
        help="iFind API 访问 Token",
    )

    args = parser.parse_args()

    # 初始化抓取实例
    fetcher = ThsIndexFetcher(
        token=args.token, codes=args.codes, output_file=args.output
    )

    # 运行抓取逻辑
    fetcher.run(start_month=args.start, end_month=args.end)

    # 全局清洗对齐
    fetcher.finalize_data()


if __name__ == "__main__":
    main()