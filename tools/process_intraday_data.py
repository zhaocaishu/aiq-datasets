import os
import glob
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# 1. 配置——数据目录和输出路径
DATA_DIR = "/Users/darren/Downloads/a股5分钟"  # 存放各只股票 CSV 的文件夹
OUTPUT_PATH = "ts_quotation_intraday_daily.csv"  # 最终结果保存路径

# 2. 获取所有 CSV 文件列表
all_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))


def process_file(filepath):
    """
    对单个文件（单只股票）：
    - 读取 CSV
    - 按交易日分组，计算当日尾盘（最后 6 根 5 分钟线）成交量占比
    - 返回带有 ts_code、trade_date、total_vol、tail_vol、tail_ratio、vwap 的 DataFrame
    """
    ts_code = os.path.splitext(os.path.basename(filepath))[0]
    df = pd.read_csv(filepath)
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df["trade_time"].dt.strftime("%Y%m%d")

    def calc_intraday_features(group):
        total_vol = group["vol"].sum()
        tail_vol = group.sort_values("trade_time").tail(6)["vol"].sum()

        # VWAP 用典型价加权
        group["typical_price"] = (group["high"] + group["low"] + group["close"]) / 3.0
        total_vol = group["vol"].sum()
        if total_vol == 0:
            vwap_tp = None
        else:
            vwap_tp = (group["typical_price"] * group["vol"]).sum() / total_vol

        return pd.Series(
            {
                "total_vol": total_vol,
                "tail_vol": tail_vol,
                "tail_ratio": tail_vol / total_vol if total_vol > 0 else 0,
                "vwap": vwap_tp,
            }
        )

    daily = df.groupby("trade_date").apply(calc_intraday_features).reset_index()
    daily["ts_code"] = ts_code
    return daily[
        ["ts_code", "trade_date", "total_vol", "tail_vol", "tail_ratio", "vwap"]
    ]


def main():
    results = []
    # 3. 并行处理 + 进度条
    with ProcessPoolExecutor(max_workers=8) as executor:
        # 提交所有任务
        futures = {executor.submit(process_file, fp): fp for fp in all_files}
        # as_completed + tqdm 来显示进度
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing stocks"
        ):
            try:
                df_daily = future.result()
                results.append(df_daily)
            except Exception as e:
                filepath = futures[future]
                print(f"[Error] {filepath} 处理失败：{e}")

    # 4. 合并并保存
    if results:
        result_df = pd.concat(results, ignore_index=True)
        result_df.to_csv(OUTPUT_PATH, index=False)
        print(f"✅ 已将尾盘成交量占比结果保存至：{OUTPUT_PATH}")
    else:
        print("⚠️ 没有任何结果，检查数据和代码。")


if __name__ == "__main__":
    main()
