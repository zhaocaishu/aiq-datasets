import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# 1. 配置：数据目录、输出路径和窗口长度 T
DATA_DIR = Path("/Users/darren/Downloads/a股5分钟")
OUTPUT_PATH = Path("ts_quotation_intraday_daily.csv")
T_DAYS = 5  # 最近T天窗口

# 2. 单文件处理函数
def process_file(filepath: Path) -> pd.DataFrame:
    ts_code = filepath.stem
    # 读取并预处理
    df = pd.read_csv(filepath, parse_dates=["trade_time"] )
    df["trade_date"] = df["trade_time"].dt.strftime("%Y%m%d")

    # 计算每日特征
    def _daily(group: pd.DataFrame) -> pd.Series:
        total_vol = group["vol"].sum()
        tail_vol = group["vol"].iloc[-6:].sum()
        return pd.Series({
            "trade_date": group.name,
            "total_vol": total_vol,
            "tail_vol": tail_vol,
        })

    daily = (
        df
        .groupby("trade_date")
        .apply(_daily)
        .reset_index(drop=True)
        .sort_values("trade_date")
    )
    # 计算最近 T_DAYS 天的累积尾盘占比：sum(tail_vol) / sum(total_vol)
    daily["tail_sum_T"] = daily["tail_vol"].rolling(window=T_DAYS, min_periods=1).sum()
    daily["vol_sum_T"] = daily["total_vol"].rolling(window=T_DAYS, min_periods=1).sum()
    daily["tail_ratio"] = daily["tail_sum_T"] / daily["vol_sum_T"]

    daily.insert(0, "ts_code", ts_code)
    return daily[["ts_code", "trade_date", "total_vol", "tail_vol", "tail_sum_T", "vol_sum_T", "tail_ratio"]]

# 3. 主函数：并行处理并合并
if __name__ == "__main__":
    files = list(DATA_DIR.glob("*.csv"))
    results = []
    with ProcessPoolExecutor(max_workers=8) as executor:
        for df_daily in tqdm(executor.map(process_file, files), total=len(files), desc="Processing stocks"):
            results.append(df_daily)

    # 4. 合并并保存
    if results:
        pd.concat(results, ignore_index=True).to_csv(OUTPUT_PATH, index=False)
        print(f"✅ 保存至: {OUTPUT_PATH} (窗口 {T_DAYS} 天)")
    else:
        print("⚠️ 未生成任何结果，请检查输入数据。")
