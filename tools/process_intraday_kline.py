import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# 1. 配置：数据目录和输出路径
DATA_DIR = Path("/Users/darren/Downloads/a股5分钟")
OUTPUT_PATH = Path("ts_quotation_intraday_daily.csv")

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
            "total_vol": total_vol,
            "tail_vol": tail_vol,
            "tail_ratio": tail_vol / total_vol if total_vol else 0
        })

    daily = (
        df
        .groupby("trade_date")
        .apply(_daily)
        .reset_index()
    )
    daily.insert(0, "ts_code", ts_code)
    return daily

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
        print(f"✅ 保存至: {OUTPUT_PATH}")
    else:
        print("⚠️ 未生成任何结果，请检查输入数据。")
