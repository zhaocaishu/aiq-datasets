import argparse
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from scipy.stats import skew, kurtosis


def process_file(filepath: Path) -> pd.DataFrame:
    ts_code = filepath.stem
    df = pd.read_csv(filepath, parse_dates=["trade_time"])
    df["trade_date"] = df["trade_time"].dt.strftime("%Y%m%d")

    def compute_skew_kurtosis(df):
        df["log_return"] = (df["close"] / df["close"].shift(1)).apply(
            lambda x: 0 if pd.isna(x) else np.log(x)
        )
        returns = df["log_return"].dropna()
        return skew(returns), kurtosis(returns)

    def compute_price_vol_corr(df):
        df["log_vol"] = np.log(df["vol"] + 1)
        return df["close"].corr(df["log_vol"])

    def compute_downside_ratio(df):
        df["log_return"] = (df["close"] / df["close"].shift(1)).apply(
            lambda x: 0 if pd.isna(x) else np.log(x)
        )
        returns = df["log_return"].dropna()
        neg_sq_sum = np.sum(returns[returns < 0] ** 2)
        total_sq_sum = np.sum(returns**2)
        return neg_sq_sum / (total_sq_sum + 1e-12)

    def _daily(group: pd.DataFrame) -> pd.Series:
        total_vol = group["vol"].sum()
        tail_start = pd.to_datetime("14:30:00").time()
        tail_end = pd.to_datetime("15:00:00").time()
        tail_mask = (group["trade_time"].dt.time >= tail_start) & (
            group["trade_time"].dt.time <= tail_end
        )
        tail_vol = group.loc[tail_mask, "vol"].sum()
        returns_skewness, returns_kurtosis = compute_skew_kurtosis(group.copy())
        price_vol_corr = compute_price_vol_corr(group)
        downside_ratio = compute_downside_ratio(group.copy())

        return pd.Series(
            {
                "trade_date": group.name,
                "tail_vol": tail_vol,
                "total_vol": total_vol,
                "tail_ratio": tail_vol / (total_vol + 1e-12),
                "returns_skewness": returns_skewness,
                "returns_kurtosis": returns_kurtosis,
                "price_vol_corr": price_vol_corr,
                "downside_ratio": downside_ratio,
            }
        )

    daily = (
        df.groupby("trade_date")
        .apply(_daily)
        .reset_index(drop=True)
        .sort_values("trade_date")
    )

    daily.insert(0, "ts_code", ts_code)
    return daily[
        [
            "ts_code",
            "trade_date",
            "tail_vol",
            "total_vol",
            "tail_ratio",
            "returns_skewness",
            "returns_kurtosis",
            "price_vol_corr",
            "downside_ratio",
        ]
    ]


def main(data_dir: str, output_path: str, workers: int = 8):
    data_dir = Path(data_dir)
    output_path = Path(output_path)
    files = list(data_dir.glob("*.csv"))
    results = []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for df_daily in tqdm(
            executor.map(process_file, files),
            total=len(files),
            desc="Processing stocks",
        ):
            results.append(df_daily)

    if results:
        pd.concat(results, ignore_index=True).to_csv(output_path, index=False)
        print(f"✅ 保存至: {output_path}")
    else:
        print("⚠️ 未生成任何结果，请检查输入数据。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="计算分钟级股票日特征")
    parser.add_argument(
        "--data_dir", type=str, required=True, help="输入数据目录，CSV 文件路径"
    )
    parser.add_argument(
        "--output_path", type=str, required=True, help="输出 CSV 文件路径"
    )
    parser.add_argument("--workers", type=int, default=8, help="并行进程数量，默认 8")
    args = parser.parse_args()

    main(args.data_dir, args.output_path, args.workers)
