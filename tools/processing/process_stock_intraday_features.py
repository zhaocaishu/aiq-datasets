import argparse
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

# 常量，避免重复构造
TAIL_START = pd.to_datetime("14:30:00").time()
TAIL_END = pd.to_datetime("15:00:00").time()


def compute_returns_skew(returns: pd.Series):
    if returns.empty:
        return np.nan
    return returns.skew()


def compute_price_vol_corr(df: pd.DataFrame):
    if df["vol"].sum() == 0:
        return np.nan
    df["vol_ratio"] = df["vol"] / df["vol"].sum()
    return df["close"].corr(df["vol_ratio"])


def compute_downside_ratio(returns: pd.Series):
    if returns.empty:
        return np.nan
    neg_sq_sum = np.sum(returns[returns < 0] ** 2)
    total_sq_sum = np.sum(returns**2)
    return neg_sq_sum / (total_sq_sum + 1e-12)


def process_file(filepath: Path) -> pd.DataFrame:
    ts_code = filepath.stem
    df = pd.read_csv(filepath, parse_dates=["trade_time"])
    df["trade_date"] = df["trade_time"].dt.strftime("%Y%m%d")

    def _daily(group: pd.DataFrame, trade_date: str) -> pd.Series:
        total_vol = group["vol"].sum()

        # 计算尾盘成交量
        tail_mask = (group["trade_time"].dt.time >= TAIL_START) & (
            group["trade_time"].dt.time <= TAIL_END
        )
        tail_vol = group.loc[tail_mask, "vol"].sum()

        # 提前算 log_return，避免重复计算
        group = group.copy()
        group["log_return"] = (group["close"] / group["close"].shift(1)).apply(
            lambda x: 0 if pd.isna(x) else np.log(x)
        )
        returns = group["log_return"].dropna()

        returns_skewness = compute_returns_skew(returns)
        price_vol_corr = compute_price_vol_corr(group)
        downside_ratio = compute_downside_ratio(returns)

        return pd.Series(
            {
                "trade_date": trade_date,
                "tail_vol": tail_vol,
                "total_vol": total_vol,
                "tail_ratio": tail_vol / (total_vol + 1e-12),
                "returns_skewness": returns_skewness,
                "price_vol_corr": price_vol_corr,
                "downside_ratio": downside_ratio,
            }
        )

    daily = (
        df.groupby("trade_date")
        .apply(lambda g: _daily(g, g.name))
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
            executor.map(process_file, files, chunksize=1),  # ✅ 防止大内存膨胀
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
