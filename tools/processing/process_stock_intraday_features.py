import argparse
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import pandas as pd


def process_file(filepath: Path, daily_dir: Path, dst_dir: Path):
    """
    处理单个CSV文件（5min数据）：
      - 若日线缺失 -> SKIP
      - 否则标准化 + 合并复权因子 + 输出到 dst_dir
    """
    try:
        instrument_id = filepath.stem
        daily_file = daily_dir / f"{instrument_id}.csv"

        if not daily_file.exists():
            return "SKIP"

        # 读取数据
        df_min = pd.read_csv(filepath)
        df_day = pd.read_csv(daily_file, usecols=["Date", "Adj_factor"])

        # 时间处理（保持 datetime64）
        df_min["Date"] = pd.to_datetime(df_min["trade_time"]).dt.normalize()
        df_day["Date"] = pd.to_datetime(df_day["Date"]).dt.normalize()

        # 重命名
        df_min.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "vol": "Volume",
                "amount": "Amount",
            },
            inplace=True,
        )

        # merge优化：用map代替merge
        adj_map = df_day.set_index("Date")["Adj_factor"]
        df_min["Adj_factor"] = df_min["Date"].map(adj_map)

        # 插入Instrument
        df_min.insert(0, "Instrument", instrument_id)

        # 列排序
        df_min = df_min[
            [
                "Instrument",
                "Date",
                "Adj_factor",
                "Open",
                "Close",
                "High",
                "Low",
                "Volume",
                "Amount",
            ]
        ]

        # 输出路径
        out_path = dst_dir / filepath.name
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df_min.to_csv(out_path, index=False)

        return True

    except Exception as e:
        return f"{filepath.name}: {e}"


def main(minute_dir: str, daily_dir: str, dst_dir: str, workers: int = 8):
    minute_dir = Path(minute_dir)
    daily_dir = Path(daily_dir)
    dst_dir = Path(dst_dir)

    if not minute_dir.exists():
        print(f"❌ 分钟目录不存在: {minute_dir}")
        return

    if not daily_dir.exists():
        print(f"❌ 日线目录不存在: {daily_dir}")
        return

    files = list(minute_dir.glob("*.csv"))

    if not files:
        print(f"❌ 没有找到CSV文件: {minute_dir}")
        return

    print(f"🚀 处理 {len(files)} 个文件 | workers={workers}")

    process_func = partial(
        process_file,
        daily_dir=daily_dir,
        dst_dir=dst_dir,
    )

    success, skipped, errors = 0, 0, []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for res in tqdm(
            executor.map(process_func, files, chunksize=20),
            total=len(files),
            desc="Processing",
        ):
            if res is True:
                success += 1
            elif res == "SKIP":
                skipped += 1
            else:
                errors.append(res)

    print("\n📊 统计结果:")
    print(f"  ✅ 成功: {success}")
    print(f"  ⏭️ 跳过: {skipped}")
    print(f"  ❌ 失败: {len(errors)}")

    if errors:
        print("\n⚠️ 错误详情:")
        for e in errors[:20]:  # 避免刷屏
            print("  ", e)

    print("\n✅ 完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="分钟数据补充复权因子")

    parser.add_argument("--src_minute_dir", required=True)
    parser.add_argument("--src_daily_dir", required=True)
    parser.add_argument("--dst_dir", required=True)
    parser.add_argument("--workers", type=int, default=8)

    args = parser.parse_args()

    main(
        minute_dir=args.src_minute_dir,
        daily_dir=args.src_daily_dir,
        dst_dir=args.dst_dir,
        workers=args.workers,
    )
