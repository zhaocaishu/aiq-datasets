import argparse
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import pandas as pd


def process_file(filepath: Path):
    """
    处理单个CSV文件：重命名列、添加Instrument、重新排序并覆盖保存
    """
    try:
        # 1. 提取文件名作为 Instrument (例如 000990.SZ.csv -> 000990.SZ)
        instrument_id = filepath.stem

        # 2. 读取数据
        df = pd.read_csv(filepath)

        # 3. 定义列名映射
        # 原始: trade_time, open, high, low, close, vol, amount
        column_mapping = {
            "trade_time": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "vol": "Volume",
            "amount": "AMount",
        }

        # 4. 执行重命名
        df.rename(columns=column_mapping, inplace=True)

        # 5. 插入 Instrument 列
        df["Instrument"] = instrument_id

        # 6. 按照指定顺序排列列名
        target_columns = [
            "Instrument",
            "Date",
            "Open",
            "Close",
            "High",
            "Low",
            "Volume",
            "AMount",
        ]

        # 仅保留需要的列并排序 (防止原文件中存在多余列)
        df = df[target_columns]

        # 7. 覆盖保存原始文件
        df.to_csv(filepath, index=False)
        return True
    except Exception as e:
        return f"Error processing {filepath.name}: {e}"


def main(data_dir: str, workers: int = 8):
    data_path = Path(data_dir)
    # 获取所有 csv 文件列表
    files = list(data_path.glob("*.csv"))

    if not files:
        print(f"❌ 在目录 {data_dir} 中未找到 CSV 文件。")
        return

    print(f"🚀 开始处理 {len(files)} 个文件，使用 {workers} 个进程...")

    # 使用进程池并行处理
    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # chunksize=10 适合这种轻量级的文件读写任务，减少进程间通信开销
        for res in tqdm(
            executor.map(process_file, files, chunksize=10),
            total=len(files),
            desc="Formatting CSVs",
        ):
            if res is not True:
                results.append(res)

    # 错误汇总打印
    if results:
        print("\n⚠️ 部分文件处理出错:")
        for error in results:
            print(error)

    print(f"\n✅ 处理完成。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量格式化股票CSV文件列名")
    parser.add_argument(
        "--data_dir", type=str, required=True, help="包含CSV文件的目录路径"
    )
    parser.add_argument("--workers", type=int, default=8, help="并行进程数量，默认 8")

    args = parser.parse_args()

    main(args.data_dir, args.workers)
