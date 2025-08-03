import sys
import pandas as pd
from tqdm import tqdm

sys.path.append("/Applications/Wind API.app/Contents/python")

from WindPy import w

w.start()


def collect_stock_status(code: str, start_date: str, end_date: str):
    data = w.wsd(code, "vwap,riskwarning,susp_days,maxupordown", start_date, end_date)
    return data


def format_yyyymmdd(date_int):
    """将 int 类型的 yyyymmdd 转换为 yyyy-mm-dd 字符串"""
    if pd.isna(date_int):
        return None
    date_str = str(int(date_int))
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"


if __name__ == "__main__":
    output_path = "/Users/darren/Downloads/stock_status.csv"
    df = pd.read_csv("/Users/darren/Downloads/ts_basic_stock_metadata.csv")

    result_df = pd.DataFrame()
    batch_size = 5

    # 写入表头
    pd.DataFrame(
        columns=["ts_code", "ts_date", "vwap", "is_st", "suspend_days", "maxupordown"]
    ).to_csv(output_path, index=False)

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing Stocks"):
        code = row["ts_code"]
        list_date = format_yyyymmdd(row["list_date"])
        delist_date = (
            format_yyyymmdd(row["delist_date"]) if "delist_date" in row else None
        )

        start_date = max(list_date, "2008-01-02")
        end_date = delist_date if pd.notna(delist_date) else "2025-08-03"

        data = collect_stock_status(code, start_date, end_date)

        if data.ErrorCode != 0:
            print(f"Error fetching {code}: {data.ErrorCode}")
        else:
            data_content = list(zip(*data.Data))
            temp_df = pd.DataFrame(
                data_content, columns=["vwap", "is_st", "suspend_days", "maxupordown"]
            )
            temp_df["ts_code"] = code
            temp_df["ts_date"] = [d.strftime("%Y%m%d") for d in data.Times]
            temp_df = temp_df[
                ["ts_code", "ts_date", "vwap", "is_st", "suspend_days", "maxupordown"]
            ]

            result_df = pd.concat([result_df, temp_df], ignore_index=True)

        # 每 batch_size 个股票写入一次
        if (index + 1) % batch_size == 0:
            result_df.to_csv(output_path, mode="a", header=False, index=False)
            result_df = pd.DataFrame()  # 清空缓存

    # 写入剩余未满 batch 的数据
    if not result_df.empty:
        result_df.to_csv(output_path, mode="a", header=False, index=False)

    print(f"股票状态数据已完成抓取并保存至{output_path}")
