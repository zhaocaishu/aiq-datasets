import sys
import pandas as pd
from tqdm import tqdm

sys.path.append("/Applications/Wind API.app/Contents/python")

from WindPy import w

w.start()

TARGET_COLS = "mfd_buyamt_d,mfd_sellamt_d,mfd_buyvol_d,mfd_sellvol_d,mfd_netbuyamt,mfd_netbuyvol,mfd_buyord,mfd_sellord,mfd_buyamt_a,mfd_sellamt_a,mfd_buyvol_a,mfd_sellvol_a,mfd_buyamt_at,mfd_sellamt_at,mfd_buyvol_at,mfd_sellvol_at,mfd_netbuyamt_a,mfd_netbuyvol_a,mf_amt,mf_amt_open,mf_amt_close,mf_amt_ratio,mfd_inflowrate_open_a,mfd_inflowrate_close_a,mfd_inflowproportion_a,mfd_inflowproportion_open_a,mfd_inflowproportion_close_a,mf_vol,mfd_inflowvolume_open_a,mfd_inflowvolume_close_a,mfd_volinflowrate_a,mfd_volinflowrate_open_a,mfd_volinflowrate_close_a,mf_vol_ratio,mfd_volinflowproportion_open_a,mfd_volinflowproportion_close_a,mfd_inflow_m,mfd_inflow_open_m,mfd_inflow_close_m,mfd_inflowrate_m,mfd_inflowrate_open_m,mfd_inflowrate_close_m,mfd_inflowproportion_m,mfd_inflowproportion_open_m,mfd_inflowproportion_close_m,mfd_buyvol_m,mfd_buyvol_open_m,mfd_buyvol_close_m,mfd_volinflowrate_m,mfd_volinflowrate_open_m,mfd_volinflowrate_close_m,mfd_volinflowproportion_m,mfd_volinflowproportion_open_m,mfd_volinflowproportion_close_m,mf_netinflow"


def collect_stock_data(code: str, start_date: str, end_date: str):
    data = w.wsd(
        code, TARGET_COLS, start_date, end_date, "unit=1;traderType=1;PriceAdj=B"
    )
    return data


def format_yyyymmdd(date_int):
    """将 int 类型的 yyyymmdd 转换为 yyyy-mm-dd 字符串"""
    if pd.isna(date_int):
        return None
    date_str = str(int(date_int))
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"


if __name__ == "__main__":
    output_path = "/Users/darren/Downloads/stock_moneyflow.csv"
    df = pd.read_csv("/Users/darren/Downloads/ts_basic_stock_metadata.csv")

    result_df = pd.DataFrame()
    batch_size = 5

    # 写入表头
    pd.DataFrame(columns=["ts_code", "ts_date"] + TARGET_COLS.split(",")).to_csv(
        output_path, index=False
    )

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing Stocks"):
        code = row["ts_code"]
        list_date = format_yyyymmdd(row["list_date"])
        delist_date = (
            format_yyyymmdd(row["delist_date"]) if "delist_date" in row else None
        )

        start_date = max(list_date, "2008-01-02")
        end_date = delist_date if pd.notna(delist_date) else "2025-08-03"

        data = collect_stock_data(code, start_date, end_date)

        if data.ErrorCode != 0:
            print(f"Error fetching {code}: {data.ErrorCode}")
        else:
            data_content = list(zip(*data.Data))
            temp_df = pd.DataFrame(data_content, columns=TARGET_COLS.split(","))
            temp_df["ts_code"] = code
            temp_df["ts_date"] = [d.strftime("%Y%m%d") for d in data.Times]
            temp_df = temp_df[["ts_code", "ts_date"] + TARGET_COLS.split(",")]

            result_df = pd.concat([result_df, temp_df], ignore_index=True)

        # 每 batch_size 个股票写入一次
        if (index + 1) % batch_size == 0:
            result_df.to_csv(output_path, mode="a", header=False, index=False)
            result_df = pd.DataFrame()  # 清空缓存

    # 写入剩余未满 batch 的数据
    if not result_df.empty:
        result_df.to_csv(output_path, mode="a", header=False, index=False)

    print(f"股票状态数据已完成抓取并保存至{output_path}")
