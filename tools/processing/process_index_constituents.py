import pandas as pd


def get_trading_calendar(start_date, end_date):
    """生成指定日期范围内的理论交易日历"""
    df = pd.read_csv("/Users/darren/Downloads/day.csv")
    df = df[df["Trade_date"] >= start_date]
    df = df[df["Trade_date"] <= end_date]

    trade_dates = df[df["Exchange"] == "SSE"]["Trade_date"].tolist()
    return sorted(set(trade_dates))


if __name__ == "__main__":
    index_code = "000985.SH"
    df_detail = pd.read_excel(
        f"/Users/darren/Downloads/成分详情({index_code}).xls",
        usecols=["证券代码"],
    )

    df_history = pd.read_excel(
        f"/Users/darren/Downloads/成分进出记录({index_code}).xls",
        usecols=["证券代码", "调整类型", "调整日期"],
    )

    # 时间范围内的全部交易日期
    min_date = df_history["调整日期"].min()
    max_date = df_history["调整日期"].max()
    trading_dates = get_trading_calendar(min_date, max_date)

    # 按日期分组的调整映射，方便快速查找
    history_by_date = {date: group for date, group in df_history.groupby("调整日期")}

    # 初始化当前成分股集合
    current_set = set(df_detail["证券代码"])

    # 遍历每个交易日，先应用当天（如果有）的调整，然后写入快照
    records = []
    for trade_date in trading_dates:
        # 如果这一天有调整，就先全部应用
        if trade_date in history_by_date:
            for _, row in history_by_date[trade_date].iterrows():
                code = row["证券代码"]
                if row["调整类型"] == "纳入":
                    current_set.add(code)
                elif row["调整类型"] == "剔除":
                    current_set.discard(code)
                else:
                    raise ValueError(f"不支持的调整类型：{row['调整类型']}")

        # 将当日（调整后）的全量成分股记录下来
        for code in sorted(current_set):
            records.append(
                {
                    "指数代码": index_code,
                    "交易日期": trade_date,
                    "证券代码": code,
                }
            )

    # 构造最终的快照 DataFrame
    df_snapshots = pd.DataFrame(records)

    # 将结果保存为csv
    df_snapshots.to_csv(
        f"/Users/darren/Downloads/成分股({index_code})_按调整日期.csv",
        index=False,
    )
