import pandas as pd


if __name__ == "__main__":
    df = pd.read_csv("./ts_basic_stock_metadata.csv")
    df = df[df['list_status'] == 'L']
    
    df2 = pd.read_excel("./全部A股-行情报价.xlsx")
    code_set = set(df2["代码"].dropna().apply(lambda x: str(int(x))).tolist())
    
    # 处理ts_code：去掉后缀，去掉前导0
    df["ts_code_clean"] = df["ts_code"].str.replace(r'\.\w+', '', regex=True)  # 去掉如 .SZ
    df["ts_code_clean"] = df["ts_code_clean"].str.lstrip('0')  # 去掉左侧0

    # 判断是否在code_set中
    df["in_code_set"] = df["ts_code_clean"].isin(code_set)

    # 打印前几条结果
    print(df[~df["in_code_set"]], df[df["in_code_set"]].shape)