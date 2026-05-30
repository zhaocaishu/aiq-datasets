import os
import glob
import pandas as pd
import argparse


def calculate_nan_percentage(directory_path):
    """
    遍历指定目录下的所有 csv 文件，统计所有字段的 NaN 数量和占比。
    """
    # 检查目录是否存在
    if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
        print(f"错误: 目录 '{directory_path}' 不存在或不是一个有效的文件夹。")
        return

    # 匹配目录下所有的 csv 文件
    file_pattern = os.path.join(directory_path, "*.csv")
    file_list = glob.glob(file_pattern)

    if not file_list:
        print(f"在目录 '{directory_path}' 下没有找到CSV文件。")
        return

    total_rows = 0
    # 用于累加每个字段的 NaN 数量
    total_nans = pd.Series(dtype=float)

    print(f"开始处理，共发现 {len(file_list)} 个文件，请稍候...")

    for file_path in file_list:
        try:
            # 逐个读取 csv 文件
            df = pd.read_csv(file_path)

            # 累加总行数
            total_rows += len(df)

            # 统计当前文件各个字段的 NaN 数量
            nans_in_file = df.isna().sum()

            # 累加到总计数值中
            if total_nans.empty:
                total_nans = nans_in_file
            else:
                total_nans = total_nans.add(nans_in_file, fill_value=0)

        except Exception as e:
            print(f"读取文件 {os.path.basename(file_path)} 时出错: {e}")

    # 计算占比并输出结果
    if total_rows > 0:
        # 计算百分比
        nan_percentage = (total_nans / total_rows) * 100

        # 整理成 DataFrame 方便查看
        result_df = pd.DataFrame(
            {
                "NaN_Count": total_nans.astype(int),
                "NaN_Percentage(%)": nan_percentage.round(4),  # 保留4位小数
            }
        )

        # 按缺失值占比从高到低排序
        result_df = result_df.sort_values(by="NaN_Percentage(%)", ascending=False)

        print("-" * 50)
        print(f"处理完成！汇总总数据行数: {total_rows}")
        print("-" * 50)
        print(result_df.to_string())

        return result_df
    else:
        print("所有文件均无有效数据行，无法计算。")
        return None


if __name__ == "__main__":
    # 1. 初始化解析器，添加用户指定的描述
    parser = argparse.ArgumentParser(
        description="A股个股数据质量校验（交易日 + 停复牌）"
    )

    # 2. 增加输入目录参数 (必填项)
    parser.add_argument(
        "-i",
        "--input_dir",
        type=str,
        required=True,
        help="指定包含待校验A股CSV数据的输入目录路径",
    )

    # 3. 解析命令行输入的参数
    args = parser.parse_args()

    # 4. 提取参数并执行核心逻辑
    input_directory = args.input_dir
    print(f"当前输入目录: {input_directory}")

    # 调用统计函数
    calculate_nan_percentage(input_directory)
