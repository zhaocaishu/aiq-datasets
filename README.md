# AIQ-Datasets

A 股数据准备工具，用于从 MySQL 导出股票行情、交易日历和指数股票池，处理分钟数据，并检查 CSV 数据质量。

股票基础数据和行情由已有数据库提供，行业表由外部整理导入，指数特征来自同花顺 iFind。数据表及来源见 [数据依赖](docs/基本数据.md)。

## 安装

在项目根目录安装本地包和脚本依赖，后续命令也在项目根目录执行：

```bash
python3 -m pip install -e . pandas mysql-connector-python sqlalchemy pymysql requests tqdm
```

处理 Excel 文件时，另行安装 `openpyxl` 和 `xlrd`。当前 `setup.py` 未声明脚本依赖，因此需要显式安装。

## 脚本入口

| 功能 | 脚本 | 输入与输出 |
| --- | --- | --- |
| 股票日线导出 | [collect_stock_features.py](tools/collection/collect_stock_features.py) | MySQL → 每只股票一个 CSV，包含行情、复权因子及行业标签 |
| 交易日历导出 | [collect_stock_calendar.py](tools/collection/collect_stock_calendar.py) | MySQL → 开市日期 CSV |
| 指数股票池导出 | [collect_index_stocks.py](tools/collection/collect_index_stocks.py) | 每日指数成分、历史股票名称 → 排除当日 ST 股票的股票池 CSV |
| 指数特征导出 | [collect_index_features.py](tools/collection/collect_index_features.py) | 同花顺数据表 → 指数特征 CSV |
| 同花顺指数抓取 | [fetch_index_features_from_ths.py](tools/crawler/fetch_index_features_from_ths.py) | iFind API → 指数原始 CSV；需要访问 Token |
| 分钟数据处理 | [process_stock_intraday_features.py](tools/processing/process_stock_intraday_features.py) | 原始分钟 CSV、日线 CSV → 补充复权因子的分钟 CSV |
| 字段缺失率统计 | [check_stock_nan_percentage.py](tools/validation/check_stock_nan_percentage.py) | 同一目录下的 CSV → 各字段空值比例 |
| 缺失交易日检查 | [check_stock_missing_data.py](tools/validation/check_stock_missing_data.py) | 交易日历、停复牌、股票 CSV → 缺失日期报告；默认路径和日期筛选逻辑待修正 |
| 文件与数据库互导 | [export_db_to_file.py](tools/operations/export_db_to_file.py)、[import_file_to_db.py](tools/operations/import_file_to_db.py) | 使用前需调整脚本中的 `DB_URI`；导入入口当前固定写入同花顺指数日线表 |
| 历史成分加工草稿 | [process_index_constituents.py](tools/processing/process_index_constituents.py) | 成分详情、进出记录、交易日历 → 每日成分；使用前需修正历史还原逻辑和本机文件路径 |

除历史成分加工草稿外，脚本均可使用 `--help` 查看参数，例如：

```bash
python3 -m tools.collection.collect_stock_features --help
```

## 常用操作

导出脚本连接的数据库名固定为 `stock_info`，需要先准备相应表。以下示例中的账号和密码应替换为实际连接信息。

导出交易日历和股票日线：

```bash
python3 -m tools.collection.collect_stock_calendar \
  --host 127.0.0.1 --user YOUR_USER --passwd YOUR_PASSWORD \
  --save_dir ./data

python3 -m tools.collection.collect_stock_features \
  --host 127.0.0.1 --user YOUR_USER --passwd YOUR_PASSWORD \
  --save_dir ./data --freq daily
```

股票导出范围为当前正常上市的主板、创业板和科创板股票，排除当前名称含 ST 的股票，并要求存在匹配的行业记录。

统计导出文件的字段缺失率：

```bash
python3 -m tools.validation.check_stock_nan_percentage \
  --input_dir ./data/features/daily
```

为已有 5 分钟数据补充复权因子，源目录与输出目录应分开：

```bash
python3 -m tools.processing.process_stock_intraday_features \
  --src_minute_dir ./raw/5min \
  --src_daily_dir ./data/features/daily \
  --dst_dir ./data/features/5min
```

分钟源文件按股票代码命名，例如 `000001.SZ.csv`，包含 `trade_time, open, close, high, low, vol, amount`；日线文件需包含 `Date, Adj_factor`。缺少对应日线日期的分钟记录会在合并时被过滤。

## 输出目录

日历、股票和指数导出脚本使用同一个 `--save_dir` 时，分别写入以下位置：

```text
data/
├── calendars/trade_days.csv
├── features/daily/<股票代码>.csv
├── features/index/daily/<指数代码>.csv
└── universes/<指数代码>.csv
```

## 当前使用限制

- 股票和指数行情导出目前只实现日线查询，使用 `--freq daily`。分钟脚本处理的是已有文件。
- 日线导出以行情与辅助表的内连接结果为准，辅助表缺少记录时会丢失对应日期；导出文件不代表完整历史行情。
- 缺失交易日检查默认读取 `calendars/day.csv` 和 `features/*.csv`，与上述导出布局不同，且停牌日期筛选、区间首尾缺失检查仍需修正。
- 指数特征导出的账号参数目前为 `-user`，指数列表固定在脚本中。
