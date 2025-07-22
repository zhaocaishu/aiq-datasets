import pandas as pd
import tqdm
import os
import time
import random

from aiq_datasets.stocks import Stocks
from aiq_datasets.wind_processor import WindProcessor
from aiq_datasets.config import MINIMUM_START_DATE
from aiq_datasets.helpers.date import current_date, max_date, min_date

FINANCIAL_INDICATORS = [
    "stm_issuingdate", # 定期报告实际披露日期
    "stm_issuingdate_fs", # 定期报告正报披露日期（上报给证监会的日期）
    "stm_rpt_s", # 报告起始日期
    "stm_rpt_e", # 报告结束日期
    "fa_errorcorrectiondate", # 会计差错更正披露日期
    # "fa_errorcorrectionornot", # 是否存在会计差错更正
    "qfa_eps", "qfa_deductedprofit", "qfa_operateincome", "qfa_investincome", "qfa_roe", "qfa_roe_deducted",
    "qfa_roa", "qfa_netprofitmargin", "qfa_deductedprofitmargin", "qfa_grossprofitmargin", "nptocostexpense_qfa",
    "optoebt_qfa", "qfa_gctogr", "impairtoop_qfa", "qfa_optogr", "qfa_profittogr", "qfa_saleexpensetogr",
    "qfa_adminexpensetogr", "qfa_finaexpensetogr", "qfa_rdetogr", "qfa_operateincometoebt", "qfa_investincometoebt",
    "qfa_deductedprofittoprofit", "qfa_salescashintoor", "qfa_ocftosales", "qfa_ocftoor", "ocftocf_qfa",
    "icftocf_qfa", "fcftocf_qfa", "qfa_yoygr", "qfa_cgrgr", "qfa_yoysales", "qfa_cgrsales", "qfa_yoyop",
    "qfa_cgrop", "qfa_yoyprofit", "qfa_cgrprofit", "qfa_yoynetprofit", "qfa_cgrnetprofit", "deductedprofit_yoy",
    "qfa_deductedprofit_cgr", "qfa_yoyocf", "qfa_yoyeps", "qfa_yoycf" 
]


class StockFinancialDataProcessor(WindProcessor):
    
    def process_data(self, **kwargs) -> pd.DataFrame:
        """
        从WindAPI下载股票财务数据
        """
        # 获取当前所有的股票代码
        stocks = Stocks().get_stocks()
        stocks_count = len(stocks)
        df = pd.DataFrame()
        # 基于每个股票的ts_code、上市时间，从WindAPI下载财务数据
        for index, row in tqdm.tqdm(stocks.iterrows(), total=stocks_count):
            if index > 2:
                break
            self.logger.info(f"🟡 开始获取股票: {row['ts_code']} 的财务数据，上市日期: {row['list_date']}，退市日期: {row['delist_date']}，总计{len(FINANCIAL_INDICATORS)}个财务指标...")
            (error_code, wind_data) = self.wind_client.wsd(
                codes=row['ts_code'],
                fields=FINANCIAL_INDICATORS,
                beginTime=max_date(row['list_date'], MINIMUM_START_DATE), # 数据最小开始日期(上市日期和数据最小开始日期中的最大值)
                endTime=min_date(row['delist_date'], current_date()), # 数据最大结束日期(当前日期和退市日期中的最小值)
                options="unit=1;dataType=0;Period=Q;Days=ALLDAYS;PriceAdj=B", # 单位：1，数据类型：0，周期：Q（季（报告期）），天数：ALLDAYS（日历天），价格调整：B（后复权）
                usedf=True
            )

            if error_code != 0:
                if error_code == -40520007:
                    self.logger.warning(f"🟡 股票: {row['ts_code']} 的财务数据为空,可能是因为该股票在该时间范围已退市或未上市!")
                elif error_code == -40522017:
                    self.logger.warning(f"🟡 股票: {row['ts_code']} 数据请求过频繁，已限流！")
                else:
                    self.logger.error(f"🔴 获取股票: {row['ts_code']} 的财务数据失败! 错误码: {error_code}")
                continue
            
            if wind_data is None or len(wind_data) == 0:
                self.logger.warning(f"🟡 股票: {row['ts_code']} 的财务数据为空!")
                continue
            else:  
                columns = ['ts_code', 'name'] + wind_data.columns.tolist()
                wind_data['ts_code'] = row['ts_code'] # 股票代码
                wind_data['name'] = row['name']  # 股票名称
                wind_data = wind_data[columns]
                wind_data.columns = [name.lower() for name in wind_data.columns]
                df = pd.concat([df, wind_data])
            time.sleep(random.randint(1, 3)) # 请求间隔1-3秒

        self.logger.info(f"🟢 获取股票财务数据完成! 共获取 {len(df)} 条数据")
        return df    

    def save_data_to_csv(self, data: pd.DataFrame, file_path: str):
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        data.to_csv(file_path, index=False, encoding="utf-8", mode="w")
        self.logger.info(f"🟢 股票财报数据已成功保存到 {file_path}.")

    def save_data_to_db(self, data: pd.DataFrame, table_name: str):
        pass


if __name__ == "__main__":
    processor = StockFinancialDataProcessor()
    df = processor.process_data()
    processor.save_data_to_csv(df, "data/financial_data.csv")
    processor.save_data_to_db(df, "ts_financial_indicator_q")
