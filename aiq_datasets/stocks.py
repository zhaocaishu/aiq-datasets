import pandas as pd
from aiq_datasets.db_query import DBQuery

from typing import Union

class Stocks(object):
    def __init__(self):
        self.db_query = DBQuery()

    def get_stocks(self) -> pd.DataFrame:
        """
        获取所有股票基本信息
        """
        sql = """
            SELECT * FROM ts_basic_stock_metadata
        """
        return self.db_query.query(sql)
    
    def get_stock(self, stock_code: str) -> Union[dict, None]:
        """
        获取股票基本信息
        """
        if not stock_code:
            raise ValueError("stock_code is required")
        
        sql = """
            SELECT * FROM ts_basic_stock_metadata
            WHERE ts_code = %s
        """
        df = self.db_query.query(sql)
        if df.empty:
            return None
        return df.to_dict(orient="records")[0]
    