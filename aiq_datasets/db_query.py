import pandas as pd
from sqlalchemy import create_engine

from aiq_datasets.config import DB_URI

import logging

logger = logging.getLogger(__name__)

class DBQuery(object):
    def __init__(self):
        self.engine = create_engine(DB_URI)
    
    def query(self, sql: str) -> pd.DataFrame:
        logger.debug(f">>>> Query SQL: {sql}")
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(sql, conn)
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise e

    def close(self):
        self.engine.dispose()