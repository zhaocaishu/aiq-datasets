import pandas as pd
from abc import ABC, abstractmethod
from WindPy import w

import logging

logger = logging.getLogger(__name__)

class WindProcessor(ABC):
    def __init__(self):
        if not w.isconnected(): # type: ignore
            w.start() # type: ignore
        self.wind_client = w
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def process_data(self, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_data_to_csv(self, data: pd.DataFrame, file_path: str):
        pass

    @abstractmethod
    def save_data_to_db(self, data: pd.DataFrame, table_name: str):
        pass

    def __del__(self):
        if self.wind_client.isconnected(): # type: ignore
            self.wind_client.stop() # type: ignore