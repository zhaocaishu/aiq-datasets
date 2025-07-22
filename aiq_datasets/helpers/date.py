from datetime import datetime
from typing import Union

def current_date(date_format: str = "%Y%m%d") -> str:
    return datetime.now().strftime(date_format)

def max_date(date1: Union[str, None], date2: Union[str, None], *, date_format: str = "%Y%m%d") -> str:
    if date1 is None or date2 is None:
        return date1 if date1 is not None else date2
    return max(datetime.strptime(date1, date_format), datetime.strptime(date2, date_format))

def min_date(date1: Union[str, None], date2: Union[str, None], *, date_format: str = "%Y%m%d") -> str:
    if date1 is None or date2 is None:
        return date1 if date1 is not None else date2
    return min(datetime.strptime(date1, date_format), datetime.strptime(date2, date_format))