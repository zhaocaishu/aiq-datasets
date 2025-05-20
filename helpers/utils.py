from typing import Dict, Tuple


def fetch_listed_stocks(connection) -> Dict[str, Tuple[str, str]]:
    """获取交易所全部上市股票的基本信息

    从数据库获取当前正常上市的主板、中小板、创业板、科创板股票信息，
    包含股票代码、所属板块、所属行业和上市日期。

    Args:
        connection: 数据库连接对象

    Returns:
        字典格式的股票信息，键为股票代码 (ts_code)，值为包含板块、行业和上市日期的元组

    Example:
        {
            '600000.SH': ('主板', '银行', '2000-12-19'),
            '300001.SZ': ('主板', '医疗保健', '2009-10-30')
        }
    """
    stocks = {}
    query = (
        "SELECT basic.ts_code, basic.market, TRIM(REPLACE(industry.l2_name, 'Ⅱ', '')), DATE_FORMAT(basic.list_date, '%Y-%m-%d') "
        "FROM ts_basic_stock_list basic "
        "JOIN ts_idx_index_member_all industry "
        "ON basic.ts_code=industry.ts_code "
        "WHERE basic.market IN ('主板', '中小板', '创业板', '科创板') "
        "AND basic.list_status = 'L' "
        "AND industry.is_new = 'Y'"
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        for ts_code, market, industry, list_date in cursor.fetchall():
            stocks[ts_code] = (market, industry, list_date)

    print(f"合计 {len(stocks)} 个上市股票代码")
    return stocks
