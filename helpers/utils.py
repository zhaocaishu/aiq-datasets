from typing import Dict, Tuple


def fetch_listed_stocks(connection) -> Dict[str, Tuple[str, str, str]]:
    """
    从数据库中筛选当前状态为“正常上市”的A股股票（覆盖主板、中小板、创业板、科创板），并提取其股票代码、所属板块、一级行业、二级行业及上市日期等信息。

    Args:
        connection: 数据库连接对象

    Returns:
        dict: 字典格式的股票信息，键为股票代码 (ts_code)，
              值为包含板块、一级行业、二级行业和上市日期的元组 (market, industry_l1, industry_l2, list_date)

    Example:
        {
            '002678.SZ': ('主板', '轻工制造', '文娱用品', '2012-05-30'),
            '688798.SH': ('科创板', '电子', '半导体', '2021-08-16')
        }
    """
    stocks = {}

    query = """
        SELECT basic.ts_code,
               basic.market,
               TRIM(industry.l1_name) AS l1_name,
               TRIM(REPLACE(industry.l2_name, 'Ⅱ', '')) AS l2_name,
               DATE_FORMAT(basic.list_date, '%Y-%m-%d') AS list_date
        FROM ts_basic_stock_metadata basic
        JOIN (
            SELECT ts_code,
                   l1_name,
                   l2_name
            FROM ts_idx_industry_cons
        ) industry
        ON basic.ts_code = industry.ts_code
        WHERE basic.market IN ('主板', '创业板', '科创板')
        AND basic.list_status = 'L'
        AND basic.name not LIKE '%ST%'
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        for ts_code, market, industry_l1, industry_l2, list_date in cursor.fetchall():
            stocks[ts_code] = (market, industry_l1, industry_l2, list_date)

    print(f"合计 {len(stocks)} 个上市股票代码")
    return stocks
