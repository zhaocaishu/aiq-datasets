def get_codes(connection) -> list:
    """获取全部上市的股票"""
    codes = {}
    query = (
        "SELECT stock.ts_code, industry.industry_name_lv1, DATE_FORMAT(stock.list_date, '%Y-%m-%d') "
        "FROM ts_basic_stock_list stock JOIN ts_idx_sw_member industry ON stock.ts_code = industry.ts_code "
        "COLLATE utf8mb4_unicode_ci WHERE stock.market in ('主板', '中小板', '创业板', '科创板') "
        "AND stock.list_status = 'L'"
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        for row in cursor:
            list_row = list(row)
            codes[list_row[0]] = [list_row[1], list_row[2]]

    print("合计%d个股票" % (len(codes)))

    return codes
