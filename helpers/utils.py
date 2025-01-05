def get_codes(connection) -> list:
    """获取全部上市的股票"""
    codes = {}
    query = (
        "SELECT ts_code, industry, DATE_FORMAT(list_date, '%Y-%m-%d') FROM ts_basic_stock_list "
        "WHERE market in ('主板', '中小板', '创业板', '科创板') AND list_status = 'L' AND industry != 'NULL'"
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        for row in cursor:
            list_row = list(row)
            codes[list_row[0]] = [list_row[1], list_row[2]]

    print("合计%d个股票" % (len(codes)))

    return codes
