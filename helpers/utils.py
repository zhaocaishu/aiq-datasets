from typing import Dict, Tuple


def fetch_listed_stocks(connection) -> Dict[str, Tuple[str, str, str]]:
    """
    从数据库获取当前正常上市的主板、中小板、创业板、科创板股票信息，
    包含股票代码、所属板块、所属一级行业、所属二级行业和上市日期。

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
        WITH 
        -- 步骤1: 为每个交易所生成连续的交易日序号
        calendar_with_seq AS (
            SELECT 
                exchange, 
                cal_date, 
                ROW_NUMBER() OVER (PARTITION BY exchange ORDER BY cal_date) AS trade_seq
            FROM ts_basic_trade_cal
            WHERE is_open = '1'  -- 只考虑交易日
        ),
        -- 步骤2: 关联停盘记录并添加交易日序号
        suspended_stocks AS (
            SELECT 
                s.ts_code,
                s.trade_date,
                SUBSTRING_INDEX(s.ts_code, '.', -1) AS exchange_suffix,  -- 提取股票后缀(SH/SZ/BJ)
                CASE 
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'SH' THEN 'SSE'
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'SZ' THEN 'SZSE'
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'BJ' THEN 'BSE'
                END AS exchange,  -- 映射为交易所代码
                c.trade_seq
            FROM ts_quotation_suspend_d s
            JOIN calendar_with_seq c ON 
                c.cal_date = s.trade_date AND
                c.exchange = CASE 
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'SH' THEN 'SSE'
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'SZ' THEN 'SZSE'
                    WHEN SUBSTRING_INDEX(s.ts_code, '.', -1) = 'BJ' THEN 'BSE'
                END
            WHERE s.suspend_type = 'S'  -- 只处理停盘记录
        ),
        -- 步骤3: 标记连续停盘的断点
        group_markers AS (
            SELECT 
                *,
                CASE 
                    WHEN trade_seq - LAG(trade_seq, 1) OVER (PARTITION BY ts_code ORDER BY trade_date) = 1 
                    THEN 0 
                    ELSE 1 
                END AS is_new_group
            FROM suspended_stocks
        ),
        -- 步骤4: 计算连续停盘的分组ID
        group_ids AS (
            SELECT 
                *,
                SUM(is_new_group) OVER (PARTITION BY ts_code ORDER BY trade_date) AS group_id
            FROM group_markers
        ),
        -- 步骤5: 计算每组连续停盘的天数及日期范围
        stock_suspend_days AS (
            SELECT 
                ts_code,
                MIN(trade_date) AS start_date,  -- 连续停盘起始日
                MAX(trade_date) AS end_date,    -- 连续停盘结束日
                COUNT(*) AS suspend_days        -- 连续停盘天数
            FROM group_ids
            GROUP BY ts_code, group_id
            ORDER BY ts_code, start_date
        )
        -- 步骤6: 去除ST/*ST、连续停盘天数大于90天的股票
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
        LEFT JOIN ts_basic_namechange AS nc
        ON basic.ts_code = nc.ts_code
        AND nc.change_reason IN ('ST', '*ST')
        LEFT JOIN (
            SELECT ts_code,
                   MAX(suspend_days) AS suspend_days
            FROM stock_suspend_days
            GROUP BY ts_code
        ) suspend
        ON basic.ts_code = suspend.ts_code
        WHERE basic.market IN ('主板', '创业板', '科创板')
        AND basic.list_status = 'L'
        AND nc.ts_code IS NULL
        AND COALESCE(suspend.suspend_days, 0) <= 90
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        for ts_code, market, industry_l1, industry_l2, list_date in cursor.fetchall():
            stocks[ts_code] = (market, industry_l1, industry_l2, list_date)

    print(f"合计 {len(stocks)} 个上市股票代码")
    return stocks
