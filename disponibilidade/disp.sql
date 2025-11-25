-- DISPONIBILIDADE
WITH parameters AS (
    SELECT
        DATE('2025-08-01') as init_date
    ,   DATE('2025-10-31') as end_date
),

calls AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date_gmt as ts_to_date
    ,   date_format(ts, '%Y-%m-%d %H:%i:00') as minute_ts
    ,   statuscode
    ,   COUNT(1) as request_num
    FROM pcm_reports_clients
    WHERE 1=1
        AND (
            role = 'CLIENT'
            OR (role = 'SERVER' AND status = 'UNPAIRED')
        )
        AND status <> 'PAIRED_INCONSISTENT'
        AND DATE(ts_to_date_gmt) BETWEEN 
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
        AND (
            statuscode LIKE '2%'
            OR statuscode LIKE '5%'
            OR statuscode IN ('422', '408')
        )
    GROUP BY 1, 2, 3, 4, 5, 6
),

sucess AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   minute_ts
    ,   CAST(SUM(CASE WHEN statuscode LIKE '2%' OR statuscode = '422' THEN request_num ELSE 0 END) AS DOUBLE) as sucess
    ,   SUM(request_num) as total_requests
    FROM calls
    GROUP BY 1, 2, 3, 4, 5
),

disponible AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   SUM(CASE WHEN sucess / total_requests >= 0.95 THEN 1 ELSE 0 END) as minutes_disponible
    ,   COUNT(1) as total_minutes
    ,   SUM(total_requests) as volume
    FROM sucess
    GROUP BY 1, 2, 3, 4
)

SELECT
    serverorgid
,   endpoint
,   httpmethod
,   ts_to_date
,   minutes_disponible
,   total_minutes
,   volume
,   CAST(minutes_disponible AS DOUBLE) / total_minutes as disp_dia
FROM disponible
