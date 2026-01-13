-- DISPONIBILIDADE
WITH parameters AS (
    SELECT
        DATE('2025-08-16') as init_date
    ,   DATE('2025-08-31') as end_date
),

calls AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date_gmt as ts_to_date
    ,   date_format(ts, '%Y-%m-%d %H:%i:00') as minute_ts
    ,   statuscode
    ,   CASE
            WHEN (additionalinfo_authorisationflow = 'FIDO_FLOW' AND endpoint LIKE '%pix/payments%') OR endpoint LIKE '%enrollments' THEN 'JSR'
            WHEN (endpoint LIKE '%/consents%' OR endpoint LIKE '%pix/payments%') THEN 'PAGAMENTOS_IMEDIATOS'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'AUTOMATIC' THEN 'PIX_AUTOMATICO'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'SWEEPING' THEN 'TRANSF_INTEL'
            ELSE 'TRANSF_INTEL'
        END AS produto
    ,   COUNT(1) as request_num
    FROM pcm_reports_payments
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    
    UNION ALL
    
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date_gmt as ts_to_date
    ,   date_format(ts, '%Y-%m-%d %H:%i:00') as minute_ts
    ,   statuscode
    ,   'DADOS_CLIENTES' AS produto -- se clients
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

sucess AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   minute_ts
    ,   produto
    ,   CAST(SUM(CASE WHEN statuscode LIKE '2%' OR statuscode = '422' THEN request_num ELSE 0 END) AS DOUBLE) as sucess
    ,   SUM(request_num) as total_requests
    FROM calls
    GROUP BY 1, 2, 3, 4, 5, 6
),

available AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   produto
    ,   SUM(CASE WHEN sucess / total_requests >= 0.95 THEN 1 ELSE 0 END) as minutes_available
    ,   COUNT(1) as total_minutes
    ,   SUM(total_requests) as volume
    ,   SUM(sucess) as total_successful_requests
    FROM sucess
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    serverorgid
,   produto
,   endpoint
,   httpmethod
,   ts_to_date
,   volume as total_valid_requests_day
,   total_successful_requests
,   volume - total_successful_requests as total_failed_requests
,   minutes_available as total_minutes_available
,   total_minutes - minutes_available as total_minutes_unavailable
,   CAST(minutes_available AS DOUBLE) / total_minutes as percent_available_day
FROM available
