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
    ,   statuscode,
    -- CASE
    --     WHEN additionalinfo_authorisationflow = 'FIDO_FLOW' THEN 'JSR'
    --     WHEN additionalinfo_authorisationflow = 'HYBRID_FLOW' AND endpoint LIKE '%pix/payments%' THEN 'PAGAMENTOS_IMEDIATOS'
    --     WHEN additionalinfo_authorisationflow = 'HYBRID_FLOW' AND endpoint LIKE '%pix/recurring-payments%' AND additionalinfo_paymenttype = 'AUTOMATIC' THEN 'PIX_AUTOMATICO'
    --     WHEN additionalinfo_authorisationflow = 'HYBRID_FLOW' AND endpoint LIKE '%pix/recurring-payments%' AND additionalinfo_paymenttype = 'SWEEPING' THEN 'TRANSF_INTEL'
    --     ELSE 'NAO_INFORMADO'
    -- END AS produto -- se payments
    'DADOS_CLIENTES' AS produto -- se clients
    -- NULL AS produto -- se security
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

disponible AS (
    SELECT
        serverorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   produto
    ,   SUM(CASE WHEN sucess / total_requests >= 0.95 THEN 1 ELSE 0 END) as minutes_disponible
    ,   COUNT(1) as total_minutes
    ,   SUM(total_requests) as volume
    FROM sucess
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    serverorgid
,   endpoint
,   httpmethod
,   ts_to_date
,   produto
,   minutes_disponible
,   total_minutes
,   volume
,   CAST(minutes_disponible AS DOUBLE) / total_minutes as disp_dia
FROM disponible