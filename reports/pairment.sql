-- REPORT
WITH parameters as (
    SELECT
        DATE '2025-11-16' as init_date
    ,   DATE '2025-11-30' as end_date
),

requests as (
    SELECT
        ts_to_date_gmt as ts_to_date
    ,   serverorgid
    ,   clientorgid
    ,   role
    ,   endpoint
    ,   httpmethod
    ,   'DADOS_CLIENTES' AS produto
    ,   status
    ,   arrived_at - INTERVAL '3' HOUR AS arrived_at
    ,   ts - INTERVAL '3' HOUR AS ts
    FROM "pcm_product"."pcm_reports_clients"
    WHERE 1=1
        AND ts_to_date_gmt BETWEEN 
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
        AND (
            orgid = clientorgid
            OR (
                orgid = serverorgid
                AND status = 'UNPAIRED'
            )
        )

    UNION ALL

    SELECT
        ts_to_date_gmt as ts_to_date
    ,   serverorgid
    ,   clientorgid
    ,   role
    ,   endpoint
    ,   httpmethod
    ,   CASE
            WHEN (additionalinfo_authorisationflow = 'FIDO_FLOW' AND endpoint LIKE '%pix/payments%') OR endpoint LIKE '%enrollments' THEN 'JSR'
            WHEN (endpoint LIKE '%/consents%' OR endpoint LIKE '%pix/payments%') THEN 'PAGAMENTOS_IMEDIATOS'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'AUTOMATIC' THEN 'PIX_AUTOMATICO'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'SWEEPING' THEN 'TRANSF_INTEL'
            ELSE 'TRANSF_INTEL'
        END AS produto
    ,   status
    ,   arrived_at - INTERVAL '3' HOUR AS arrived_at
    ,   ts - INTERVAL '3' HOUR AS ts
    FROM "pcm_product"."pcm_reports_payments"
    WHERE 1=1
        AND ts_to_date_gmt BETWEEN 
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
        AND (
            orgid = clientorgid
            OR (
                orgid = serverorgid
                AND status = 'UNPAIRED'
            )
        )
        AND (
            additionalinfo_authorisationflow = 'FIDO_FLOW'
            OR (
                additionalinfo_authorisationflow = 'HYBRID_FLOW'
                AND (endpoint LIKE '%/consents%' OR endpoint LIKE '%pix/payments%' OR endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%')
            )
        )
),

pairment as (
    SELECT
        ts_to_date
    ,   clientorgid as orgid
    ,   endpoint
    ,   httpmethod
    ,   produto
    ,   CASE WHEN arrived_at <= (CAST(ts_to_date AS TIMESTAMP) + INTERVAL '1' DAY + INTERVAL '8' HOUR) THEN 1 ELSE 0 END AS eight_hour
    ,   CASE WHEN arrived_at <= (ts + INTERVAL '7' DAY) THEN 1 ELSE 0 END AS seven_days
    ,   SUM(CASE WHEN status = 'PAIRED' THEN 1 ELSE 0 END) as paired_count
    ,   SUM(CASE WHEN status = 'UNPAIRED' THEN 1 ELSE 0 END) as unpaired_self_count
    ,   SUM(CASE WHEN status = 'PAIRED_INCONSISTENT' THEN 1 ELSE 0 END) as inconsisten_count
    ,   SUM(CASE WHEN status = 'SINGLE' THEN 1 ELSE 0 END) as single_count
    FROM requests
    GROUP BY 1, 2, 3, 4, 5, 6, 7

    UNION ALL

    SELECT
        ts_to_date
    ,   serverorgid as orgid
    ,   endpoint
    ,   httpmethod
    ,   produto
    ,   CASE WHEN arrived_at <= (CAST(ts_to_date AS TIMESTAMP) + INTERVAL '1' DAY + INTERVAL '8' HOUR) THEN 1 ELSE 0 END AS eight_hour
    ,   CASE WHEN arrived_at <= (ts + INTERVAL '7' DAY) THEN 1 ELSE 0 END AS seven_days
    ,   SUM(CASE WHEN status = 'PAIRED' THEN 1 ELSE 0 END) as paired_count
    ,   SUM(CASE WHEN status = 'UNPAIRED' THEN 1 ELSE 0 END) as unpaired_self_count
    ,   SUM(CASE WHEN status = 'PAIRED_INCONSISTENT' THEN 1 ELSE 0 END) as inconsisten_count
    ,   SUM(CASE WHEN status = 'SINGLE' THEN 1 ELSE 0 END) as single_count
    FROM requests
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

counterpart as (
    SELECT
        ts_to_date
    ,   CASE WHEN role = 'SERVER' THEN clientorgid ELSE serverorgid END as orgid
    ,   endpoint
    ,   httpmethod
    ,   produto
    ,   1 AS eight_hour
    ,   1 AS seven_days
    ,   SUM(CASE WHEN status = 'UNPAIRED' THEN 1 ELSE 0 END) as unpaired_counterpart_count
    FROM requests
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    COALESCE(p.ts_to_date, c.ts_to_date) AS ts_to_date
,   COALESCE(p.orgid, c.orgid) AS orgid
,   COALESCE(p.endpoint, c.endpoint) AS endpoint
,   COALESCE(p.httpmethod, c.httpmethod) AS httpmethod
,   COALESCE(p.produto, c.produto) AS produto
,   COALESCE(p.eight_hour, c.eight_hour) AS eight_hour
,   COALESCE(p.seven_days, c.seven_days) AS seven_days
,   COALESCE(p.paired_count, 0) AS paired_count
,   COALESCE(p.unpaired_self_count, 0) AS unpaired_self_count
,   COALESCE(c.unpaired_counterpart_count, 0) AS unpaired_counterpart_count
,   COALESCE(p.inconsisten_count, 0) AS inconsisten_count
,   COALESCE(p.single_count, 0) AS single_count
FROM pairment p FULL OUTER JOIN counterpart c
ON 1=1
    AND p.ts_to_date = c.ts_to_date
    AND p.orgid = c.orgid
    AND p.endpoint = c.endpoint
    AND p.httpmethod = c.httpmethod
    AND p.produto = c.produto
    AND p.eight_hour = c.eight_hour
    AND p.seven_days = c.seven_days
