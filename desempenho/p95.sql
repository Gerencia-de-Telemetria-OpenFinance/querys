-- DESEMPENHO
with parameters as (
    SELECT
        DATE('2025-12-01') as init_date
    ,   DATE('2025-12-15') as end_date
),

dados as (
    SELECT
        DATE(ts_to_date_gmt) as ts_to_date_gmt
    ,   serverorgid
    ,   endpoint
    ,   httpmethod
    ,   CASE
            WHEN (additionalinfo_authorisationflow = 'FIDO_FLOW' AND endpoint LIKE '%pix/payments%') OR endpoint LIKE '%enrollments' THEN 'JSR'
            WHEN (endpoint LIKE '%/consents%' OR endpoint LIKE '%pix/payments%') THEN 'PAGAMENTOS_IMEDIATOS'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'AUTOMATIC' THEN 'PIX_AUTOMATICO'
            WHEN (endpoint LIKE '%recurring-consents%' OR endpoint LIKE '%pix/recurring-payments%') AND additionalinfo_paymenttype = 'SWEEPING' THEN 'TRANSF_INTEL'
            ELSE 'TRANSF_INTEL'
        END AS produto
    ,   processtimespan
    FROM 
        pcm_reports_payments
    WHERE
        status <> 'PAIRED_INCONSISTENT'
        AND processtimespan > 0
        AND statuscode NOT IN ('423', '429', '529')
        AND (
            clientorgid = orgid
            OR (
                serverorgid = orgid
                AND status = 'UNPAIRED'
            )
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
            
    UNION ALL
    
    SELECT
        DATE(ts_to_date_gmt) as ts_to_date_gmt
    ,   serverorgid
    ,   endpoint
    ,   httpmethod
    ,   'DADOS_CLIENTES' AS produto -- se clients
    ,   processtimespan
    FROM 
        pcm_reports_clients
    WHERE
        status <> 'PAIRED_INCONSISTENT'
        AND processtimespan > 0
        AND statuscode NOT IN ('423', '429', '529')
        AND (
            clientorgid = orgid
            OR (
                serverorgid = orgid
                AND status = 'UNPAIRED'
            )
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
)

SELECT
    ts_to_date_gmt as ts_to_date
,   serverorgid
,   endpoint
,   httpmethod
,   produto    
,   COUNT(1) as requests_count
,   CAST(ROUND(value_at_quantile(qdigest_agg(processtimespan, 1, 0.00001), 0.95)) as integer) as p95_ms
FROM dados
GROUP BY 1, 2, 3, 4, 5
