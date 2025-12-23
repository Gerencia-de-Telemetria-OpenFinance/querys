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
    ,   httpmethod,
    CASE
        WHEN r.additionalinfo_authorisationflow = 'FIDO_FLOW' THEN 'JSR'
        WHEN r.additionalinfo_authorisationflow = 'HYBRID_FLOW' AND r.endpoint LIKE '%pix/payments%' THEN 'PAGAMENTOS_IMEDIATOS'
        WHEN r.additionalinfo_authorisationflow = 'HYBRID_FLOW' AND r.endpoint LIKE '%pix/recurring-payments%' AND r.additionalinfo_paymenttype = 'AUTOMATIC' THEN 'PIX_AUTOMATICO'
        WHEN r.additionalinfo_authorisationflow = 'HYBRID_FLOW' AND r.endpoint LIKE '%pix/recurring-payments%' AND r.additionalinfo_paymenttype = 'SWEEPING' THEN 'TRANSF_INTEL'
        ELSE 'NAO_INFORMADO'
    END AS produto -- se payments
    -- 'DADOS_CLIENTES' AS produto -- se clients
    -- NULL AS produto -- se security
    ,   processtimespan
    FROM 
        pcm_reports_clients
    WHERE
        status <> 'PAIRED_INCONSISTENT'
        AND processtimespan > 0
        AND statuscode NOT IN ('423', '429', '529')
        AND (
            (clientorgid = orgid AND clientorgid <> '926e3037-a685-553c-afa3-f7cb46ff8084')
            OR (
                serverorgid = orgid
                AND (status = 'UNPAIRED' OR clientorgid = '926e3037-a685-553c-afa3-f7cb46ff8084')
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