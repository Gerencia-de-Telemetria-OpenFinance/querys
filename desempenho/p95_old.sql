-- DESEMPENHO
with parameters as (
    SELECT
        DATE('2025-03-01') as init_date
    ,   DATE('2025-09-30') as end_date
),

dados as (
    SELECT
        DATE(ts_to_date_gmt) as ts_to_date_gmt
    ,   organisationid as serverorgid
    -- ,   organisationid as serverorgid
    ,   endpoint
    ,   httpmethod
    ,   processtimespan
    FROM 
        -- pcm_reports_clients
        "pcm-product"."pcm_reports_opendata"
    WHERE
        status <> 'PAIRED_INCONSISTENT'
        AND processtimespan > 0
        AND statuscode NOT IN ('423', '429', '529')
        -- AND (clientorgid = orgid OR (serverorgid = orgid and status = 'UNPAIRED'))
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
,   COUNT(1) as requests_count
,   CAST(ROUND(approx_percentile(processtimespan, 0.95)) as integer) as p95_ms
FROM dados
GROUP BY 1, 2, 3, 4