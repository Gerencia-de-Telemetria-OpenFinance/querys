-- PERCENTIL EXATO
WITH parameters AS (
    SELECT 
    DATE('2025-07-02') as init_date
,   DATE('2025-07-02') as final_date
,   '926e3037-a685-553c-afa3-f7cb46ff8084' as orgid
,   '/open-banking/bank-fixed-incomes/v1/investments/{investmentId}/transactions-current' as ndpnt
,   'GET' as mthd
)

, ENTRADA AS (
    SELECT
        r.ts_to_date_gmt AS ts_to_date,
        r.serverorgid,
        r.endpoint,
        r.httpmethod,
        CAST(r.processtimespan AS BIGINT) AS processtimespan
    FROM "pcm_product"."pcm_reports_clients" AS r
    WHERE 1=1
      AND DATE(r.ts_to_date_gmt) BETWEEN 
        (SELECT init_date from parameters) AND (SELECT final_date from parameters)
      AND r.processtimespan > 0
      AND r.statuscode NOT IN ('429', '529')
      AND status <> 'PAIRED_INCONSISTENT'
      AND (
            role = 'CLIENT'
         OR (role = 'SERVER' AND status = 'UNPAIRED')
      )
       AND serverorgid = (SELECT orgid from parameters)
       AND endpoint = (SELECT ndpnt from parameters)
       AND httpmethod = (SELECT mthd from parameters)
),
MEIO AS (
    SELECT
        ts_to_date,
        serverorgid,
        endpoint,
        httpmethod,
        processtimespan,
        ROW_NUMBER() OVER (
            PARTITION BY ts_to_date, serverorgid, endpoint, httpmethod
            ORDER BY processtimespan
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY ts_to_date, serverorgid, endpoint, httpmethod
        ) AS total
    FROM ENTRADA
)
SELECT
    t.ts_to_date,
    t.serverorgid,
    t.endpoint,
    t.httpmethod,
    t.total AS qtd,
    t.k     AS posicao_p95_nearest_rank,
    t.processtimespan AS p95
FROM (
    SELECT
        ts_to_date,
        serverorgid,
        endpoint,
        httpmethod,
        processtimespan,
        rn,
        total,
        CAST(
            LEAST(
                GREATEST(ROUND(0.95 * CAST(total AS DOUBLE)), 1.0),
                CAST(total AS DOUBLE)
            ) AS BIGINT
        ) AS k
    FROM MEIO
) t
WHERE t.rn = t.k
ORDER BY 1,2,3,4;