-- VOLUME
WITH parameters as (
    SELECT
        DATE('2025-09-01') as init_date
    ,   DATE('2025-09-30') as end_date
),

requests as (
    SELECT
        serverorgid
    ,   clientorgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date_gmt as ts_to_date
    FROM pcm_reports_clients
    WHERE 1=1
        AND (role = 'CLIENT'
            OR (role = 'SERVER' AND status = 'UNPAIRED'))
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
),

volumes AS (
    SELECT
        serverorgid AS orgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   COUNT(1) AS volume_as_server
    ,   0 AS volume_as_client
    FROM requests
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        clientorgid AS orgid
    ,   endpoint
    ,   httpmethod
    ,   ts_to_date
    ,   0 AS volume_as_server
    ,   COUNT(1) AS volume_as_client
    FROM requests
    GROUP BY 1, 2, 3, 4
)
SELECT
    orgid
,   endpoint
,   httpmethod
,   ts_to_date
,   SUM(volume_as_server) AS volume_as_server
,   SUM(volume_as_client) AS volume_as_client
,   SUM(volume_as_server + volume_as_client) AS volume_total
FROM volumes
GROUP BY 1, 2, 3, 4


-- VOLUME opendata
SELECT
    organisationid AS orgid
,   endpoint
,   httpmethod
,   ts_to_date_gmt as ts_to_date
,   COUNT(1) AS volume_as_server
,   0 AS volume_as_client
FROM "pcm-product"."pcm_reports_opendata"
WHERE 1=1
    AND DATE(ts_to_date_gmt) BETWEEN DATE '2025-03-01' AND DATE '2025-09-30'
GROUP BY 1, 2, 3, 4