-- VÁLIDAS / INVÁLIDAS
WITH parameters AS (
    SELECT
        DATE('2025-06-01') as init_date
    ,   DATE('2025-06-30') as end_date
),

uniao_requisicoes AS (
    SELECT ts_to_date_gmt, serverorgid, clientorgid, endpoint, httpmethod, statuscode, COUNT(*) as status_count
    FROM pcm_reports_clients
    WHERE 1=1
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
        GROUP BY 1, 2, 3, 4, 5, 6
        
    UNION ALL

    SELECT ts_to_date_gmt, serverorgid, clientorgid, endpoint, httpmethod, statuscode, COUNT(*) as status_count
    FROM pcm_reports_payments
    WHERE 1=1
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
    GROUP BY 1, 2, 3, 4, 5, 6
    
    UNION ALL

    SELECT ts_to_date_gmt, serverorgid, clientorgid, endpoint, httpmethod, statuscode, COUNT(*) as status_count
    FROM pcm_reports_security
    WHERE 1=1
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
    GROUP BY 1, 2, 3, 4, 5, 6
    
    UNION ALL

    SELECT
        DATE(ts_to_date) as ts_to_date_gmt,
        organisationid as serverorgid,
        NULL as clientorgid,
        endpoint,
        httpmethod,
        statuscode,
        COUNT(*) as status_count
    FROM "pcm-product"."pcm_reports_opendata"
    WHERE 1=1
        AND DATE(ts_to_date) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    ts_to_date_gmt as ts_to_date,
    serverorgid,
    clientorgid,
    endpoint,
    httpmethod,
    SUM(status_count) as total_requisicoes,
    SUM(
        CASE 
            WHEN statuscode LIKE '2%'
                OR statuscode LIKE '5%'
                OR statuscode = '408'
                OR statuscode = '422'
            THEN status_count
            ELSE 0
            END
    ) as requisicoes_validas
FROM uniao_requisicoes
GROUP BY 1, 2, 3, 4, 5;