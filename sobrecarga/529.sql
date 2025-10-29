-- SOBRECARGA DO SISTEMA
with parameters as (
    SELECT
        DATE('2025-08-01') as init_date
    ,   DATE('2025-09-30') as end_date
)

, calls_cli as (
    SELECT
        serverorgid
    ,   statuscode
    ,   ts_to_date_gmt
    FROM pcm_reports_clients
    WHERE 1=1
        AND status NOT IN ('PAIRED_INCONSISTENT', 'DISCARDED')
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
        AND (
            statuscode LIKE '2%'
            OR statuscode LIKE '5%'
            OR statuscode IN ('408', '422')
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
), calls_pay as (
    SELECT
        serverorgid
    ,   statuscode
    ,   ts_to_date_gmt
    FROM pcm_reports_payments
    WHERE 1=1
        AND status NOT IN ('PAIRED_INCONSISTENT', 'DISCARDED')
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
        AND (
            statuscode LIKE '2%'
            OR statuscode LIKE '5%'
            OR statuscode IN ('408', '422')
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
), calls_sec as (
    SELECT
        serverorgid
    ,   statuscode
    ,   ts_to_date_gmt
    FROM pcm_reports_security
    WHERE 1=1
        AND status NOT IN ('PAIRED_INCONSISTENT', 'DISCARDED')
        AND (
            role = 'CLIENT'
            OR (
                role = 'SERVER'
                AND status = 'UNPAIRED'
            )
        )
        AND (
            statuscode LIKE '2%'
            OR statuscode LIKE '5%'
            OR statuscode IN ('408', '422')
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
), calls_opn as (
    SELECT
        organisationid as serverorgid
    ,   statuscode
    ,   DATE(ts_to_date) as ts_to_date_gmt
    FROM "pcm-product"."pcm_reports_opendata"
    WHERE 1=1
        AND status NOT IN ('PAIRED_INCONSISTENT', 'DISCARDED')
        AND (
            statuscode LIKE '2%'
            OR statuscode LIKE '5%'
            OR statuscode IN ('408', '422')
        )
        AND DATE(ts_to_date) BETWEEN
            (SELECT init_date FROM parameters) AND (SELECT end_date FROM parameters)
), calls as(
    SELECT * FROM calls_cli
    UNION ALL
    SELECT * FROM calls_pay
    UNION ALL
    SELECT * FROM calls_sec
    UNION ALL
    SELECT * FROM calls_opn
), volumes as (
    SELECT
        serverorgid as orgid
    ,   ts_to_date_gmt as ts_to_date
    ,   COUNT(*) FILTER(WHERE statuscode = '529') as volume_529
    ,   COUNT(*) as volume_total
    FROM calls
    GROUP BY 1, 2
)

SELECT
    *,
    CAST(volume_529 as DOUBLE) / volume_total as percentual_529
FROM volumes
ORDER BY orgid, ts_to_date