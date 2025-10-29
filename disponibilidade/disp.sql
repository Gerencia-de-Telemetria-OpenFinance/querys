-- DISPONIBILIDADE
WITH parameters AS (
    SELECT
        DATE('2025-03-01') as init_date
    ,   DATE('2025-09-30') as end_date
),

base as (
    SELECT
        organisationid AS serverorgid,
        NULL AS clientorgid,
        -- serverorgid,
        -- clientorgid,
        endpoint,
        httpmethod,
        ts_to_date,
        ts,
        statuscode,
        'CLIENT' AS role,
        -- role,
        status
    FROM "pcm-product"."pcm_reports_opendata"
    -- FROM pmc_reports_clients
    WHERE 1=1
        AND (
            statuscode = '408'
            OR statuscode like '5%'
            OR statuscode like '2%'
            OR statuscode = '422'
        )
        AND DATE(ts_to_date_gmt) BETWEEN
            (SELECT init_date FROM parameters)
                AND
            (SELECT end_date FROM parameters)
)

SELECT
                calc_disp_dia.serverorgid,
                calc_disp_dia.clientorgid,
                calc_disp_dia.endpoint,
                calc_disp_dia.httpmethod,
                calc_disp_dia.ts_to_date,
                calc_disp_dia.total_valid_requests_day,
                calc_disp_dia.total_successful_requests,
                calc_disp_dia.total_failed_requests,
                calc_disp_dia.total_minutes_available,
                calc_disp_dia.total_minutes_unavailable
                ROUND(
                    CAST(calc_disp_dia.total_minutes_available AS DOUBLE) /
                    CASE
                        WHEN calc_disp_dia.total_minutes_available + calc_disp_dia.total_minutes_unavailable = 0
                        THEN NULL
                        ELSE
                            CAST(calc_disp_dia.total_minutes_available + calc_disp_dia.total_minutes_unavailable AS DOUBLE)
                            END,
                    2
                ) AS percent_available_day
          FROM (
              SELECT 
                    calc_disp_minuto.serverorgid,
                    calc_disp_minuto.clientorgid,
                    calc_disp_minuto.endpoint,
                    calc_disp_minuto.httpmethod,
                    calc_disp_minuto.ts_to_date,
                    sum(total_chamadas_validas) as total_valid_requests_day,
                    sum(success) as total_successful_requests,
                    sum(errors) as total_failed_requests,
                    SUM(
                        CASE
                            WHEN calc_disp_minuto.disponibilidade_no_minuto >= 95 THEN 1
                            ELSE 0
                        END
                    ) AS total_minutes_available,
                    SUM(
                        CASE
                            WHEN calc_disp_minuto.disponibilidade_no_minuto < 95 THEN 1
                            ELSE 0
                        END
                    ) AS total_minutes_unavailable
              FROM (
                  SELECT
                        filtro_minuto.serverorgid,
                        filtro_minuto.clientorgid,
                        filtro_minuto.endpoint,
                        filtro_minuto.httpmethod,
                        filtro_minuto.ts_to_date,
                        filtro_minuto.minutes_timestamp,
                        filtro_minuto.total_chamadas_validas,
                        filtro_minuto.success,
                        filtro_minuto.errors,
                        CAST(ROUND((CAST(filtro_minuto.success AS DOUBLE) / (filtro_minuto.success + filtro_minuto.errors)) * 100, 2) AS DOUBLE) AS disponibilidade_no_minuto
                    FROM (
                        SELECT
                            serverorgid,
                            clientorgid,
                            endpoint,
                            httpmethod,
                            ts_to_date,
                            date_format(ts, '%Y-%m-%d %H:%i:00') AS minutes_timestamp,
                            COUNT(*) AS total_chamadas_validas,
                            SUM(
                                CASE
                                    WHEN (r.statuscode = '408' OR r.statuscode like '5%') THEN 1
                                    ELSE 0
                                END
                            ) AS errors,
                            SUM(
                                CASE
                                    WHEN r.statuscode like '2%' OR r.statuscode = '422' THEN 1
                                    ELSE 0
                                END
                            ) AS success
                        FROM base r
                        WHERE
                            (
                                r.role = 'CLIENT'
                                OR (
                                    r.role = 'SERVER'
                                    AND r.status = 'UNPAIRED'
                                )
                            )
                        GROUP BY
                            serverorgid,
                            clientorgid,
                            endpoint,
                            httpmethod,
                            ts_to_date,
                            date_format(ts, '%Y-%m-%d %H:%i:00')
                    ) AS filtro_minuto
             ) AS calc_disp_minuto
    GROUP by
        calc_disp_minuto.serverorgid,
        calc_disp_minuto.clientorgid,
        calc_disp_minuto.endpoint,
        calc_disp_minuto.httpmethod,
        calc_disp_minuto.ts_to_date
) as calc_disp_dia

order by ts_to_date