INSERT INTO {table_name}

-- with weekly_retention_rate as (

with get_dates AS (
    SELECT
         DATE_TRUNC('week',cast('{start_date}' as datetime)) AS start_week
        , DATE_TRUNC('week',cast('{end_date}' as datetime)) + interval '7 days' AS end_week

)

select
address as address
,wk as wk
,active_days_per_week as active_days_per_week
,coalesce(cast(active_days_per_week as float) / 7.0, 0) as weekly_retention_rate
,case
when active_days_per_week > 0 then 1
else 0
end as is_active_week,
now() AS job_insert_time
    from (
    select
    aw.address AS address
    ,aw.wk AS wk
    ,coalesce(adpw.active_days_per_week, 0) as active_days_per_week
    from
    retention_address_weeks_nofilter aw
    left join
        (SELECT address, active_days_per_week, cast(wk as datetime) as wk_cast FROM gs_debug_active_days_per_week_nofilter
            WHERE cast(wk as datetime) BETWEEN (SELECT start_week FROM get_dates) and (SELECT end_week FROM get_dates)
            AND cast(wk as datetime) < DATE_TRUNC('week',NOW())
        ) adpw
    on aw.address = adpw.address
    and aw.wk = adpw.wk_cast

    WHERE aw.wk BETWEEN (SELECT start_week FROM get_dates) and (SELECT end_week FROM get_dates)
    AND aw.wk < DATE_TRUNC('week',NOW())

    group by 1,2,3
    
    ) w
-- )