WITH date_ranges AS (
        SELECT 
            chain,
            mv_name,
            start_date,
            end_date,
            leadInFrame(start_date) OVER (
                PARTITION BY chain, mv_name 
                ORDER BY start_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS next_start_date
        FROM backfill_tracking
    ),
    gaps AS (
        SELECT 
            chain,
            mv_name,
            end_date + INTERVAL 1 DAY AS gap_start,
            next_start_date - INTERVAL 1 DAY AS gap_end
        FROM date_ranges
        WHERE next_start_date > end_date + INTERVAL 1 DAY
    )
    SELECT 
        chain,
        mv_name AS table_view_name,
        gap_start,
        gap_end
    FROM gaps
    ORDER BY chain, mv_name, gap_start