WITH date_ranges AS (
    SELECT 
        start_date,
        end_date,
        leadInFrame(start_date) OVER (
            ORDER BY start_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS next_start_date
    FROM backfill_tracking
    WHERE chain = '{chain}'
    AND mv_name = '{mv_name}'
    ORDER BY start_date
)
SELECT 
    COALESCE(
        MAX(CASE 
            WHEN start_date <= toDate('{start_date}') 
                AND (next_start_date > toDate('{start_date}') OR next_start_date IS NULL)
                AND end_date < toDate('{end_date}')
            THEN end_date
        END),
        MIN(start_date) - INTERVAL 1 DAY
    ) AS latest_fill_start
FROM date_ranges