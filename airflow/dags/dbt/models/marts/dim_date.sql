-- Calendar spine covering the extract window (role-played by facts).
WITH spine AS (
    SELECT DATEADD('day', SEQ4(), DATE '2025-12-01') AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 400))
)

SELECT
    date_day,
    YEAR(date_day) AS year_number,
    MONTH(date_day) AS month_number,
    DAY(date_day) AS day_of_month,
    DAYOFWEEKISO(date_day) AS day_of_week_iso,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month
FROM spine
WHERE date_day <= DATE '2026-11-30'
