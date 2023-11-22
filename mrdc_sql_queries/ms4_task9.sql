WITH datetime_table AS (
    SELECT 
        "year",
		"month",
		"day",
		"timestamp",
        CONCAT("year", '-', 
               LPAD("month"::text, 2, '0'), 
               '-', LPAD("day"::text, 2, '0'),
               ' ', "timestamp") AS datetime_column 
    FROM 
        dim_date_times
)
, time_diff_seconds AS (
    SELECT 
        "year",
        EXTRACT(HOUR FROM (LEAD(datetime_column) OVER (ORDER BY datetime_column)::timestamp - datetime_column::timestamp)::interval)::NUMERIC * 60 * 60 +
        EXTRACT(MINUTE FROM (LEAD(datetime_column) OVER (ORDER BY datetime_column)::timestamp - datetime_column::timestamp)::interval)::NUMERIC * 60 +
        EXTRACT(SECOND FROM (LEAD(datetime_column) OVER (ORDER BY datetime_column)::timestamp - datetime_column::timestamp)::interval)::NUMERIC AS seconds_taken
    FROM 
        datetime_table
)

SELECT 
    "year",
    TO_CHAR(
        INTERVAL '1 second' * AVG(seconds_taken) 
        , '"hours": HH, "minutes": MI, "seconds": SS') AS actual_time_taken
FROM 
    time_diff_seconds
WHERE "year" 
	IN ('2013','1993','2002','2022','2008')
GROUP BY "year"
ORDER BY  
	CASE "year"
        WHEN '2013' THEN 1
        WHEN '1993' THEN 2
        WHEN '2002' THEN 3
        WHEN '2022' THEN 4
        WHEN '2008' THEN 5
	END;
