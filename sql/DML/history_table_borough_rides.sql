BEGIN;

DROP TABLE IF EXISTS nyc_tlc.history_table_borough_rides;
CREATE TABLE nyc_tlc.history_table_borough_rides AS (

With t as (
SELECT 
    *
    ,lag(dropoff_borough,1) over (partition by rank_sum_passenger_count,pickup_borough order by month) as lag_d
FROM 
    nyc_tlc.popular_destination_borough_rides
  )

SELECT 
    * 
FROM 
    t
WHERE 
    dropoff_borough<>lag_d 
    OR lag_d is null
);

COMMIT;