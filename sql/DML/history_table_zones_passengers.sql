BEGIN;

DROP TABLE IF EXISTS nyc_tlc.history_table_zones_passengers;
CREATE TABLE nyc_tlc.history_table_zones_passengers AS (

With t as (
SELECT 
    *
    ,lag(dropoff_zone,1) over (partition by rank_sum_passenger_count,pickup_zone order by month) as lag_d
FROM 
    nyc_tlc.popular_destination_zones
  )

SELECT 
    * 
FROM 
    t
WHERE 
    dropoff_zone<>lag_d 
    OR lag_d is null
);

COMMIT;