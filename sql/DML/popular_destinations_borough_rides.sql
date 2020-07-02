BEGIN;

INSERT INTO nyc_tlc.popular_destination_borough_rides (
SELECT
  "month",
  pickup_borough,
  dropoff_borough,
  ROW_NUMBER() OVER (
    PARTITION BY month,pickup_borough
    ORDER BY count(unique_id) DESC) AS rank_sum_passenger_count
FROM
  landing_nyc_tlc.stg_taxis_data
WHERE
    total_amount>=0
Group by
  "month",
  pickup_borough,
  dropoff_borough
Order by
  "month",
  pickup_borough,
  dropoff_borough
  );

COMMIT;