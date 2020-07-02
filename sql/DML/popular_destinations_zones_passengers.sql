BEGIN;

INSERT INTO nyc_tlc.popular_destination_zones (
  SELECT
    "month",
    pickup_zone,
    dropoff_zone,
    ROW_NUMBER() OVER (
         PARTITION BY month,pickup_zone
         ORDER BY Sum(c_passenger_count) DESC) AS rank_sum_passenger_count
  FROM
    landing_nyc_tlc.stg_taxis_data
  WHERE
      total_amount>=0
  Group by
    "month",
    pickup_zone,
    dropoff_zone
  Order by
    "month",
    pickup_zone,
    dropoff_zone);

COMMIT;