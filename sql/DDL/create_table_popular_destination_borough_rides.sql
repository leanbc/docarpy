BEGIN;
CREATE TABLE IF NOT EXISTS nyc_tlc.popular_destination_borough_rides
(
  month                    TIMESTAMP,
  pickup_borough           TEXT,
  dropoff_borough          TEXT,
  rank_sum_passenger_count BIGINT
);
COMMIT;