BEGIN;
CREATE TABLE IF NOT EXISTS nyc_tlc.popular_destination_zones
(
  month                    TIMESTAMP,
  pickup_zone              TEXT,
  dropoff_zone             TEXT,
  rank_sum_passenger_count BIGINT
);
COMMIT;
