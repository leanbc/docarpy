BEGIN;

CREATE TABLE IF NOT EXISTS landing_nyc_tlc.stg_taxis_data
(
  unique_id             text,
  month                 TIMESTAMP,
  day                   TIMESTAMP,
  vendorid              integer,
  tpep_pickup_datetime  text,
  tpep_dropoff_datetime text,
  passenger_count       integer,
  trip_distance         real,
  ratecodeid            integer,
  store_and_fwd_flag    text,
  pulocationid          integer,
  dolocationid          integer,
  payment_type          integer,
  fare_amount           real,
  extra                 real,
  mta_tax               real,
  tip_amount            real,
  tolls_amount          real,
  improvement_surcharge real,
  total_amount          real,
  congestion_surcharge  real,
  bucket                text,
  file_name             text,
  pickup_borough        text,
  pickup_zone           text,
  pickup_service_zone   text,
  dropoff_borough       text,
  dropoff_zone          text,
  dropoff_service_zone  text,
  c_passenger_count     numeric
);

COMMIT;