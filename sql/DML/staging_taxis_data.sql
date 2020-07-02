BEGIN;

DROP TABLE IF EXISTS partititons_in_source;
CREATE TEMP TABLE partititons_in_source AS (

  SELECT DISTINCT
    file_name
  FROM
    landing_nyc_tlc.stg_taxis_data  t
);

COMMIT;
BEGIN;

INSERT INTO landing_nyc_tlc.stg_taxis_data (
  SELECT
    md5(coalesce(t.vendorid::text,'no_vendorid') || coalesce(t.passenger_count::text,'no_vendorid') || t.tpep_pickup_datetime || t.tpep_dropoff_datetime || pulocationid || dolocationid || trip_distance || total_amount::text ) as unique_id,
    date_trunc('month', t.tpep_pickup_datetime::TIMESTAMP) as  "month",
    date_trunc('day', t.tpep_pickup_datetime::TIMESTAMP) as  "day",
    t.*,
    p.borough as pickup_borough,
    coalesce(p.zone,'Unknown') as pickup_zone,
    coalesce(p.service_zone,'Unknown') as pickup_service_zone,
    d.borough as dropoff_borough,
    coalesce(d.zone,'Unknown') as dropoff_zone,
    coalesce(d.service_zone,'Unknown') as dropoff_service_zone,
    coalesce(t.passenger_count,(SELECT
                                  round(avg(passenger_count))
                                FROM landing_nyc_tlc.taxis
                                WHERE passenger_count is not null)) as c_passenger_count
  From
    landing_nyc_tlc.taxis t
      LEFT JOIN partititons_in_source s ON t.file_name=s.file_name
      LEFT JOIN landing_nyc_tlc.zone_lookup p ON t.pulocationid=p.locationid
      LEFT JOIN landing_nyc_tlc.zone_lookup d ON t.dolocationid=d.locationid
  Where
    date_trunc('month', t.tpep_pickup_datetime::date) = to_date(
        substring(t.file_name,27,7),
        'YYYY-MM')
    AND s.file_name IS NULL);
COMMIT;