/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
@bruin */

WITH base AS (

    SELECT
        md5(
            concat(
                t.pickup_datetime,
                t.dropoff_datetime,
                t.pickup_location_id,
                t.dropoff_location_id,
                t.fare_amount,
                t.taxi_type
            )
        ) AS trip_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.fare_amount,
        t.taxi_type,
        t.payment_type
    FROM ingestion.trips t

    -- CRITICAL: required for time_interval
    WHERE t.pickup_datetime >= '{{ start_datetime }}'
      AND t.pickup_datetime < '{{ end_datetime }}'

),

deduplicated AS (

    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY trip_id
                   ORDER BY pickup_datetime
               ) AS rn
        FROM base
    )
    WHERE rn = 1

),

enriched AS (

    SELECT
        d.trip_id,
        d.pickup_datetime,
        d.dropoff_datetime,
        d.pickup_location_id,
        d.dropoff_location_id,
        d.fare_amount,
        d.taxi_type,
        p.payment_type_name
    FROM deduplicated d
    LEFT JOIN ingestion.payment_lookup p
      ON d.payment_type = p.payment_type_id

)

SELECT *
FROM enriched;