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
  - name: trip_id
    type: varchar
    description: Deterministic surrogate key for each trip
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null

  - name: dropoff_datetime
    type: timestamp

  - name: pickup_location_id
    type: integer

  - name: dropoff_location_id
    type: integer

  - name: fare_amount
    type: double
    checks:
      - name: non_negative

  - name: taxi_type
    type: varchar

  - name: payment_type_name
    type: varchar

custom_checks:
  - name: no_duplicate_trip_ids
    description: Ensure no duplicate surrogate keys exist
    query: |
      SELECT COUNT(*)
      FROM (
          SELECT trip_id
          FROM staging.trips
          GROUP BY trip_id
          HAVING COUNT(*) > 1
      )
    value: 0

  - name: no_negative_fares
    description: Ensure no negative fare values
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE fare_amount < 0
    value: 0
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