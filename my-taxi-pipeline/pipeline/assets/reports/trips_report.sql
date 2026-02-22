/* @bruin

name: reports.daily_trips

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: taxi_type
    type: varchar
    description: Taxi service type (yellow, green)
    primary_key: true

  - name: pickup_date
    type: DATE
    description: Trip pickup date
    primary_key: true

  - name: total_trips
    type: BIGINT
    description: Number of trips per taxi type per day
    checks:
      - name: non_negative

  - name: total_revenue
    type: DOUBLE
    description: Total fare revenue per taxi type per day
    checks:
      - name: non_negative

  - name: avg_fare
    type: DOUBLE
    description: Average fare per trip

@bruin */

WITH filtered AS (

    SELECT
        taxi_type,
        CAST(pickup_datetime AS DATE) AS pickup_date,
        fare_amount
    FROM staging.trips

    -- REQUIRED for time_interval
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'

)

SELECT
    taxi_type,
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare
FROM filtered
GROUP BY taxi_type, pickup_date;