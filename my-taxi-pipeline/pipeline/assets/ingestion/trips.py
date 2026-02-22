"""@bruin

# Convention: ingestion.<asset_name>
name: ingestion.trips

type: python

# Bruin isolated Python image
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: varchar
    description: Type of taxi dataset (e.g. yellow, green)

  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp

  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp

  - name: passenger_count
    type: integer
    description: Number of passengers

  - name: trip_distance
    type: double
    description: Trip distance in miles

  - name: total_amount
    type: double
    description: Total fare amount

  - name: extracted_at
    type: timestamp
    description: Timestamp when the data was extracted

@bruin"""

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def generate_month_list(start_date: str, end_date: str):
    """
    Generate list of YYYY-MM between start and end dates (inclusive).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start.replace(day=1)

    while current <= end:
        months.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    return months


def materialize():
    """
    Ingestion logic using Bruin runtime context.
    - Uses BRUIN_START_DATE / BRUIN_END_DATE
    - Uses pipeline variable `taxi_types`
    """

    # --- 1. Read Bruin date window ---
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    # --- 2. Read pipeline variables ---
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    pipeline_vars = json.loads(vars_json)

    taxi_types = pipeline_vars.get("taxi_types", ["yellow"])

    # --- 3. Generate month partitions ---
    months = generate_month_list(start_date, end_date)

    all_dfs = []

    for taxi_type in taxi_types:
        for month in months:
            file_name = f"{taxi_type}_tripdata_{month}.parquet"
            url = f"{BASE_URL}/{file_name}"

            try:
                df = pd.read_parquet(url)

                # Normalize column names (handle schema differences)
                df.columns = df.columns.str.lower()

                # Standardize key fields
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()

                # Select minimal ingestion schema
                selected_cols = [
                    "taxi_type",
                    "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime",
                    "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime",
                    "passenger_count",
                    "trip_distance",
                    "total_amount",
                    "extracted_at",
                ]

                df = df[selected_cols]

                df = df.rename(columns={
                    selected_cols[1]: "pickup_datetime",
                    selected_cols[2]: "dropoff_datetime",
                })

                all_dfs.append(df)

                print(f"Loaded {file_name}")

            except Exception as e:
                print(f"Skipping {file_name}: {e}")

    if not all_dfs:
        return pd.DataFrame(columns=[
            "taxi_type",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
            "extracted_at",
        ])

    final_df = pd.concat(all_dfs, ignore_index=True)

    return final_df


