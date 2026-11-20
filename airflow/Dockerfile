# Airflow image with providers + libs used by purchase_order_pipeline tasks.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
  -r /requirements.txt

# dbt is not on the Airflow constraints file; install after providers.
RUN pip install --no-cache-dir "dbt-snowflake>=1.9,<2"

