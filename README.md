# Astronomer Demo

## Overview
This repo contains DAGs and supporting libraries to demo Airflow using Astro

## DAGS
1. serial-dag.py
    - A simple DAG that demonstrates how to run tasks that are dependent on one another.
    - Uses EmptyOperators
2. parallel-dag.py
    - Similar to first but tasks run concurrently
3. deranged-dag.py
    - Intended to be a more complex process
    - Simulates ingestion process
        - Download data from (fake) data API to GCS
        - Ingest data from GCS to BigQuery
        - Combine data into wide table
        - Create several semantic tables


## Setup
Requires the creation of the following Google Cloud resources:
- Storage bucket
- BigQuery dataset
- Service account with roles:
    - roles/bigquery.dataEditor
    - roles/storage.admin
    - roles/bigquery.jobUser

