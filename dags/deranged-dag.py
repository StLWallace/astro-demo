""" Part 1, exercise 3

"""

from datetime import datetime
from venv import create
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from plugins.gen_data import data_api_call
from plugins.gcs_utils import check_data_exists, delete_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import BranchPythonOperator
import yaml
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os

# Global variablwes
DAG_ID = "deranged-dag"
START_DATE = datetime(2022, 5, 22)
LANDING_BUCKET = "astro_demo_landing"
PARTITIONTIME = '{{ dag_run.logical_date.strftime("%Y%m%d%H") }}'
DATASET = "astro_demo"
PROJECT_ID = os.getenv("GCP_PROJECT")

default_args = {
    "gcp_conn_id": "google_cloud_default",
    "location": "US",
    "useLegacySql": False,
}

# This config will set various parameters of the tasks
with open("dags/_cfg/deranged-dag.yaml", "r") as f:
    cfg = yaml.safe_load(f)


def gen_ingest_task_group(
    bucket: str,
    dataset: str,
    table_name: str,
    n_row: int,
    n_id: int,
    n_code: int,
    n_metric: int,
    partition_time: str,
) -> TaskGroup:
    """ Creates a set of tasks to pull data from a mock data API, download to GCS, and load to BigQuery
    Args:
        bucket - the gcs bucket where data will be staged
        dataset - BigQuery dataset to load data into
        table_name - target table in BQ
        n_row - number of rows to generate from data source
        n_code - number of "code" columns to generate in source
        n_metric - number of "metric" columns to generate in source
        partition_time - datetime string YYYYMMDDHH for designating partition in GCS and BQ table
    Returns:
        A task group to ingest the table from source to BigQuery
    """
    write_path = f"gs://{bucket}/{table_name}"
    branching_path = f"{bucket}/{table_name}/partition_time={partition_time}"
    source_objects = f"{table_name}/partition_time={partition_time}/*"
    group_id = f"ingest_{table_name}"
    delete_task_id = "delete_gcs_partition"
    api_task_id = "api_to_gcs"
    gcs_to_bq_task_id = "gcs_to_bq"
    target_table_partition = f"{dataset}.{table_name}${partition_time}"

    with TaskGroup(group_id=group_id) as tg:

        branching = BranchPythonOperator(
            task_id="branching",
            python_callable=check_data_exists,
            op_kwargs={
                "path": branching_path,
                "exists_task": f"{group_id}.{delete_task_id}",
                "not_exists_task": f"{group_id}.{api_task_id}",
            },
        )

        delete_gcs_partition = PythonOperator(
            task_id=delete_task_id,
            python_callable=delete_data,
            op_kwargs={"path": branching_path},
        )

        api_to_gcs = data_api_call(
            task_id=api_task_id,
            path=write_path,
            n_row=n_row,
            n_id=n_id,
            n_code=n_code,
            n_metric=n_metric,
            partition_time=partition_time,
        )

        gcs_to_bigquery = GCSToBigQueryOperator(
            task_id=gcs_to_bq_task_id,
            bucket=bucket,
            source_objects=source_objects,
            destination_project_dataset_table=target_table_partition,
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "HOUR"},
            schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        )

        branching >> delete_gcs_partition >> api_to_gcs
        branching >> api_to_gcs
        api_to_gcs >> gcs_to_bigquery

    return tg


with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    catchup=False,
    default_args=default_args,
    template_searchpath="dags/sql/",
) as dag:

    create_wide_table = BigQueryInsertJobOperator(
        task_id="create_wide_table",
        configuration={
            "query": {
                "query": "{% include 'combined_table.sql' %}",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": cfg["wide_table"]["name"],
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location=default_args["location"],
        params={
            "fact_table": f'{DATASET}.{cfg["table_conf"][0]["name"]}',
            "tables": [
                f"{DATASET}.{table['name']}"
                for table in cfg["table_conf"]
                if "fact" not in table["name"]
            ],
            "partition_time": PARTITIONTIME
        },
    )

    for table in cfg["table_conf"]:
        ingest_tg = gen_ingest_task_group(
            bucket=LANDING_BUCKET,
            dataset=cfg["dataset"],
            table_name=table["name"],
            n_row=table["n_row"],
            n_id=table["n_id"],
            n_code=table["n_code"],
            n_metric=table["n_metric"],
            partition_time=PARTITIONTIME,
        )

        ingest_tg >> create_wide_table
