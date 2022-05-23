""" Part 1, exercise 3

"""

from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from plugins.gen_data import data_api_call
from plugins.gcs_utils import check_data_exists, delete_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python import BranchPythonOperator
import yaml
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Global variablwes
DAG_ID = "deranged-dag"
START_DATE = datetime(2022, 5, 22)
LANDING_BUCKET = "astro_demo_landing"
PARTITIONTIME = "{{ dag_run.logical_date.strftime('%Y%m%d%H') }}"
DATASET = "astro_demo"

default_args = {"gcp_conn_id": "google_cloud_default"}

# This config will set various parameters of the tasks
with open("dags/_cfg/deranged-dag.yaml", "r") as f:
    cfg = yaml.safe_load(f)


def gen_ingest_task_group(
    bucket: str,
    dataset: str,
    table_name: str,
    n_row: int,
    n_code: int,
    n_metric: int,
    partition_time: str,
) -> TaskGroup:
    """ Creates a set of tasks to pull data from a mock data API, download to GCS, and load to BigQuery
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


with DAG(
    dag_id=DAG_ID, start_date=START_DATE, catchup=False, default_args=default_args,
) as dag:

    tasks = [
        gen_ingest_task_group(
            bucket=LANDING_BUCKET,
            dataset=cfg["dataset"],
            table_name=table["name"],
            n_row=table["n_row"],
            n_code=table["n_code"],
            n_metric=table["n_metric"],
            partition_time=PARTITIONTIME,
        )
        for table in cfg["table_conf"]
    ]
