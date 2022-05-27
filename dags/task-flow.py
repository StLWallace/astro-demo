from airflow.decorators import task, dag
from datetime import datetime
import os

# Global variables
DAG_ID = "task-flow-dag"
START_DATE = datetime(2022, 5, 22)
LANDING_BUCKET = "astro_demo_landing"
PARTITIONTIME = '{{ dag_run.logical_date.strftime("%Y%m%d%H") }}'
DATASET = "astro_demo"
PROJECT_ID = os.getenv("GCP_PROJECT")

@dag(
    catchup=False,
    start_date=START_DATE
)

def task_flow_dag():
    @task
    def load_data_gcs(source_table: str, partition_time: str, landing_bucket: str) -> str:
        """ Fake task to load data to GCS """
        data_path = f"{landing_bucket}/fake/{source_table}/partition_time={partition_time}"
        print(f"Loaded data from {source_table} to {data_path}")
        return data_path

    @task
    def load_bigquery(data_path: str, table_name: str, dataset: str) -> str:
        """ Fake task to load data to BigQuery """
        print(f"Loaded data from {data_path} to {dataset}.{table_name}")
        return f"{dataset}.{table_name}"

    @task
    def load_wide_table(fact_table: str, dim1: str, dim2: str, dataset: str) -> str:
        """ Fake task to load wide table """
        query = f"""
            SELECT ft.*, 
            (SELECT AS STRUCT dim1.*) AS dim1, 
            (SELECT AS STRUCT dim2.*) AS dim2
            FROM {fact_table} ft
            JOIN {dim1} dim1
            ON ft.id1 = dim1.id
            JOIN {dim2} dim2
            ON ft.id2 = dim2.id
        """
        print(query)
        wide_table_name = f"{dataset}.wide_table"
        return wide_table_name

    @task
    def create_semantic_table(wide_table: str) -> None:
        """ Fake task to create semantic table from wide table """
        query = f"SELECT code1, COUNT(*) FROM {wide_table}"
        print(query)

    fact_table = load_data_gcs(
        source_table="fact_table",
        partition_time=PARTITIONTIME,
        landing_bucket=LANDING_BUCKET
    )

    dimension1 = load_data_gcs(
        source_table="dimension1",
        partition_time=PARTITIONTIME,
        landing_bucket=LANDING_BUCKET
    )

    dimension2 = load_data_gcs(
        source_table="dimension2",
        partition_time=PARTITIONTIME,
        landing_bucket=LANDING_BUCKET
    )

    load_bq_fact = load_bigquery(data_path=fact_table, table_name="fact_table", dataset=DATASET)
    load_bq_dim1 = load_bigquery(data_path=dimension1, table_name="dimension1", dataset=DATASET)
    load_bq_dim2 = load_bigquery(data_path=dimension2, table_name="dimension2", dataset=DATASET)

    wide_table = load_wide_table(fact_table=load_bq_fact, dim1=load_bq_dim1, dim2=load_bq_dim2, dataset=DATASET)

    semantic_table = create_semantic_table(wide_table=wide_table)

data_process = task_flow_dag()


