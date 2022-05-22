from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import DAG
import os
from datetime import datetime

DAG_ID = "test-bq-connection"
DATASET = "astro_demo"
PROJECT_ID = os.getenv("GCP_PROJECT")
TARGET_TABLE = "test_bq_conn"


default_args = {
    "catchup": False,
    "project_id": PROJECT_ID,
    "useLegacySql": False
}

TEST_Q = "SELECT 1 AS ID, 'A' AS CODE"

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    start_date=datetime(2022, 5, 22),
) as dag:

    insert_query_job = BigQueryInsertJobOperator(
        task_id="test_insert",
        configuration={
            "query": {
                "query": TEST_Q,
                'destinationTable': {
                    'projectId': PROJECT_ID,
                    'datasetId': DATASET,
                    'tableId': TARGET_TABLE
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
        #location=location,
    )

