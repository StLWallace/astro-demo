""" An additional library to define BQ table operators """

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os


def get_bq_job_operator(
    task_id: str,
    table: str,
    dataset: str,
    project_id: str = os.getenv("GCP_PROJECT"),
    location: str = "US",
    params: dict = None,
    query_file: str = None,
    query_string: str = None
) -> BigQueryInsertJobOperator:
    """ Creates a BigQueryInsertJobOperator with some default settings 
    Args:
        task_id - name of task 
        table - target table name
        dataset - dataset for target table
        project_id - Google Cloud project for target table
        location - location for BigQuery job/table. Defaults to 'US'
        params - a dict of parameters to render in the query
    """
    # If the query value is a file, format it correctly
    if query_file is not None:
        query = f"{{% include '{query_file}' %}}"
    elif query_string is not None:
        query = query_string
    job = BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": query,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset,
                    "tableId": table,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location=location,
        params=params,
    )

    return job
