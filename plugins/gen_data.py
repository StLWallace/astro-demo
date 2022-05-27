""" Generates fake data and writes it to GCS 
Meant to simulate a syncronous data API
"""

from typing import Literal
import pandas as pd
import random
import string
from numpy import number
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

GCS_PATH = "gs://astro_demo_landing/test/data.parquet"


class DataGenerator:
    """ Generates random data frames
    """

    def __init__(self):
        self.letters = string.ascii_letters

    def __random_id(self, a=1, b=10) -> int:
        return random.randint(a, b)

    def __random_str(self, length: int = 10) -> str:
        return "".join(random.choice(self.letters) for i in range(10))

    def __random_metric(self, a=1, b=100) -> number:
        return random.uniform(a, b)

    def __gen_col(self, type: Literal["id", "code", "metric"], length: int) -> list:
        if type == "id":
            col = [self.__random_id() for i in range(length)]
        elif type == "code":
            col = [self.__random_str() for i in range(length)]
        elif type == "metric":
            col = [self.__random_metric() for i in range(length)]

        return col

    def get_table(
        self, n_row: int = 10, n_id: int = 0, n_code: int = 2, n_metric: int = 3
    ) -> pd.DataFrame:

        df = pd.DataFrame({"id": range(1, (n_row + 1))})
        for i in range(1, (n_id + 1)):
            df[f"id_{i}"] = self.__gen_col("id", n_row)
        for i in range(1, (n_code + 1)):
            df[f"code{i}"] = self.__gen_col("code", n_row)
        for i in range(1, (n_metric + 1)):
            df[f"metric_{i}"] = self.__gen_col("metric", n_row)
        return df


def download_data(
    path: str, n_row: int = 10, n_id: int = 0, n_code: int = 2, n_metric: int = 3
) -> pd.DataFrame:
    gen = DataGenerator()
    df = gen.get_table(n_row, n_id, n_code, n_metric)
    df.to_parquet(path=path)
    print(f"Wrote data to {path}")


def data_api_call(
    task_id: str,
    path: str,
    partition_time: str,
    n_row: int = 10,
    n_id: int = 0,
    n_code: int = 2,
    n_metric: int = 3,
    trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
) -> PythonOperator:
    """ Creates a Python operator for the download_data function 
    Args:
        path - a GCS location to write the data generated
        partition_time - a datetime string YYYYMMDDHH to use in the path
        ...
    """
    full_path = f"{path}/partition_time={partition_time}/data.parquet"
    task = PythonOperator(
        task_id=task_id,
        python_callable=download_data,
        op_kwargs={
            "path": full_path,
            "n_row": n_row,
            "n_id": n_id,
            "n_code": n_code,
            "n_metric": n_metric,
        },
        trigger_rule=trigger_rule
    )
    return task


if __name__ == "__main__":

    df = download_data(path=GCS_PATH)
