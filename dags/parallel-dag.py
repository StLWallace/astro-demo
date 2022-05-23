""" For Part 1, exercise 2:
Creates a simple DAG with 4 tasks that are run in parallel
"""

from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


# This list is the name and order of tasks to create.
TASK_LIST = ["first_task", "second_task", "third_task", "fourth_task"]
DAG_ID = "parallel-dag"


def define_task(task_id: str) -> EmptyOperator:
    """ Creates operator and sets upstream task
    Args:
        task_id - name of new task to create

    Returns:
        An empty operator
    """
    new_task = EmptyOperator(task_id=task_id)

    return new_task


def gen_tasks(task_list: list) -> list:
    """ Iterates through list of tasks, creates the tasks"""
    tasks = [define_task(task) for task in task_list]
    return tasks


with DAG(dag_id=DAG_ID, start_date=datetime(2022, 5, 22), catchup=False) as dag:

    start = EmptyOperator(task_id="start")
    tasks = gen_tasks(TASK_LIST)
    end = EmptyOperator(task_id="end")

    start >> tasks >> end
