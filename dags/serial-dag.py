""" For Part 1, exercise 1:
Creates a simple DAG with 4 tasks that are serially dependent
"""

from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


# This list is the name and order of tasks to create.
TASK_LIST = ["first_task", "second_task", "third_task", "fourth_task"]


def define_task_set_upstream(task_id: str, prev_task: EmptyOperator) -> EmptyOperator:
    """ Creates operator and sets upstream task
    Args:
        task_id - name of new task to create
        prev_task - task to set upstream of new task

    Returns:
        An empty operator
    """
    new_task = EmptyOperator(task_id=task_id)
    if prev_task is not None:
        prev_task >> new_task
    return new_task


def gen_tasks(task_list: list) -> list:
    """ Iterates through list of tasks, creates the tasks, and sets dependency"""
    prev_task = None
    tasks = []
    for task in task_list:
        new_task = define_task_set_upstream(task_id=task, prev_task=prev_task)
        prev_task = new_task
        tasks.append(new_task)
    return tasks

with DAG(dag_id="serial-dag", start_date=datetime(2022, 5, 22), catchup=False) as dag:

    start = EmptyOperator(task_id="start")
    tasks = gen_tasks(TASK_LIST)
    end = EmptyOperator(task_id="end")

    start >> tasks >> end