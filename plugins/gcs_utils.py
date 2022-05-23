""" These are used for GCS maintenance """

import gcsfs
import os


def check_data_exists(path: str, not_exists_task: str, exists_task: str) -> str:
    """ Checks to see if files exist"""
    client = gcsfs.GCSFileSystem(project=os.getenv("GCP_PROJECT"))

    try:
        client.ls(path)
        task = exists_task
    except FileNotFoundError:
        task = not_exists_task
    finally:
        return task

def delete_data(path: str) -> str:
    """ Deletes GCS files from a path """
    client = gcsfs.GCSFileSystem(project=os.getenv("GCP_PROJECT"))

    client.rmdir(path)

    result = f"Deleted existing files from {path}"

    return result