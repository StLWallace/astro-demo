import pytest

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(root_dir)
from plugins.gcs_utils import check_data_exists, delete_data

@pytest.fixture
def not_exists_task():
    return "not_exists"

@pytest.fixture
def exists_task():
    return "exist"


def test_check_data_exists(not_exists_task, exists_task):
    test_task = check_data_exists(path="fake_path", not_exists_task=not_exists_task, exists_task=exists_task)
    assert test_task == "not_exists"

