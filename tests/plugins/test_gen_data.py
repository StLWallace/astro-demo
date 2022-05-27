import pytest

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(root_dir)
from plugins.gen_data import DataGenerator, data_api_call


@pytest.fixture
def client():
    return DataGenerator()

class TestDataGenerator:

    def test_get_table(self, client):
        test_table = client.get_table(n_row=2, n_id=1, n_code=1, n_metric=1)

        assert test_table.shape == (2, 4)

    def test___gen_col(self, client):
        test_col = client._DataGenerator__gen_col("id", 2)
        assert len(test_col) == 2
