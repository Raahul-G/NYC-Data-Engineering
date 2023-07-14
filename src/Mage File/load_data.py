import io
import pandas as pd
import requests
import pyarrow.parquet as pq
from io import BytesIO

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    url = 'https://storage.googleapis.com/uber_de_bucket/2020.parquet'
    response = requests.get(url)
    file_obj = BytesIO(response.content)
    
    return pd.DataFrame(pq.read_table(file_obj).to_pandas())


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
