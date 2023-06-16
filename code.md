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

    url = 'https://storage.googleapis.com/uber_de_bucket/2020.parquet'
    response = requests.get(url, stream=True)
    file_obj = BytesIO(response.content)
    
    # Read Parquet file using ParquetFile class
    parquet_file = pq.ParquetFile(file_obj)
    num_rows = parquet_file.num_rows
    
    # Iterate over chunks of data
    data_frames = []
    for i in range(0, num_rows, batch_size):
        # Read a chunk of data
        chunk = parquet_file.read_row_group(i, i+batch_size).to_pandas()
        data_frames.append(chunk)
        
        # Print the appended data in each chunk
        print(f"Appended data in chunk {i//batch_size + 1}:")
        print(chunk)
    
    # Concatenate all the data frames
    return pd.concat(data_frames)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
