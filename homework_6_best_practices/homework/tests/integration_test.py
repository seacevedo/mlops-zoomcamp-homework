import pandas as pd
import os
from datetime import datetime
import sys
import os

def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)

def test_integration(input_file):
    
    options = {
        'client_kwargs': {
            'endpoint_url': os.getenv('S3_ENDPOINT_URL')
        }
    }



    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2), dt(1, 10)),
        (1, 2, dt(2, 2), dt(2, 3)),
        (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
    ]



    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_input = pd.DataFrame(data, columns=columns)

    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

    print(df_input)

if __name__ == '__main__':
    input_file = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    test_integration(input_file)
    os.system('python /home/seacevedo/mlops-zoomcamp-homework/homework_6_best_practices/homework/batch.py {0} {1}'.format(year, month))