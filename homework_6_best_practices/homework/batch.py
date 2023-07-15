#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os



def read_data(filename, categorical):

    if os.getenv('S3_ENDPOINT_URL') is None:
         df = pd.read_parquet(filename)
    else:
        options = {
            'client_kwargs': {
                'endpoint_url': os.getenv('S3_ENDPOINT_URL')
            }
        }

        df = pd.read_parquet('s3://nyc-duration/file.parquet', storage_options=options)

    
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def save_data(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred


    df_result.to_parquet(output_file, engine='pyarrow', index=False)

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def main(year, month):

    #input_file = get_input_path(year, month)
    #output_file = get_output_path(year, month)

    # input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    # output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_file = '/home/seacevedo/mlops-zoomcamp-homework/homework_6_best_practices/homework/tests/file.parquet'
    output_file = f'taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'


    with open('/home/seacevedo/mlops-zoomcamp-homework/homework_6_best_practices/homework/model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)


    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    print(df)


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print(y_pred)


    print('predicted mean duration:', y_pred.mean())
    print('predicted sum duration:', y_pred.sum())


    save_data(df, y_pred, output_file)


if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)
