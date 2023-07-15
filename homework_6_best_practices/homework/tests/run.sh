#!/usr/bin/env bash


export S3_ENDPOINT_URL="https://localhost:4566"
export S3_BUCKET_NAME="s3://nyc-duration"
export YEAR=2022
export MONTH=1
export BUCKET_FILE='s3://nyc-duration/file.parquet/file.parquet'

docker-compose up -d

sleep 5

aws --endpoint-url=http://localhost:4566 \
    s3 \
    mb \
    ${S3_BUCKET_NAME}



pipenv run python integration_test.py ${BUCKET_FILE} ${YEAR} ${MONTH}

ERROR_CODE=$?

if [ ${ERROR_CODE} != 0 ]; then
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi


docker-compose down