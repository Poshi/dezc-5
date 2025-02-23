#!/usr/bin/env bash

# This script assumes that Python and Java 17 are installed

python3 -m venv .venv
source .venv/bin/activate

pip install pyspark jupyter

wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
