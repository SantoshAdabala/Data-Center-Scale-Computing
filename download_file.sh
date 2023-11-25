#!/bin/bash
mkdir -p /app/data
curl -o /app/data/shelter1000.csv https://shelterdata.s3.amazonaws.com/shelter1000.csv
python /app/pipeline.py
