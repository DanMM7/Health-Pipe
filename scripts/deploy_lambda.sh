#!/bin/bash
cd lambda
zip -r clean_csv.zip clean_csv.py
awslocal s3 cp clean_csv.zip s3://lambda-code/
awslocal lambda update-function-code \
  --function-name clean-csv \
  --s3-bucket lambda-code \
  --s3-key clean_csv.zip