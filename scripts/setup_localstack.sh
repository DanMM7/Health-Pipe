#!/bin/bash
set -e

echo "Waiting for LocalStack to be ready..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3":"available"'; do
  sleep 2
done

echo "Creating S3 buckets..."
awslocal s3 mb s3://county-raw-data
awslocal s3 mb s3://county-staging
awslocal s3 mb s3://county-processed
awslocal s3 mb s3://county-quarantine

echo "Listing buckets:"
awslocal s3 ls

echo "Creating IAM role for Lambda..."
awslocal iam create-role \
  --role-name lambda-execution-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

echo "Creating Lambda function..."
awslocal lambda create-function \
  --function-name clean-csv \
  --runtime python3.11 \
  --role arn:aws:iam::000000000000:role/lambda-execution-role \
  --handler clean_csv.lambda_handler \
  --code S3Bucket="lambda-code",S3Key="clean_csv.zip" \
  --timeout 300 \
  --memory-size 512

echo "LocalStack setup complete!"