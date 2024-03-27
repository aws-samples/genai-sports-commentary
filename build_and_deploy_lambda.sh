#!/bin/bash
set -e
cd lambda 
rm -rf kinesis-stream-processor.zip packages
#pip install --target ./packages dependencies/botocore-1.29.162-py3-none-any.whl dependencies/boto3-1.26.162-py3-none-any.whl 
pip install --target ./packages  urllib3==1.26.15 boto3
cd packages && zip -r ../kinesis-stream-processor.zip . 
cd .. && zip kinesis-stream-processor.zip lambda_function.py 
aws lambda update-function-code --function-name bedrock-play-by-play-commentary-processor --zip-file fileb://kinesis-stream-processor.zip
rm -rf kinesis-stream-processor.zip
