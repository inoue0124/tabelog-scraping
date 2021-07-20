import os
import json
import boto3

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    rst_names = response['Body'].read().decode('utf-8').splitlines()

    for rst_name in rst_names:
        # SQSへレストランURL取得リクエストを追加
        request = {
            "name": rst_name,
            "use_cache": True
        }
        sqs.send_message(
            QueueUrl=os.environ['GET_URL_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps(request))
        )
    return
