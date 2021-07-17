import os
import json
import boto3

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def publish_get_url_request(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    rst_names = response['Body'].read().decode('utf-8').splitlines()

    for rst_name in rst_names:
        # SQSへレストランURL取得リクエストを追加
        response = sqs.send_message(
            QueueUrl=os.environ['SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps({"name": rst_name}))
        )
    return
