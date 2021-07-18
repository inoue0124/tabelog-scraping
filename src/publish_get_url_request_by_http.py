import os
import json
import boto3

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

bucket_name = "tabelog-scraping-input"
key = "test.csv"


def handler(event, context):
    response = s3.get_object(Bucket=bucket_name, Key=key)
    rst_names = response['Body'].read().decode('utf-8').splitlines()

    for rst_name in rst_names:
        # SQSへレストランURL取得リクエストを追加
        sqs.send_message(
            QueueUrl=os.environ['GET_URL_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps({"name": rst_name}))
        )

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": "success"
    }
    return response
