import os
import json
import boto3

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def handler(event, context):
    bucket = os.environ['S3_INPUT_BUCKET']
    key = "test.csv"

    response = s3.get_object(Bucket=bucket, Key=key)
    rst_names = response['Body'].read().decode('utf-8').splitlines()

    for rst_name in rst_names:
        # SQSへレストランURL取得リクエストを追加
        request = {
            "name": rst_name,
            "use_cache": False
        }
        sqs.send_message(
            QueueUrl=os.environ['GET_URL_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps(request))
        )

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": "success"
    }
    return response
