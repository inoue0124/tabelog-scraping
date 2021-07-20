import os
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client('sqs')


def scan_all():
    table = dynamodb.Table(os.environ['DB_RST_URL_TABLE'])
    response = table.scan()
    data = response['Items']
    # レスポンスに LastEvaluatedKey が含まれなくなるまでループ処理を実行する
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def handler(event, context):
    # DynamoDBからURLを取得
    data = scan_all()

    for da in data:
        url = da['url']
        # SQSへスクレイピングリクエストを追加
        request = {
            "url": url,
            "use_cache": False
        }
        sqs.send_message(
            QueueUrl=os.environ['SCRAPE_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps(request))
        )

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": "success"
    }
    return response
