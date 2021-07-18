import os
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client('sqs')


def get_data():
    table = dynamodb.Table("TabelogRstUrl")
    response = table.scan()
    data = response['Items']
    # レスポンスに LastEvaluatedKey が含まれなくなるまでループ処理を実行する
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def handler(event, context):
    # DynamoDBからURLを取得
    data = get_data()

    for da in data:
        url = da['url']
        # SQSへスクレイピングリクエストを追加
        sqs.send_message(
            QueueUrl=os.environ['SCRAPE_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps({"url": url}))
        )

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": "success"
    }
    return response
