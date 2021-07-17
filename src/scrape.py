import requests
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client('sqs')


def scrape(url: str):
    html = BeautifulSoup(requests.get(url).content, 'html.parser')
    return {
        "url": url,
        "rating": str(html.find('span', class_='rdheader-rating__score-val-dtl').string)
    }


def handler(event, context):
    # SQSのメッセージからスクレイプ対象のURLを取得
    for record in event['Records']:
        url = json.loads(record['body'])["url"]

        # DynamoDBに保存
        table = dynamodb.Table("TabelogRstData")
        item = scrape(url)
        table.put_item(Item=item)
    return
