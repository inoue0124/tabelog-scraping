import os
import requests
import urllib.parse
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client('sqs')


def get_url(rst_name: str) -> str:
    query: str = urllib.parse.urlencode({'sw': rst_name})
    base_url: str = 'https://tabelog.com/rstLst/?'
    search_url: str = base_url + query
    html = BeautifulSoup(requests.get(search_url).content, 'html.parser')
    url_list = [x.get('href')
                for x in html.find_all('a', class_='list-rst__rst-name-target')]
    try:
        url = url_list[0]
        return url
    except:
        return None


def handler(event, context):
    # SQSのメッセージからレストラン名を取得
    for record in event['Records']:
        rst_name = json.loads(record['body'])["name"]
        url = get_url(rst_name)

        # DynamoDBに保存
        table = dynamodb.Table("TabelogRstUrl")
        item = {"url": url,
                "rst_name": rst_name}
        table.put_item(Item=item)

        # SQSへスクレイピングリクエストを追加
        sqs.send_message(
            QueueUrl=os.environ['SCRAPE_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps({"url": url}))
        )
    return
