import os
import time
import datetime
import requests
import urllib.parse
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client('sqs')


def get_url_info(input_rst_name: str):
    query: str = urllib.parse.urlencode({'sw': input_rst_name})
    base_url: str = 'https://tabelog.com/rstLst/?'
    search_url: str = base_url + query
    html = BeautifulSoup(requests.get(search_url).content, 'html5lib')

    # 検索結果の1ページ目に店名が完全一致の結果があればそれを使う
    if html.find("a", string=input_rst_name):
        name_tag = html.find("a", string=input_rst_name)
    # 店名が完全一致の結果がない場合は検索結果の一番上のものを取得
    else:
        name_tag = html.find("a", class_="list-rst__rst-name-target")

    return {"input_rst_name": input_rst_name,
            "rst_name": str(name_tag.string if name_tag else ""),
            "url": str(name_tag.get('href') if name_tag else ""),
            "is_matched_name": input_rst_name == str(name_tag.string if name_tag else ""),
            "created_at": datetime.datetime.now().isoformat()}


def handler(event, context):
    table = dynamodb.Table(os.environ['DB_RST_URL_TABLE'])
    # SQSのメッセージからレストラン名を取得
    for i, record in enumerate(event['Records']):
        input_rst_name: str = json.loads(record['body'])["name"]
        use_cache: bool = json.loads(record['body'])["use_cache"]

        # use_cacheがTrueの場合、DynamoDBにデータがあればそれを使う
        if use_cache:
            response = table.get_item(Key={"input_rst_name": input_rst_name})
            if 'Item' in response:
                item = response["Item"]
            else:
                # DynamoDBにデータが存在しなければ食べログサイトから取得してDynamoDBに保存
                item = get_url_info(input_rst_name)
                table.put_item(Item=item)
                # 次のリクエストをする前に少し待機する
                if i != len(event['Records']) - 1:
                    time.sleep(2)
        else:
            # 食べログサイトから取得してDynamoDBに保存
            item = get_url_info(input_rst_name)
            table.put_item(Item=item)
            # 次のリクエストをする前に少し待機する
            if i != len(event['Records']) - 1:
                time.sleep(2)

        # SQSへスクレイピングリクエストを追加
        request = {
            "url": item["url"],
            "use_cache": True
        }
        sqs.send_message(
            QueueUrl=os.environ['SCRAPE_REQUEST_SQS_URL'],
            DelaySeconds=0,
            MessageBody=(json.dumps(request))
        )
    return
