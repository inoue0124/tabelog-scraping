import csv
import boto3

s3 = boto3.resource('s3')
dynamodb = boto3.resource("dynamodb")

bucket_name = "tabelog-scraping-output"
key = "scraping-result.csv"
local_path = "/tmp/scraping-result.csv"
table_name = "TabelogRstData"


def get_data():
    table = dynamodb.Table("TabelogRstData")
    response = table.scan()
    data = response['Items']
    # レスポンスに LastEvaluatedKey が含まれなくなるまでループ処理を実行する
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def handler(event, context):
    # DynamoDBからデータを取得
    data = get_data()
    fieldnames = ['URL', '評価']

    # 取得データよりcsvファイルを作成
    with open(local_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for da in data:
            url = da['url']
            rating = da['rating']
            writer.writerow({'URL': url, '評価': rating})

    # S3にアップロード
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(local_path, key)

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": local_path
    }
    return response
