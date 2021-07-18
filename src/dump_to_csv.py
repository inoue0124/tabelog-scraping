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
    fieldnames = ["URL",
                  "店名",
                  "公式マーク有無",
                  "食べログスコア",
                  "レビュー数",
                  "ブックマーク数",
                  "最寄り駅",
                  "ジャンル",
                  "予算（昼）",
                  "予算（夜）",
                  "定休日",
                  "テイクアウト実施有無",
                  "予約・問い合わせ",
                  "予約可否",
                  "住所",
                  "交通手段",
                  "営業時間・定休日",
                  "支払い方法",
                  "サービス料・チャージ",
                  "席数",
                  "最大予約可能人数",
                  "個室",
                  "貸切",
                  "喫煙・禁煙",
                  "駐車場",
                  "空間・設備",
                  "携帯電話",
                  "コース",
                  "ドリンク",
                  "料理",
                  "Go to Eat",
                  "利用シーン",
                  "ロケーション",
                  "サービス",
                  "お子様連れ",
                  "ホームページ",
                  "公式アカウント（Twitter）",
                  "公式アカウント（Instagram）",
                  "公式アカウント（Facebook）",
                  "オープン日",
                  "電話番号",
                  "備考",
                  "Google広告表示有無"]

    # 取得データよりcsvファイルを作成
    with open(local_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for da in data:
            writer.writerow({"URL": da["url"],
                             "店名": da["name"],
                             "公式マーク有無": da["has_official_badge"],
                             "食べログスコア": da["score"],
                             "レビュー数": da["num_reviews"],
                             "ブックマーク数": da["num_bookmarks"],
                             "最寄り駅": da["nearest_station"],
                             "ジャンル": da["genre"],
                             "予算（昼）": da["budget_dinner"],
                             "予算（夜）": da["budget_lunch"],
                             "定休日": da["regular_holiday"],
                             "テイクアウト実施有無": da["is_serve_takeout"],
                             "予約・問い合わせ": da["booking_inquiry"],
                             "予約可否": da["booking_availability"],
                             "住所": da["address"],
                             "交通手段": da["transportation"],
                             "営業時間・定休日": da["business_hours"],
                             "支払い方法": da["payment_method"],
                             "サービス料・チャージ": da["service_charge"],
                             "席数": da["num_seat"],
                             "最大予約可能人数": da["num_max_booking"],
                             "個室": da["private_room"],
                             "貸切": da["charter"],
                             "喫煙・禁煙": da["smoking"],
                             "駐車場": da["parking"],
                             "空間・設備": da["space_equipment"],
                             "携帯電話": da["mobile_phone"],
                             "コース": da["course"],
                             "ドリンク": da["drink"],
                             "料理": da["cuisine"],
                             "Go to Eat": da["go_to_eat"],
                             "利用シーン": da["scene"],
                             "ロケーション": da["location"],
                             "サービス": da["service"],
                             "お子様連れ": da["with_children"],
                             "ホームページ": da["homepage"],
                             "公式アカウント（Twitter）": da["twitter"],
                             "公式アカウント（Instagram）": da["instagram"],
                             "公式アカウント（Facebook）": da["facebook"],
                             "オープン日": da["opening_date"],
                             "電話番号": da["telephone"],
                             "備考": da["other"],
                             "Google広告表示有無": da["has_google_ad"]})

    # S3にアップロード
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(local_path, key)

    # httpレスポンス
    response = {
        "statusCode": 200,
        "body": local_path
    }
    return response
