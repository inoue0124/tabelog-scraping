import os
import datetime
import time
import requests
from bs4 import BeautifulSoup
import json
import boto3

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")


def scrape(url: str):
    html = BeautifulSoup(requests.get(url).content, "html5lib")
    rstinfo_table = html.find("div", class_="rstinfo-table")

    def filter_booking_inquiry(tag):
        if tag.name == 'th' and tag.get_text(strip=True) == '予約・お問い合わせ':
            return True
        return False

    def filter_business_hours(tag):
        if tag.name == 'th' and tag.get_text(strip=True) == '営業時間・定休日':
            return True
        return False

    def filter_service_charge(tag):
        if tag.name == 'th' and tag.get_text(strip=True) == 'サービス料・チャージ':
            return True
        return False

    def get_pr_comment():
        if html.find("div", class_="pr-comment") is None:
            return ""
        return '\n'.join([x for x in html.find("div", class_="pr-comment").stripped_strings])

    def get_data_from_table(key: str):
        # キーが存在しない場合には空文字
        if rstinfo_table.find("th", string=key) is None:
            return ""
        return '\n'.join([x for x in rstinfo_table.find("th", string=key).find_next("td").stripped_strings])

    def get_data_from_table_by_filter(filter):
        # キーが存在しない場合には空文字
        if rstinfo_table.find(filter) is None:
            return ""
        return '\n'.join([x for x in rstinfo_table.find(filter).find_next("td").stripped_strings])

    def get_sns_url(sns_name: str):
        # 公式アカウントキーが存在しない場合は空文字
        th_tag = rstinfo_table.find("th", string="公式アカウント")
        if th_tag is None:
            return ""
        # 該当のsnsアカウントが存在しない場合は空文字
        a_tag = th_tag.find_next("td").find(
            "a", class_=f"rstinfo-sns-{sns_name}")
        if a_tag is None:
            return ""
        return a_tag.get("href")

    return {
        "url": url,
        "name": str(html.find("h2", class_="display-name").find("span").get_text(strip=True)),
        "has_official_badge": html.find("p", class_="owner-badge__icon") is not None,
        "score": str(html.find("span", class_="rdheader-rating__score-val-dtl").string),
        "num_reviews": str(html.find("span", class_="rdheader-rating__review-target").find("em").string),
        "num_bookmarks": str(html.find("span", class_="rdheader-rating__hozon-target").find("em").string),
        "nearest_station": str(html.find("dl", class_="rdheader-subinfo__item--station").find("span", class_="linktree__parent-target-text").string if html.find("dl", class_="rdheader-subinfo__item--station") is not None else ""),
        "genre": str(rstinfo_table.find("th", string="ジャンル").find_next("td").find("span").string),
        "budget_dinner": str(html.find("p", class_="rdheader-budget__icon--dinner").find("a", class_="rdheader-budget__price-target").string),
        "budget_lunch": str(html.find("p", class_="rdheader-budget__icon--lunch").find("a", class_="rdheader-budget__price-target").string),
        "regular_holiday": str(html.find("dd", class_="rdheader-subinfo__closed-text").get_text(strip=True) if html.find("dd", class_="rdheader-subinfo__closed-text") is not None else ""),
        "is_serve_takeout": html.find("div", class_="rstdtl-takeout-info") is not None,
        "pr_title": str(html.find("h3", class_="pr-comment-title").string if html.find("h3", class_="pr-comment-title") else ""),
        "pr-comment": get_pr_comment(),
        "kodawari": '\n'.join([x.get_text(strip=True) for x in html.find_all("p", class_="rstdtl-top-kodawari__title")]),
        "hygiene": '、'.join([x.get_text(strip=True) for x in html.find("div", class_="rstdtl-hygiene").find_all("p", class_="rstdtl-hygiene__data")]) if html.find("div", class_="rstdtl-hygiene") else "",
        "top-course": '、'.join([x.text for x in html.find_all("span", class_="rstdtl-course-list__price-num")]),
        "coupon": '、'.join([x.text for x in html.find_all("p", class_="rstdtl-rstinfo-coupon__description")]),
        "booking_inquiry": get_data_from_table_by_filter(filter_booking_inquiry),
        "booking_availability": get_data_from_table(key="予約可否"),
        "address": str(rstinfo_table.find("th", string="住所").find_next("td").find("p", class_="rstinfo-table__address").text),
        "transportation": get_data_from_table(key="交通手段"),
        "business_hours": get_data_from_table_by_filter(filter_business_hours),
        "payment_method": get_data_from_table(key="支払い方法"),
        "service_charge": get_data_from_table_by_filter(filter_service_charge),
        "num_seat": get_data_from_table(key="席数"),
        "num_max_booking": get_data_from_table(key="最大予約可能人数"),
        "private_room": get_data_from_table(key="個室"),
        "charter": get_data_from_table(key="貸切"),
        "smoking": get_data_from_table(key="禁煙・喫煙"),
        "parking": get_data_from_table(key="駐車場"),
        "space_equipment": get_data_from_table(key="空間・設備"),
        "mobile_phone": get_data_from_table(key="携帯電話"),
        "course": get_data_from_table(key="コース"),
        "drink": get_data_from_table(key="ドリンク"),
        "cuisine": get_data_from_table(key="料理"),
        "go_to_eat": get_data_from_table(key="Go To Eat"),
        "scene": get_data_from_table(key="利用シーン"),
        "location": get_data_from_table(key="ロケーション"),
        "service": get_data_from_table(key="サービス"),
        "with_children": get_data_from_table(key="お子様連れ"),
        "homepage": get_data_from_table(key="ホームページ"),
        "twitter": get_sns_url(sns_name="twitter"),
        "instagram": get_sns_url(sns_name="instagram"),
        "facebook": get_sns_url(sns_name="facebook"),
        "opening_date": get_data_from_table(key="オープン日"),
        "telephone": get_data_from_table(key="電話番号"),
        "other": get_data_from_table(key="備考"),
        "has_google_ad": html.find("aside", class_="rstdtl-side-banner") is not None,
        "created_at": datetime.datetime.now().isoformat()
    }


def handler(event, context):
    table = dynamodb.Table(os.environ['DB_RST_DATA_TABLE'])
    # SQSのメッセージからスクレイプ対象のURLを取得
    for i, record in enumerate(event['Records']):
        url = json.loads(record["body"])["url"]
        # URLが空の場合は処理をスキップ
        if not url:
            continue
        use_cache: bool = json.loads(record['body'])["use_cache"]

        # use_cacheがTrueの場合、DynamoDBにデータがあれば処理をしない
        if use_cache:
            response = table.get_item(Key={"url": url})
            if 'Item' in response:
                continue

        # スクレイピング結果を取得してDynamoDBに保存
        item = scrape(url)
        table.put_item(Item=item)

        # 次のリクエストをする前に少し待機する
        if i != len(event['Records']) - 1:
            time.sleep(2)
    return
