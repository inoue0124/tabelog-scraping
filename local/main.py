import urllib.parse
import sys
import csv
import datetime
import time
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup


def create_request_session():
    # 5回リトライ、リトライ毎に待ち時間を1秒追加
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    return session


def get_url_info(input_rst_name: str):
    # 検索URLの生成
    query: str = urllib.parse.urlencode({'sw': input_rst_name})
    base_url: str = 'https://tabelog.com/rstLst/?'
    search_url: str = base_url + query

    session = create_request_session()
    html = BeautifulSoup(session.request(
        'GET', search_url, timeout=10).content, 'html5lib')

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


def scrape(url: str):
    session = create_request_session()
    html = BeautifulSoup(session.request(
        'GET', url, timeout=10).content, "html5lib")

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


def dump_to_csv(data: list, file_path: str):
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
                  "PRタイトル",
                  "PRコメント",
                  "こだわり",
                  "感染症対策",
                  "コース料金",
                  "クーポン",
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

    with open(file_path, 'w') as f:
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
                             "PRタイトル": da["pr_title"],
                             "PRコメント": da["pr-comment"],
                             "こだわり": da["kodawari"],
                             "感染症対策": da["hygiene"],
                             "コース料金": da["top-course"],
                             "クーポン": da["coupon"],
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


def main():
    # インプットの店名は行ごとになっていて、コマンドライン引数から与えられることを想定
    rst_names = open(sys.argv[1]).read().splitlines()
    output_file_path = f"scraping-result_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    scrape_data = []
    for i, rst_name in enumerate(rst_names):
        # 店舗詳細URLを取得
        try:
            url_info = get_url_info(input_rst_name=rst_name)
        except:
            print(f"network error occured for {rst_name}, skipping...")
            continue

        # 店舗詳細URLが取得できなかった時はスキップ
        target_url = url_info["url"]
        if not target_url:
            continue

        try:
            data = scrape(url=target_url)
        except:
            print(f"network error occured for {target_url}, skipping...")
            continue

        print(data)
        scrape_data.append(data)

        # # 次のリクエストをする前に少し待機する
        # if i != len(rst_names) - 1:
        #     time.sleep(2)

    # csvファイルにダンプ
    dump_to_csv(data=scrape_data, file_path=output_file_path)


if __name__ == "__main__":
    main()
