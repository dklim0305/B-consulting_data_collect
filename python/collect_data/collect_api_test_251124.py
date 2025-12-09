import urllib.request as req
from urllib.parse import urlencode
import json
from datetime import datetime
import pandas as pd

API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo'
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='


def log_msg(msg):
    """로그 출력"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)


def fetch_page(page_num, num_of_rows, start_date, end_date):
    """API에서 한 페이지 데이터 받기"""
    params = urlencode({
        'serviceKey': SERVICE_KEY,
        'pageNo': page_num,
        'numOfRows': num_of_rows,
        'type': 'json',
        'bidNtceBgnDt': start_date,
        'bidNtceEndDt': end_date
    })
    url = API_URL + '?' + params

    try:
        request = req.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with req.urlopen(request, timeout=60) as response:
            data = response.read()

        result = json.loads(data.decode('utf-8', errors='replace'))
        body = result.get('response', {}).get('body', {})
        items = body.get('items', [])
        total_count = body.get('totalCount')

        return items, total_count, None
    except Exception as e:
        return None, None, str(e)


def main():
    """메인 프로그램"""

    start_date = "202507010000"
    end_date = "202507312359"
    num_of_rows = 15
    start_page = 1

    log_msg("7월 데이터 전체 수집 테스트")
    log_msg(f"기간: {start_date} ~ {end_date}")
    log_msg(f"시작 페이지: {start_page}")
    log_msg("")

    page = start_page
    total_data_count = 0
    api_total_count = None
    all_data = []

    while True:
        items, total_count, error = fetch_page(page, num_of_rows, start_date, end_date)

        if api_total_count is None and total_count is not None:
            api_total_count = int(total_count)
            log_msg(f"API 보고 총합: {api_total_count:,}건")
            log_msg("")

        if error:
            log_msg(f"오류: {error}")
            break

        if items is None:
            log_msg("응답 없음")
            break

        item_count = len(items)
        log_msg(f"Page {page}: {item_count}건 반환")

        if item_count == 0:
            log_msg("더 이상 데이터 없음")
            break

        total_data_count += item_count

        for item in items:
            all_data.append(item)

        page += 1

        import time
        time.sleep(0.5)

    log_msg("")
    log_msg(f"수집 완료")
    log_msg(f"총 수집 건수: {total_data_count:,}건")

    if api_total_count:
        log_msg(f"API 보고: {api_total_count:,}건")
        log_msg(f"차이: {api_total_count - total_data_count:,}건")

    log_msg("")
    log_msg(f"=== 중복 분석 ===")

    df = pd.DataFrame(all_data)

    total_rows = len(df)
    log_msg(f"merge 전 (중복 포함): {total_rows:,}건")

    unique_rows = df.drop_duplicates(subset=['bidNtceNo'], keep='first')
    unique_count = len(unique_rows)
    log_msg(f"merge 후 (중복 제거): {unique_count:,}건")

    duplicate_count = total_rows - unique_count
    log_msg(f"제거된 중복: {duplicate_count:,}건")

    if api_total_count:
        log_msg("")
        log_msg(f"=== 최종 비교 ===")
        log_msg(f"API 보고: {api_total_count:,}건")
        log_msg(f"실제 unique: {unique_count:,}건")
        log_msg(f"차이: {api_total_count - unique_count:,}건")


if __name__ == "__main__":
    main()