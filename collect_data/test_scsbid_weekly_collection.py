import urllib.request as req
from urllib.parse import urlencode
import json
from datetime import datetime

API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdScsbidInfo'
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='


def log_msg(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")


def fetch_page(page_num, num_of_rows, start_date, end_date):
    """API에서 한 페이지 데이터 받기"""
    params = urlencode({
        'serviceKey': SERVICE_KEY,
        'pageNo': page_num,
        'numOfRows': num_of_rows,
        'type': 'json',
        'bsnsDivCd': 5,
        'opengBgnDt': start_date,
        'opengEndDt': end_date
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
        num_of_rows = body.get('numOfRows')
        page_no = body.get('pageNo')

        return items, total_count, num_of_rows, page_no, None
    except Exception as e:
        return None, None, None, None, str(e)


# 진단
start_date = "202510010000"
end_date = "202510072359"

log_msg("=" * 80)
log_msg(f"구간: {start_date[:8]} ~ {end_date[:8]}")
log_msg("=" * 80)

# 첫 페이지 조회
log_msg("\n【페이지 1 조회】")
items, total_count, num_rows, page_no, error = fetch_page(1, 15, start_date, end_date)

if error:
    log_msg(f"❌ 오류: {error}")
else:
    log_msg(f"✅ totalCount: {total_count}건")
    log_msg(f"   numOfRows: {num_rows}")
    log_msg(f"   pageNo: {page_no}")
    log_msg(f"   현재 페이지 데이터: {len(items)}개")

    # 필요한 페이지 계산
    import math

    required_pages = math.ceil(total_count / num_rows) if num_rows else 0
    log_msg(f"   필요한 페이지: {required_pages} (ceil({total_count}/{num_rows}))")

# 페이지별 상세 조회
log_msg("\n【페이지별 상세 조회】")
total_collected = 0

for page in range(1, 15):  # 최대 15페이지까지 조회
    items, total_count, num_rows, page_no, error = fetch_page(page, 15, start_date, end_date)

    if error:
        log_msg(f"Page {page}: ❌ 오류 - {error}")
        break

    if not items:
        log_msg(f"Page {page}: 빈 응답 (0개) → 종료")
        break

    total_collected += len(items)
    log_msg(f"Page {page}: {len(items):2d}개 (누적: {total_collected:3d}개)")

log_msg(f"\n최종 결과:")
log_msg(f"  totalCount: {total_count}건")
log_msg(f"  실제 수집: {total_collected}건")
log_msg(f"  차이: {total_count - total_collected}건")