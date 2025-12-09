import urllib.request as req
from urllib.parse import urlencode
import json
import sys
from datetime import datetime, timedelta

# API 설정
API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo' # 1. 데이터셋 개방표준에 따른 입찰공고정보
# API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdScsbidInfo'   # 2. 데이터셋 개방표준에 따른 낙찰정보
# API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdCntrctInfo'     # 3. 데이터셋 개방표준에 따른 계약정보
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='


def check_data_count(start_date, end_date):
    """특정 기간의 데이터 개수 확인 (계약정보용)"""

    # 계약정보는 YYYYMMDD만 사용
    s = start_date[:8]
    e = end_date[:8]

    params = urlencode({
        'serviceKey': SERVICE_KEY,
        'pageNo': 1,
        'numOfRows': 1,  # 1개만 요청 (totalCount만 필요)
        'type': 'json',
        # 'bsnsDivCd': 5,             # 2. 데이터셋 개방표준에 따른 낙찰정보에서만 사용
        'bidNtceBgnDt': start_date, # 1. 데이터셋 개방표준에 따른 입찰공고정보에서만 사용
        'bidNtceEndDt': end_date,
        # 'opengBgnDt': start_date,   # 2. 데이터셋 개방표준에 따른 낙찰정보에서만 사용
        # 'opengEndDt': end_date,
        # 'cntrctCnclsBgnDate': s,      # 3. 데이터셋 개방표준에 따른 계약정보는 YYYYMMDD
        # 'cntrctCnclsEndDate': e
    })

    url = API_URL + '?' + params

    try:
        print("데이터 개수 확인 중")
        print(f"시작: {s}\n종료: {e}\n")

        request = req.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        with req.urlopen(request, timeout=30) as response:
            data = response.read()

        text = data.decode('utf-8', errors='replace')
        result = json.loads(text)

        # API 응답 확인
        header = result.get('response', {}).get('header', {})
        result_code = header.get('resultCode')

        if result_code not in ('', '00', None):
            print(f"API 오류: {header.get('resultMsg', 'Unknown error')}")
            return None

        # totalCount 추출
        body = result.get('response', {}).get('body', {})
        total_count = body.get('totalCount')

        if total_count is not None:
            total_count = int(total_count)
            print(f"기간 내 데이터: {total_count:,}개\n")

            # 예상 수집 시간 계산
            print(" 예상 수집 시간:")
            print(f"NUM_OF_ROWS=100  → {total_count // 100}페이지 = 약 {(total_count // 100) * 10 / 60:.1f}분")
            print(f"NUM_OF_ROWS=500  → {total_count // 500}페이지 = 약 {(total_count // 500) * 2:.1f}분")
            print(f"NUM_OF_ROWS=1000 → {total_count // 1000}페이지 = 약 {(total_count // 1000) * 1:.1f}분\n")

            return total_count
        else:
            print("totalCount를 찾을 수 없습니다.")
            print(f"응답 내용: {json.dumps(body, ensure_ascii=False, indent=2)}")
            return None

    except Exception as e:
        print(f"오류 발생: {str(e)}")
        return None


# 낙찰정보용 range 함수는 그대로 두되, 계약정보에서는 사용하지 않음
def check_data_count_range(start_date, end_date):
    """(낙찰정보 전용) 최대 7일을 고려해 기간을 잘라서 합산"""
    total = 0

    start_dt = datetime.strptime(start_date, "%Y%m%d%H%M")
    end_dt = datetime.strptime(end_date, "%Y%m%d%H%M")

    cur = start_dt
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=7) - timedelta(minutes=1), end_dt)

        s = cur.strftime("%Y%m%d%H%M")
        e = chunk_end.strftime("%Y%m%d%H%M")

        print("=" * 60)
        print(f"구간 요청: {s[:8]} {s[8:10]}:{s[10:]}  ~  {e[:8]} {e[8:10]}:{e[10:]}")
        cnt = check_data_count(s, e) or 0
        total += cnt

        cur = chunk_end + timedelta(minutes=1)

    print("=" * 60)
    print(f"전체 합계(total): {total:,}개")
    return total


def format_date(year, month, day, hour, minute):
    """날짜 포맷팅"""
    return (
        f"{year:04d}"
        f"{month:02d}"
        f"{day:02d}"
        f"{hour:02d}"
        f"{minute:02d}"
    )


def main():
    if len(sys.argv) == 3:
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]

        if len(start_date_str) != 8 or len(end_date_str) != 8:
            print("오류: 날짜는 YYYYMMDD 형식이어야 합니다!")
            print("예: python 데이터_개수_확인.py 20250401 20250430")
            sys.exit(1)

        start_year = int(start_date_str[0:4])
        start_month = int(start_date_str[4:6])
        start_day = int(start_date_str[6:8])

        end_year = int(end_date_str[0:4])
        end_month = int(end_date_str[4:6])
        end_day = int(end_date_str[6:8])

        START_DATE = format_date(start_year, start_month, start_day, 0, 0)
        END_DATE = format_date(end_year, end_month, end_day, 23, 59)

    else:
        print("사용 방법:")
        print("python 데이터_개수_확인.py 20250401 20250430")
        print("\n또는 코드 내 기본값 사용:\n")

        START_DATE = '202504010000'
        END_DATE = '202504302359'

    # 계약정보 → 단일 호출
    check_data_count(START_DATE, END_DATE)

    # 낙찰정보 사용할 때만 아래 사용 (지금은 주석)
    # check_data_count_range(START_DATE, END_DATE)


if __name__ == "__main__":
    main()
