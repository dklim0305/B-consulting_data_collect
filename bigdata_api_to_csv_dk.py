import urllib.request as req
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
import pandas as pd
import json
import time
import os
import glob
import socket
from datetime import datetime
import sys

# ============================================
# API 운영 정보 설정
# ============================================

# API 기본 정보
API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo'
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='

# ============================================
# API 운영 명세서 참고 설정
# ============================================

# 메시지 크기
MAX_MESSAGE_SIZE = 8000  # bytes (기본값: 4000bytes)

# TPS(Transaction Per Second)
TPS_LIMIT = 60
INTERVAL_PER_REQUEST = 1.0 / TPS_LIMIT  # ≈ 0.0333초
SAFETY_INTERVAL = 0.02
REQUEST_INTERVAL = INTERVAL_PER_REQUEST + SAFETY_INTERVAL

# 응답시간
AVG_RESPONSE_TIME_MS = 500  # 평균 응답시간: 500ms
TIMEOUT_SEC = 30  # 타임아웃: 30초

# 【재시도 제약】
MAX_RETRIES = 7

# ============================================
# 일반 설정
# ============================================

NUM_OF_ROWS = 100  # 초기값: 한 번에 가져올 데이터 개수
BATCH_SIZE = 5000  # 파트 파일 생성 기준

# 저장 경로
SAVE_DIR = r"C:\Users\admin\Downloads"


# ============================================
# 파일/경로 관리
# ============================================

def get_file_paths(start_date, end_date):
    """START_DATE, END_DATE를 매개변수로 받음"""
    os.makedirs(SAVE_DIR, exist_ok=True)

    prefix = f"bigdata_api_{start_date}_{end_date}"

    return {
        'buffer': os.path.join(SAVE_DIR, f"{prefix}_buffer.jsonl"),
        'checkpoint': os.path.join(SAVE_DIR, f"{prefix}_ckpt.json"),
        'merged': os.path.join(SAVE_DIR, f"{prefix}_merged.csv"),
        'summary': os.path.join(SAVE_DIR, f"{prefix}_summary.txt"),
    }


# ============================================
# 로그
# ============================================

def log_message(msg, file_path=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)

    if file_path:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


# ============================================
# NUM_OF_ROWS 조정
# ============================================

def estimate_row_size(sample_data):
    if not sample_data:
        return 0

    # 첫 번째 항목을 JSON으로 변환해서 크기 측정
    json_str = json.dumps(sample_data[0], ensure_ascii=False)
    json_bytes = json_str.encode('utf-8')

    return len(json_bytes)


def optimize_num_of_rows(current_rows, response_size):
    """
    응답 크기에 따라 NUM_OF_ROWS를 동적으로 조정
    """
    if response_size <= MAX_MESSAGE_SIZE:
        # 정상 범위 내
        return current_rows, False

    # 초과했다면 비율만큼 감소
    excess_ratio = response_size / MAX_MESSAGE_SIZE
    new_rows = max(1, int(current_rows / excess_ratio * 0.9))  # 90%로 여유있게

    log_message(f"메시지 크기 초과 ({response_size}bytes > {MAX_MESSAGE_SIZE}bytes)")
    log_message(f"NUM_OF_ROWS 조정: {current_rows} → {new_rows}")

    return new_rows, True


# ============================================
# API 호출
# ============================================

def fetch_page(page_num, current_num_of_rows, start_date, end_date):
    params = urlencode({
        'serviceKey': SERVICE_KEY,
        'pageNo': page_num,
        'numOfRows': current_num_of_rows,
        'type': 'json',
        'bidNtceBgnDt': start_date,
        'bidNtceEndDt': end_date
    })
    url = API_URL + '?' + params

    try:
        # API 요청 보내기
        request = req.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        start_time = time.time()
        with req.urlopen(request, timeout=TIMEOUT_SEC) as response:
            data = response.read()
        elapsed_ms = (time.time() - start_time) * 1000
        response_size = len(data)  # 응답 크기 (bytes)

        # JSON 파싱
        text = data.decode('utf-8', errors='replace')
        result = json.loads(text)

        # API 응답 상태 확인
        header = result.get('response', {}).get('header', {})
        if header.get('resultCode') not in ('', '00', None):
            return None, f"API 응답 오류: {header}", current_num_of_rows, response_size

        log_message(f"✓ 페이지 {page_num}: {elapsed_ms:.0f}ms, 응답크기: {response_size}bytes")

        return result, None, current_num_of_rows, response_size

    except HTTPError as e:
        if e.code == 429 or e.code == 503:
            return None, "트래픽 초과 또는 서버 과부하", current_num_of_rows, 0
        else:
            return None, f"HTTP 오류 {e.code}", current_num_of_rows, 0
    except socket.timeout:
        return None, "네트워크 타임아웃", current_num_of_rows, 0
    except URLError as e:
        return None, f"네트워크 오류: {str(e)}", current_num_of_rows, 0
    except json.JSONDecodeError:
        return None, "JSON 파싱 실패", current_num_of_rows, 0
    except Exception as e:
        return None, f"예상치 못한 오류: {str(e)}", current_num_of_rows, 0


# ============================================
# 데이터 관리
# ============================================

def save_to_buffer(items, buffer_path):
    """
    받은 데이터를 임시 버퍼에 저장
    """
    if not items:
        return 0

    # 중복 제거
    seen = set()
    unique_items = []

    for item in items:
        bid_id = item.get('bidNtceNo', '')
        if bid_id and bid_id not in seen:
            seen.add(bid_id)
            unique_items.append(item)

    # 버퍼 파일에 저장
    with open(buffer_path, "a", encoding="utf-8") as f:
        for item in unique_items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return len(unique_items)


def create_part_file(buffer_path, part_num, output_dir, prefix):
    """
    버퍼를 CSV 파일로 변환
    """
    if not os.path.exists(buffer_path):
        return 0

    try:
        df = pd.read_json(buffer_path, lines=True, dtype=str)
        output_path = os.path.join(output_dir, f"{prefix}_part{part_num:03d}.csv")
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        row_count = len(df)
        log_message(f"파트 파일 생성: part{part_num:03d}.csv ({row_count}행)")

        os.remove(buffer_path)
        return row_count

    except Exception as e:
        log_message(f"파트 생성 실패: {str(e)}")
        return 0


# ============================================
# 체크포인트
# ============================================

def load_checkpoint(checkpoint_path):
    """
    이전 진행 상황 불러오기
    """
    if not os.path.exists(checkpoint_path):
        return None

    try:
        with open(checkpoint_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def save_checkpoint(checkpoint_path, next_page, part_num, buffer_count, total_rows, total_pages, current_num_of_rows,
                    status):
    """
    현재 진행 상황 저장
    """
    checkpoint = {
        'next_page': next_page,
        'part_num': part_num,
        'buffer_count': buffer_count,
        'total_rows': total_rows,
        'total_pages': total_pages,
        'current_num_of_rows': current_num_of_rows,
        'status': status,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        with open(checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ============================================
# 파일 병합
# ============================================

def merge_all_parts(output_dir, prefix, merged_path):
    """
    모든 파트 파일을 하나의 CSV로 합치기
    """
    part_files = sorted(glob.glob(os.path.join(output_dir, f"{prefix}_part*.csv")))

    if not part_files:
        return 0

    dataframes = []
    for part_file in part_files:
        df = pd.read_csv(part_file, dtype=str, encoding='utf-8-sig')
        dataframes.append(df)

    merged_df = pd.concat(dataframes, ignore_index=True)
    merged_df = merged_df.drop_duplicates()

    merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')

    log_message(f"최종 병합 완료: {merged_path} ({len(merged_df)}행)")

    return len(merged_df)


# ============================================
# 최종 요약
# ============================================

def create_summary(summary_path, total_rows, total_pages, elapsed_sec, reason, is_success, api_info):
    h, r = divmod(int(elapsed_sec), 3600)
    m, s = divmod(r, 60)
    elapsed_str = f"{h:02d}:{m:02d}:{s:02d}"

    if is_success:
        status = "수집 완료"
        detail = "모든 데이터를 성공적으로 수집했습니다."
    else:
        status = "수집 중단"
        detail = f"수집 중 문제가 발생했습니다:\n{reason}"

    summary_text = f"""{status}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

수집 소요 시간: {elapsed_str}
수집된 총 데이터: {total_rows:,}행
처리한 페이지: {total_pages}개

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【API 운영 정보】
최대 메시지: {api_info['max_msg']}bytes
평균 응답시간: {api_info['avg_response']}ms
TPS 제한: {api_info['tps']} tps
요청 간격: {api_info['interval']:.0f}ms
재시도 횟수: {api_info['max_retries']}회

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{detail}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
작성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        log_message(f"요약 파일 생성: {summary_path}")
    except Exception as e:
        log_message(f"요약 파일 생성 실패: {str(e)}")


# ============================================
# 메인 프로그램
# ============================================

def main():
    """
    입찰정보 데이터 수집 프로그램
    YYYYMMDD 형식 입력

    사용법: python my_script.py 20250401 20250430
    """
    # 1. 명령줄 인자 확인
    if len(sys.argv) != 3:
        print("\n【사용 방법】")
        print("python my_script.py 시작날짜 종료날짜")
        print("\n【예제】")
        print("python my_script.py 20250401 20250430")
        print("\n【설명】")
        print("시작: 2025년 4월 1일 (시간: 0000)")
        print("종료: 2025년 4월 30일 (시간: 2359)")
        print()
        sys.exit(1)

    # 2. 명령줄 인자 받기
    try:
        start_date_str = sys.argv[1]  # "20250401"
        end_date_str = sys.argv[2]  # "20250430"

        # 문자열 길이 확인
        if len(start_date_str) != 8 or len(end_date_str) != 8:
            print("❌ 오류: 날짜는 YYYYMMDD 형식이어야 합니다!")
            print("예: 20250401")
            sys.exit(1)

        # 문자 → 숫자 변환 및 유효성 확인
        start_year = int(start_date_str[0:4])
        start_month = int(start_date_str[4:6])
        start_day = int(start_date_str[6:8])

        end_year = int(end_date_str[0:4])
        end_month = int(end_date_str[4:6])
        end_day = int(end_date_str[6:8])

    except (ValueError, IndexError):
        print("❌ 오류: 날짜가 잘못되었습니다!")
        print("올바른 형식: 20250401")
        sys.exit(1)

    # 3. START_DATE, END_DATE 생성 (시간은 고정)
    START_DATE = f"{start_year:04d}{start_month:02d}{start_day:02d}0000"
    END_DATE = f"{end_year:04d}{end_month:02d}{end_day:02d}2359"

    # 4. 날짜 유효성 검사
    try:
        start = datetime(start_year, start_month, start_day)
        end = datetime(end_year, end_month, end_day)
        if start > end:
            print("❌ 오류: 시작일이 종료일보다 뒤입니다!")
            sys.exit(1)
    except ValueError as e:
        print(f"❌ 오류: 잘못된 날짜입니다 ({e})")
        sys.exit(1)

    # 5. 확인 메시지 출력
    print("\n" + "=" * 50)
    print("입찰정보 데이터 수집")
    print("=" * 50)
    print(f"시작: {START_DATE} (00:00)")
    print(f"종료: {END_DATE} (23:59)")
    print("=" * 50 + "\n")

    # prefix 정의
    prefix = f"bigdata_api_{START_DATE}_{END_DATE}"

    paths = get_file_paths(START_DATE, END_DATE)
    start_time = time.time()

    # ==================== 초기화 ====================
    log_message("=" * 50)
    log_message("데이터 수집 시작")
    log_message("=" * 50)
    log_message(f"API 설정: 최대메시지={MAX_MESSAGE_SIZE}bytes, TPS={TPS_LIMIT}, 재시도={MAX_RETRIES}회")

    # 체크포인트 확인
    checkpoint = load_checkpoint(paths['checkpoint'])

    if checkpoint:
        current_page = checkpoint['next_page']
        part_num = checkpoint['part_num']
        buffer_count = checkpoint['buffer_count']
        total_rows = checkpoint['total_rows']
        total_pages = checkpoint['total_pages']
        current_num_of_rows = checkpoint.get('current_num_of_rows', NUM_OF_ROWS)
        log_message(f"이전 진행상황에서 재개 (페이지 {current_page}부터, NUM_OF_ROWS={current_num_of_rows})")
    else:
        current_page = 1
        part_num = 1
        buffer_count = 0
        total_rows = 0
        total_pages = 0
        current_num_of_rows = NUM_OF_ROWS
        log_message("새로운 수집 시작")

    # ==================== 데이터 수집 ====================
    failure_reason = None
    request_count = 0  # TPS 모니터링용
    last_request_time = 0

    try:
        while True:
            # ========== TPS 제한 구현 ==========
            elapsed_since_last = time.time() - last_request_time
            if elapsed_since_last < REQUEST_INTERVAL:
                wait_time = REQUEST_INTERVAL - elapsed_since_last
                time.sleep(wait_time)

            last_request_time = time.time()
            request_count += 1

            # ========== 재시도 루프 ==========
            retry_count = 0
            data = None
            last_error = None

            while retry_count < MAX_RETRIES:
                data, error, adjusted_rows, response_size = fetch_page(
                    current_page, current_num_of_rows, START_DATE, END_DATE
                )

                if data:
                    # 성공
                    new_rows, was_adjusted = optimize_num_of_rows(current_num_of_rows, response_size)
                    if was_adjusted:
                        current_num_of_rows = new_rows

                    break
                else:
                    # 실패 - 재시도
                    retry_count += 1
                    last_error = error

                    if retry_count < MAX_RETRIES:
                        wait_sec = min(60, 2 ** (retry_count - 1))
                        log_message(f"{error} → {wait_sec}초 후 재시도 ({retry_count}/{MAX_RETRIES})")
                        time.sleep(wait_sec)

            # 7번 재시도 후에도 실패
            if not data:
                failure_reason = last_error or "알 수 없는 오류"
                log_message(f"페이지 {current_page}: 재시도 초과. 이유: {failure_reason}")
                break

            # ========== 데이터 처리 ==========
            items = data.get('response', {}).get('body', {}).get('items')

            if not items:
                log_message("모든 데이터 수집 완료")
                break

            # 버퍼에 저장
            added_count = save_to_buffer(items, paths['buffer'])
            total_rows += added_count
            total_pages += 1
            buffer_count += added_count

            log_message(f"→ 버퍼 상태: {buffer_count}/{BATCH_SIZE} ({added_count}개 추가)")

            # ========== 파트 파일 생성 ==========
            if buffer_count >= BATCH_SIZE:
                create_part_file(paths['buffer'], part_num, SAVE_DIR, prefix)
                part_num += 1
                buffer_count = 0

            # 체크포인트 저장
            save_checkpoint(paths['checkpoint'], current_page + 1, part_num,
                            buffer_count, total_rows, total_pages, current_num_of_rows, 'running')

            current_page += 1

    except Exception as e:
        failure_reason = f"예상치 못한 오류: {str(e)}"
        log_message(f"오류 발생: {failure_reason}")

    # ==================== 마무리 ====================

    # 버퍼에 남은 데이터도 파트로 저장
    if buffer_count > 0 and os.path.exists(paths['buffer']):
        create_part_file(paths['buffer'], part_num, SAVE_DIR, prefix)

    # 모든 파트 병합
    merged_rows = merge_all_parts(SAVE_DIR, prefix, paths['merged'])

    # 최종 요약 생성
    elapsed_sec = time.time() - start_time
    is_success = failure_reason is None

    api_info = {
        'max_msg': MAX_MESSAGE_SIZE,
        'avg_response': AVG_RESPONSE_TIME_MS,
        'tps': TPS_LIMIT,
        'interval': REQUEST_INTERVAL * 1000,  # ms로 변환
        'max_retries': MAX_RETRIES
    }

    create_summary(
        paths['summary'],
        total_rows,
        total_pages,
        elapsed_sec,
        failure_reason or "정상 완료",
        is_success,
        api_info
    )

    # 최종 상태 저장
    save_checkpoint(paths['checkpoint'], current_page, part_num,
                    buffer_count, total_rows, total_pages, current_num_of_rows,
                    'completed' if is_success else 'failed')

    log_message("=" * 50)
    log_message(f"프로그램 종료 (총 요청: {request_count}건)")
    log_message("=" * 50)


# ============================================
# 프로그램 시작
# ============================================

if __name__ == "__main__":
    main()
