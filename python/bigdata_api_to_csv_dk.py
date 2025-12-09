import urllib.request as req
from urllib.parse import urlencode
import pandas as pd
import json
import time
import os
import glob
import re
from datetime import datetime
import sys
import math

# ============================================
# 설정
# ============================================

API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo'
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='

# 설정
NUM_OF_ROWS = 15  # 한 번에 받을 데이터 개수 (10~20 권장)
MAX_TOTAL_ROWS = 15000  # 한 번에 수집할 목표 건수 (매번 이 만큼씩 받음)

# API 명세서 기준
TPS_LIMIT = 30  # 초당 요청 제한
AVG_RESPONSE_TIME = 0.5  # 평균 응답시간: 500ms = 0.5초
REQUEST_INTERVAL = 1.0 / TPS_LIMIT + 0.01  # 요청 간격
DAILY_LIMIT = 10000  # 하루 최대 요청 횟수
TIMEOUT_SEC = 60
BATCH_SIZE = 5000  # 파트 파일 생성 기준

SAVE_DIR = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\data_set_all"


# ============================================
# 함수
# ============================================

def get_paths(start_date, end_date):
    """파일 경로 생성"""
    os.makedirs(SAVE_DIR, exist_ok=True)
    prefix = f"bigdata_{start_date}_{end_date}"
    return {
        'buffer': os.path.join(SAVE_DIR, f"{prefix}_buffer.jsonl"),
        'checkpoint': os.path.join(SAVE_DIR, f"{prefix}_ckpt.json"),
        'merged': os.path.join(SAVE_DIR, f"{prefix}_merged.csv"),
    }


def log_msg(msg):
    """로그 출력"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")


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
        with req.urlopen(request, timeout=TIMEOUT_SEC) as response:
            data = response.read()

        result = json.loads(data.decode('utf-8', errors='replace'))
        body = result.get('response', {}).get('body', {})
        items = body.get('items', [])
        total_count = body.get('totalCount')

        return items, total_count, None
    except Exception as e:
        return None, None, str(e)


def get_actual_part_num(prefix):
    """실제 존재하는 파트 파일 기반으로 다음 part_num 계산"""
    part_files = glob.glob(os.path.join(SAVE_DIR, f"{prefix}_part*.csv"))

    if not part_files:
        return 1  # 파트 파일이 없으면 1부터 시작

    # 파트 파일에서 숫자 추출 (part001.csv → 1)
    part_numbers = []
    for part_file in part_files:
        match = re.search(r'part(\d+)', part_file)
        if match:
            part_numbers.append(int(match.group(1)))

    if part_numbers:
        next_num = max(part_numbers) + 1
        log_msg(f"실제 파트 파일 확인: part001~part{max(part_numbers):03d} 존재")
        return next_num

    return 1


def get_total_count(start_date, end_date):
    """전체 데이터 개수 확인 (첫 호출에서만)"""
    items, total_count, error = fetch_page(1, 1, start_date, end_date)
    if error or total_count is None:
        return None
    return int(total_count)


def save_buffer(items, buffer_path):
    """데이터를 버퍼에 저장 (중복 제거)"""
    if not items:
        return 0, 0  # (요청한 개수, 저장한 개수)

    request_count = len(items)  # 실제로 받은 아이템 개수
    saved_count = 0
    seen = set()

    with open(buffer_path, "a", encoding="utf-8") as f:
        for item in items:
            bid_id = item.get('bidNtceNo', '')
            if bid_id and bid_id not in seen:
                seen.add(bid_id)
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                saved_count += 1

    return request_count, saved_count


def create_part_file(buffer_path, part_num, prefix):
    """버퍼를 파트 파일로 변환"""
    if not os.path.exists(buffer_path):
        return 0

    try:
        df = pd.read_json(buffer_path, lines=True, dtype=str)
        part_path = os.path.join(SAVE_DIR, f"{prefix}_part{part_num:03d}.csv")
        df.to_csv(part_path, index=False, encoding='utf-8-sig')
        row_count = len(df)
        log_msg(f"파트 파일 생성: part{part_num:03d}.csv ({row_count}행)")
        os.remove(buffer_path)
        return row_count
    except Exception as e:
        log_msg(f"파트 생성 실패: {str(e)}")
        return 0


def load_checkpoint(ckpt_path):
    """체크포인트 불러오기"""
    if os.path.exists(ckpt_path):
        try:
            return json.load(open(ckpt_path, 'r', encoding='utf-8'))
        except:
            pass
    return None


def save_checkpoint(ckpt_path, next_page, total_rows, request_count, part_num, buffer_count, total_count):
    """체크포인트 저장"""
    with open(ckpt_path, 'w', encoding='utf-8') as f:
        json.dump({
            'next_page': next_page,
            'total_rows': total_rows,  # 지금까지 누적한 총 건수
            'request_count': request_count,
            'part_num': part_num,
            'buffer_count': buffer_count,
            'total_count': total_count,  # 전체 데이터 개수 (변하지 않음)
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }, f, ensure_ascii=False, indent=2)


def merge_parts(prefix, target_count=None):
    """모든 파트 파일을 최종 파일로 병합"""
    part_files = sorted(glob.glob(os.path.join(SAVE_DIR, f"{prefix}_part*.csv")))

    if not part_files:
        return 0

    dataframes = []
    for part_file in part_files:
        df = pd.read_csv(part_file, dtype=str, encoding='utf-8-sig')
        dataframes.append(df)

    merged_df = pd.concat(dataframes, ignore_index=True)
    merged_df = merged_df.drop_duplicates()

    # 【초과분 제거 (target_count 지정 시)
    if target_count is not None and len(merged_df) > target_count:
        log_msg(f"초과분 제거: {len(merged_df)}건 → {target_count}건")
        merged_df = merged_df.iloc[:target_count]

    merged_path = os.path.join(SAVE_DIR, f"{prefix}_merged.csv")
    merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')

    log_msg(f"최종 병합: {merged_path} ({len(merged_df)}행)")

    return len(merged_df)


def create_summary(paths, total_rows, target_rows, total_count, elapsed_sec, reason, is_success, request_count,
                   current_page):
    """요약 파일 생성"""
    h, r = divmod(int(elapsed_sec), 3600)
    m, s = divmod(r, 60)
    elapsed_str = f"{h:02d}:{m:02d}:{s:02d}"

    if is_success and total_rows >= target_rows:
        status = "이번 목표 달성"
        detail = f"목표 건수({target_rows:,}건)를 달성했습니다.\n다음 실행 시 pageNo={current_page}부터 자동 재개됩니다."
    elif request_count >= DAILY_LIMIT:
        status = "⚠일일 제한 도달"
        detail = f"일일 요청 횟수({DAILY_LIMIT:,})에 도달했습니다.\n내일 같은 명령어를 실행하면 계속 수집됩니다."
    elif not is_success:
        status = "수집 중단"
        detail = f"오류 발생: {reason}\n다음 실행 시 pageNo={current_page}부터 재개됩니다."
    else:
        status = "모든 데이터 수집 완료"
        detail = f"API에 더 이상 데이터가 없습니다.\n다음에 더 받을 데이터가 없습니다."

    summary_text = f"""{status}
{'=' * 70}

【수집 결과】
완료 상태: {status}
소요 시간: {elapsed_str}
총 요청 횟수: {request_count:,}건
사용률: {(request_count / DAILY_LIMIT * 100):.1f}%

【데이터 통계】
이번 수집: {total_rows:,}건
전체 데이터: {total_count:,}건
수집률: {(total_rows / total_count * 100):.1f}%

{'=' * 70}

【상세 정보】
{detail}

마지막 pageNo: {current_page - 1}
다음 pageNo: {current_page}

{'=' * 70}
작성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    summary_path = os.path.join(SAVE_DIR, f"{os.path.basename(paths['merged']).replace('_merged.csv', '_summary.txt')}")
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        log_msg(f"요약 파일 생성: {summary_path}")
    except Exception as e:
        log_msg(f"요약 파일 생성 실패: {str(e)}")


# ============================================
# 메인
# ============================================

def main():
    # 1. 명령줄 입력
    if len(sys.argv) != 3:
        print("사용: python script.py 20250501 20250531")
        sys.exit(1)

    start_str, end_str = sys.argv[1], sys.argv[2]
    START_DATE = start_str + "0000"
    END_DATE = end_str + "2359"
    prefix = f"bigdata_{START_DATE}_{END_DATE}"

    # 2. 경로 및 체크포인트
    paths = get_paths(START_DATE, END_DATE)
    checkpoint = load_checkpoint(paths['checkpoint'])

    # 3. 전체 데이터 개수 확인 (첫 실행 또는 체크포인트 없을 때)
    if checkpoint:
        # 재개: 이전에 저장된 정보 로드
        total_count = checkpoint.get('total_count')  # 전체 데이터 (변함 없음)
        previous_total_rows = checkpoint['total_rows']  # 지금까지 받은 건수
        current_page = checkpoint['next_page']
        request_count = checkpoint['request_count']
        part_num_checkpoint = checkpoint['part_num']
        buffer_count = checkpoint['buffer_count']

        # 이번 실행의 목표: 이전 + MAX_TOTAL_ROWS
        target_rows = total_count
        data_start_num = previous_total_rows + 1

        # 실제 존재하는 파트 파일 확인 후 part_num 재계산
        actual_part_num = get_actual_part_num(prefix)
        if actual_part_num != part_num_checkpoint:
            log_msg(f"경고: checkpoint part_num={part_num_checkpoint} → 실제 파일 기반 part_num={actual_part_num}으로 수정")
            part_num = actual_part_num
        else:
            part_num = part_num_checkpoint

        log_msg(f"재개: {previous_total_rows:,}건 수집됨 (전체 {total_count:,}건)")
        log_msg(f"이번 목표: {target_rows:,}건까지 받기")
        log_msg(f"pageNo={current_page}, 요청={request_count}/{DAILY_LIMIT}, part={part_num}")
        log_msg(f"다음 데이터 범위: {data_start_num:,d}~")

        total_rows = previous_total_rows
    else:
        # 첫 실행: 전체 데이터 카운트
        log_msg("전체 데이터 개수 확인 중...")
        total_count = get_total_count(START_DATE, END_DATE)
        if total_count is None:
            log_msg("전체 데이터 개수를 가져올 수 없습니다")
            sys.exit(1)
        log_msg(f"기간 내 전체 데이터: {total_count:,}건")

        # 이번 실행의 목표: 0 + MAX_TOTAL_ROWS
        target_rows = total_count
        current_page = 1
        total_rows = 0
        request_count = 0
        part_num = 1
        buffer_count = 0
        data_start_num = 1

    # 4. 안전성 검사
    required_requests = math.ceil(MAX_TOTAL_ROWS / NUM_OF_ROWS)
    if required_requests > DAILY_LIMIT:
        log_msg(f"오류: 요청 횟수 {required_requests}번 > 제한 {DAILY_LIMIT}번")
        log_msg(f"MAX_TOTAL_ROWS를 {DAILY_LIMIT * NUM_OF_ROWS:,}건 이하로 설정하세요")
        sys.exit(1)

    # buffer 파일 상태 확인
    if os.path.exists(paths['buffer']):
        buffer_size = os.path.getsize(paths['buffer'])
        log_msg(f"기존 buffer 파일 발견: {buffer_size / 1024:.1f}KB (계속 사용됨)")

    if not checkpoint:
        log_msg(f"설정: NUM_OF_ROWS={NUM_OF_ROWS}, 이번 목표={MAX_TOTAL_ROWS:,}건")
        log_msg(f"필요 요청: {required_requests}번 / 제한: {DAILY_LIMIT}번")

        # 예상 소요시간 계산
        estimated_time = required_requests * (REQUEST_INTERVAL + AVG_RESPONSE_TIME)
        minutes = int(estimated_time // 60)
        seconds = int(estimated_time % 60)
        log_msg(f"예상 소요시간: 약 {minutes}분 {seconds}초 (응답시간 500ms 포함)")

    # 5. 데이터 수집 (target_rows까지만!)
    start_time = time.time()
    last_request_time = 0
    error_reason = None  # 오류 추적

    try:
        while total_rows < target_rows and request_count < DAILY_LIMIT:
            # TPS 제한
            elapsed = time.time() - last_request_time
            if elapsed < REQUEST_INTERVAL:
                time.sleep(REQUEST_INTERVAL - elapsed)

            last_request_time = time.time()
            request_count += 1

            # API 호출
            items, _, error = fetch_page(current_page, NUM_OF_ROWS, START_DATE, END_DATE)

            if error:
                log_msg(f"오류: {error}")
                error_reason = error
                break

            if not items:
                log_msg("모든 데이터 수집 완료 (API에 더 이상 데이터 없음)")
                break

            # 데이터 저장
            request_count_page, saved_count = save_buffer(items, paths['buffer'])
            total_rows += saved_count
            buffer_count += saved_count

            # 진행상황 출력
            data_end_num = data_start_num + saved_count - 1
            remaining = target_rows - total_rows

            log_msg(f"Page {current_page:5d} [요청 {request_count:5d}/{DAILY_LIMIT}] | "
                    f"데이터 {data_start_num:,d}~{data_end_num:,d} "
                    f"(요청:{request_count_page}개, 저장:{saved_count}개) | "
                    f"누적: {total_rows:,d}/{target_rows:,d}건 | 남은: {remaining:,d}건")

            # 다음 페이지를 위해 데이터 시작 번호 업데이트
            data_start_num = data_end_num + 1

            # 파트 파일 생성 (BATCH_SIZE 도달 시)
            if buffer_count >= BATCH_SIZE:
                create_part_file(paths['buffer'], part_num, prefix)
                part_num += 1
                buffer_count = 0

            # 체크포인트 저장 (total_rows = 지금까지 누적한 총 건수)
            save_checkpoint(paths['checkpoint'], current_page + 1, total_rows, request_count, part_num, buffer_count,
                            total_count)

            current_page += 1

        # 목표 달성 또는 제한 도달 확인
        if total_rows >= target_rows:
            log_msg(
                f"이번 목표 달성! ({total_rows:,}건 누적 / 전체 {total_count:,}건 중 {(total_rows / total_count * 100):.1f}%)")
        elif request_count >= DAILY_LIMIT:
            log_msg(f"일일 제한 도달 ({request_count}번) - {total_rows:,}건 누적됨")
            error_reason = "일일 트래픽 제한 도달"

    except Exception as e:
        log_msg(f"오류: {e}")
        error_reason = str(e)

    # 6. 파일 정리
    if buffer_count > 0 and os.path.exists(paths['buffer']):
        create_part_file(paths['buffer'], part_num, prefix)

    # 7. 파트 파일 병합
    merged_count = merge_parts(prefix, target_rows)

    # 8. 마무리
    elapsed = time.time() - start_time
    is_success = error_reason is None  # 오류가 없으면 성공

    # 요약 파일 생성
    create_summary(
        paths,
        total_rows,
        merged_count,
        total_count,
        elapsed,
        error_reason if error_reason else "정상 완료",
        is_success,
        request_count,
        current_page
    )

    log_msg(f"완료")
    log_msg(f"누적: {merged_count:,}건 / 전체: {total_count:,}건 ({(merged_count / total_count * 100):.1f}%)")
    log_msg(f"시간: {elapsed:.1f}초")
    log_msg(f"다음: pageNo={current_page}부터 시작 (다시 실행하면 자동 재개)")
    log_msg(f"파일: {paths['merged']}")


if __name__ == "__main__":
    main()