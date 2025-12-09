import urllib.request as req
from urllib.parse import urlencode
import pandas as pd
import json
import time
import os
import glob
import sys
from datetime import datetime
import calendar


# 설정
API_URL = 'https://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo'
SERVICE_KEY = 'GkdwpZIMCMx6y3h8SR+VRHft47yi6WPAG1ugNGWTsNVfXtdcZriNYv4C/ufr11gzctGilPi8rFm6O03M51qT0w=='

BASE_DIR = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\data_set_all"
NUM_OF_ROWS = 15
TPS_LIMIT = 30
REQUEST_INTERVAL = 1.0 / TPS_LIMIT + 0.01
DAILY_LIMIT = 10000
BATCH_SIZE = 5000

# 기본 함수
def log_msg(msg):
    """로그 출력"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")


def get_last_day(year, month):
    """해당 월의 마지막 일"""
    return calendar.monthrange(year, month)[1]


def get_prev_month(year, month):
    """이전 월 반환"""
    if month == 1:
        return year - 1, 12
    return year, month - 1


def format_date(year, month, day, time_part="0000"):
    """날짜 포맷: YYYYMMDDHHSS"""
    return f"{year:04d}{month:02d}{day:02d}{time_part}"


# 폴더/경로 관리
def get_folder_path(year, month):
    """월별 폴더 경로"""
    folder_name = f"public_procurement_{year:04d}{month:02d}"
    path = os.path.join(BASE_DIR, folder_name)
    os.makedirs(path, exist_ok=True)
    return path


def get_prefix(year, month):
    """파일 프리픽스"""
    last_day = get_last_day(year, month)
    start = format_date(year, month, 1, "0000")
    end = format_date(year, month, last_day, "2359")
    return f"bigdata_{start}_{end}"


def get_file_paths(year, month):
    """필요한 모든 파일 경로"""
    folder = get_folder_path(year, month)
    prefix = get_prefix(year, month)
    return {
        'folder': folder,
        'buffer': os.path.join(folder, f"{prefix}_buffer.jsonl"),
        'checkpoint': os.path.join(folder, f"{prefix}_ckpt.json"),
        'merged': os.path.join(folder, f"{prefix}_merged.csv"),
        'summary': os.path.join(folder, f"{prefix}_summary.txt"),
        'log': os.path.join(folder, "execution_log.txt"),
    }

# 상태 관리
def load_status():
    """상태 파일 읽기"""
    status_file = os.path.join(BASE_DIR, "status_8month.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None


def save_status(year, month):
    """상태 파일 저장"""
    status_file = os.path.join(BASE_DIR, "status_8month.json")
    status = {
        "start_year": year,
        "start_month": month,
        "current_year": year,
        "current_month": month,
        "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(status_file, 'w', encoding='utf-8') as f:
        json.dump(status, f, ensure_ascii=False, indent=2)


# API 통신
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


def get_total_count(start_date, end_date):
    """전체 데이터 개수"""
    items, total_count, error = fetch_page(1, 1, start_date, end_date)
    if error or total_count is None:
        return None
    return int(total_count)


# 데이터 처리
def save_buffer(items, buffer_path, current_total, target_total, seen):
    """버퍼에 데이터 저장 (중복 제거, 초과 방지)"""
    if not items:
        return 0, 0

    request_count = len(items)
    saved_count = 0

    # 이미 수집한 건수 + 현재 추가할 건수가 target을 초과하지 않도록 제한
    remaining = target_total - current_total

    with open(buffer_path, "a", encoding="utf-8") as f:
        for item in items:

            # target에 도달하면 중단
            if saved_count >= remaining:
                break
            bid_id = item.get('bidNtceNo', '')
            if bid_id and bid_id not in seen:
                seen.add(bid_id)
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                saved_count += 1

    return request_count, saved_count


def create_part_file(buffer_path, part_num, prefix, folder):
    """버퍼를 파트 파일로 변환"""
    if not os.path.exists(buffer_path):
        return 0

    try:
        df = pd.read_json(buffer_path, lines=True, dtype=str)
        part_path = os.path.join(folder, f"{prefix}_part{part_num:03d}.csv")
        df.to_csv(part_path, index=False, encoding='utf-8-sig')
        row_count = len(df)
        log_msg(f"파트 파일 생성: part{part_num:03d}.csv ({row_count}행)")
        os.remove(buffer_path)
        return row_count
    except Exception as e:
        log_msg(f"파트 생성 실패: {str(e)}")
        return 0


def merge_parts(prefix, folder, target_count):
    """모든 파트 파일 병합"""
    part_files = sorted(glob.glob(os.path.join(folder, f"{prefix}_part*.csv")))

    if not part_files:
        return 0

    dataframes = []
    for part_file in part_files:
        df = pd.read_csv(part_file, dtype=str, encoding='utf-8-sig')
        dataframes.append(df)

    merged_df = pd.concat(dataframes, ignore_index=True)

    before_dedup = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['bidNtceNo'], keep='first')
    after_dedup = len(merged_df)

    if before_dedup > after_dedup:
        log_msg(f"중복 제거: {before_dedup}건 → {after_dedup}건 ({before_dedup - after_dedup}건 제거)")

    # target_count 만큼만 유지
    if len(merged_df) > target_count:
        log_msg(f"초과분 제거: {len(merged_df)}건 → {target_count}건")
        merged_df = merged_df.head(target_count)

    merged_path = os.path.join(folder, f"{prefix}_merged.csv")
    merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')

    log_msg(f"최종 병합: {os.path.basename(merged_path)} ({len(merged_df)}행)")
    return len(merged_df)

# 체크포인트 관리
def load_checkpoint(ckpt_path):
    """체크포인트 읽기"""
    if os.path.exists(ckpt_path):
        try:
            with open(ckpt_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None


def save_checkpoint(ckpt_path, page, rows, requests, part_num, buffer_count, total_count):
    """체크포인트 저장"""
    data = {
        'next_page': page,
        'total_rows': rows,
        'request_count': requests,
        'part_num': part_num,
        'buffer_count': buffer_count,
        'total_count': total_count,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(ckpt_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def validate_data(merged_count, total_count):
    """데이터 검증 - 수집한 데이터가 API 보고 수량과 일치하는지 확인"""
    if merged_count == total_count:
        log_msg(f"데이터 검증 성공: {merged_count:,}건 == {total_count:,}건")
        return True
    else:
        log_msg(f"데이터 검증 실패")
        log_msg(f"예상: {total_count:,}건")
        log_msg(f"실제: {merged_count:,}건")
        log_msg(f"차이: {abs(merged_count - total_count):,}건")
        return False



# 로깅
def write_summary(summary_path, merged_count, total_count, elapsed_sec, error_reason,
                  is_success, request_count, current_page):
    """요약 파일 작성"""
    h, r = divmod(int(elapsed_sec), 3600)
    m, s = divmod(r, 60)
    elapsed_str = f"{h:02d}:{m:02d}:{s:02d}"

    if is_success and merged_count >= total_count:
        status = "목표 달성"
    elif request_count >= DAILY_LIMIT:
        status = "일일 제한 도달"
    elif not is_success:
        status = "수집 중단"
    else:
        status = "진행 중"

    text = f"""{'=' * 70}
{status}
{'=' * 70}

【수집 결과】
상태: {status}
시간: {elapsed_str}
요청: {request_count:,}건 / {DAILY_LIMIT:,}건 ({request_count / DAILY_LIMIT * 100:.1f}%)

【데이터 통계】
수집: {merged_count:,}건
전체: {total_count:,}건
완성도: {merged_count / total_count * 100:.1f}%

마지막 page: {current_page - 1}
다음 page: {current_page}

{'=' * 70}
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(text)


def write_execution_log(log_path, year, month, status, elapsed_sec, merged_count, total_count, reason):
    """실행 이력 로그 추가"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    text = f"""
[{timestamp}] 실행: {year:04d}-{month:02d}
[{timestamp}] 상태: {status}
[{timestamp}] 수집: {merged_count:,}건 / {total_count:,}건 ({merged_count / total_count * 100:.1f}%)
[{timestamp}] 시간: {int(elapsed_sec)}초
[{timestamp}] 이유: {reason}
"""

    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(text)


# 메인 로직
def main():
    """메인 프로그램"""

    # 1. 파라미터 처리
    if len(sys.argv) >= 3 and sys.argv[1] == '--init':
        init_str = sys.argv[2]
        if len(init_str) != 6:
            print("사용: python bigdata_api_program.py --init 202510")
            sys.exit(1)
        year, month = int(init_str[:4]), int(init_str[4:6])
        save_status(year, month)
        log_msg(f"초기화: {year}년 {month}월부터 시작")
    else:
        status = load_status()
        if status is None:
            print("첫 실행: python bigdata_api_program.py --init 202510")
            sys.exit(1)
        year, month = status['current_year'], status['current_month']
        log_msg(f"재개: {year}년 {month}월")

    # 2. 경로 설정
    paths = get_file_paths(year, month)
    last_day = get_last_day(year, month)
    start_date = format_date(year, month, 1, "0000")
    end_date = format_date(year, month, last_day, "2359")
    prefix = get_prefix(year, month)

    log_msg(f"기간: {year}년 {month}월 ({start_date} ~ {end_date})")

    # 3. 전체 데이터 개수 확인
    log_msg("전체 데이터 확인 중")
    total_count = get_total_count(start_date, end_date)
    if total_count is None:
        log_msg("데이터를 가져올 수 없습니다")
        sys.exit(1)
    log_msg(f"전체 데이터: {total_count:,}건")

    # 4. 체크포인트 확인
    seen_this_month = set()
    checkpoint = load_checkpoint(paths['checkpoint'])
    if checkpoint:
        current_page = checkpoint['next_page']
        total_rows = checkpoint['total_rows']
        request_count = checkpoint['request_count']
        part_num = checkpoint['part_num']
        buffer_count = checkpoint['buffer_count']
        log_msg(f"재개: {total_rows:,}건 수집됨 (page {current_page}부터)")
    else:
        current_page = 1
        total_rows = 0
        request_count = 0
        part_num = 1
        buffer_count = 0

    target_rows = total_count
    data_start_num = total_rows + 1

    # 5. 데이터 수집
    start_time = time.time()
    last_request_time = 0
    error_reason = None

    log_msg(f"수집 시작 (목표: {target_rows:,}건)")

    try:
        while total_rows < target_rows and request_count < DAILY_LIMIT:
            # TPS 제한
            elapsed = time.time() - last_request_time
            if elapsed < REQUEST_INTERVAL:
                time.sleep(REQUEST_INTERVAL - elapsed)

            last_request_time = time.time()
            request_count += 1

            # API 호출
            items, _, error = fetch_page(current_page, NUM_OF_ROWS, start_date, end_date)

            if error:
                log_msg(f"오류: {error}")
                error_reason = error
                break

            if not items:
                log_msg("모든 데이터 수집 완료")
                break

            # 데이터 저장
            req_count, saved_count = save_buffer(items, paths['buffer'], total_rows, target_rows, seen_this_month)

            if saved_count == 0 and total_rows < target_rows:
                log_msg(f"경고: 수집 불가 상태 (API 응답 없음)")
                log_msg(f"수집됨: {total_rows:,}건 / 목표: {target_rows:,}건")
                error_reason = "API 응답 한계"
                break

            total_rows += saved_count
            buffer_count += saved_count

            # 진행상황
            data_end_num = data_start_num + saved_count - 1
            remaining = target_rows - total_rows

            log_msg(f"Page {current_page:5d} [요청 {request_count:5d}/{DAILY_LIMIT}] | "
                    f"데이터 {data_start_num:,d}~{data_end_num:,d} | "
                    f"누적: {total_rows:,d}/{target_rows:,d}건 | 남은: {remaining:,d}건")

            data_start_num = data_end_num + 1

            # 파트 파일 생성
            if buffer_count >= BATCH_SIZE:
                create_part_file(paths['buffer'], part_num, prefix, paths['folder'])
                part_num += 1
                buffer_count = 0

            # 체크포인트 저장
            save_checkpoint(paths['checkpoint'], current_page + 1, total_rows, request_count,
                            part_num, buffer_count, total_count)

            current_page += 1

    except Exception as e:
        log_msg(f"오류: {e}")
        error_reason = str(e)

    # 6. 파일 정리
    if buffer_count > 0 and os.path.exists(paths['buffer']):
        create_part_file(paths['buffer'], part_num, prefix, paths['folder'])

    # 7. 병합
    merged_count = merge_parts(prefix, paths['folder'], target_rows)

    # 8. 검증
    is_success = (error_reason is None)
    validate_data(merged_count, total_count)

    # 9. 로깅
    elapsed = time.time() - start_time
    status_msg = "완료" if is_success else "중단"

    write_summary(paths['summary'], merged_count, total_count, elapsed, error_reason or "정상",
                  is_success, request_count, current_page)
    write_execution_log(paths['log'], year, month, status_msg, elapsed, merged_count,
                        total_count, error_reason or "정상")

    log_msg(f"{status_msg} - {int(elapsed)}초")

    # 10. 다음 월 준비 (완료 시에만)
    if is_success and merged_count >= total_count:
        next_year, next_month = get_prev_month(year, month)
        save_status(next_year, next_month)
        log_msg(f"다음: {next_year}년 {next_month}월")

        # 체크포인트 삭제 (다음 월을 위해)
        if os.path.exists(paths['checkpoint']):
            os.remove(paths['checkpoint'])


if __name__ == "__main__":
    main()