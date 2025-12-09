import json
import time
import os
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

# 선택: 엑셀 저장용
import pandas as pd

# ---------------- 설정 ----------------
KEY = "41b2f29c22b92f1f912e4c80a33fbf1f62ebb4ce"  # 오픈DART 인증키
INPUT_JSON = r"C:\Users\admin\Desktop\오픈다트 회사 정보 관련 데이터 셋\corpCode.json"
SAVE_DIR = r"C:\Users\admin\Desktop\오픈다트 회사 정보 관련 데이터 셋\변환 데이터"

BATCH_SIZE = 100               # 배치 크기(저장 갯수 의미)
DELAY_SEC_EACH = 0.3          # 개별 호출 간 대기
DELAY_SEC_BETWEEN_BATCH = 1.5 # 배치 사이 대기
MAX_RETRY_STATUS020 = 5       # 020(요청 제한) 재시도 횟수
STATUS020_BASE_WAIT = 5       # 020 최초 대기(초), 이후 2^n 배
CHECKPOINT_PATH = str(Path(SAVE_DIR) / "company_checkpoint.txt")
# =========================================

BASE_URL = "https://opendart.fss.or.kr/api/company.json"

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) requests",
        "Accept": "application/json",
        "Connection": "close",
    })
    return s

def load_corp_codes(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    seen = set()
    codes = []
    for rec in data:
        c = (rec.get("corp_code") or "").strip()
        if c and c not in seen:
            seen.add(c)
            codes.append(c)
    return codes

def read_checkpoint(default_start=0):
    if not os.path.exists(CHECKPOINT_PATH):
        return default_start
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            idx = int(f.read().strip())
            return max(default_start, idx)
    except:
        return default_start

def write_checkpoint(idx):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        f.write(str(idx))

def fetch_company_info(sess, corp_code):
    params = {"crtfc_key": KEY, "corp_code": corp_code}
    r = sess.get(BASE_URL, params=params, timeout=(10, 60))
    r.raise_for_status()
    return r.json()

def handle_status020_retry(sess, corp_code):
    # 020(요청 제한 초과) 재시도: 지수 백오프
    for attempt in range(MAX_RETRY_STATUS020):
        wait = STATUS020_BASE_WAIT * (2 ** attempt)
        time.sleep(wait)
        try:
            data = fetch_company_info(sess, corp_code)
            if str(data.get("status", "")) != "020":
                return data
        except Exception:
            pass
    # 최종 실패 시 마지막 응답 반환 시도
    try:
        return fetch_company_info(sess, corp_code)
    except Exception as e:
        return {"status": "999", "message": f"retry_failed: {repr(e)}"}

def process_batch(sess, codes, start_idx, end_idx, save_dir):
    slice_codes = codes[start_idx:end_idx]
    results, failures = [], []

    for i, code in enumerate(slice_codes, 1):
        try:
            data = fetch_company_info(sess, code)
            status = str(data.get("status", ""))

            if status == "020":
                data = handle_status020_retry(sess, code)
                status = str(data.get("status", ""))

            if status and status != "000":
                failures.append({
                    "corp_code": code,
                    "status": status,
                    "message": data.get("message", ""),
                    "raw": data
                })
            else:
                data["corp_code"] = code
                results.append(data)
        except Exception as e:
            failures.append({"corp_code": code, "error": repr(e)})

        time.sleep(DELAY_SEC_EACH)

    # 저장
    n1 = start_idx + 1
    n2 = end_idx
    base = f"company_{n1:06d}_{n2:06d}"
    out_json = str(Path(save_dir) / f"{base}.json")
    out_xlsx = str(Path(save_dir) / f"{base}.xlsx")
    out_fail = str(Path(save_dir) / f"{base}_failures.json")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    if failures:
        with open(out_fail, "w", encoding="utf-8") as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)

    if results:
        df = pd.DataFrame(results)
        preferred_cols = [
            "corp_code", "corp_name", "corp_name_eng", "stock_code",
            "ceo_nm", "corp_cls", "jurir_no", "bizr_no",
            "adres", "hm_url", "ir_url", "phn_no", "fax_no",
            "est_dt", "acc_mt", "induty_code", "industry_cls"
        ]
        df = df[[c for c in preferred_cols if c in df.columns]]
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="company")

    return len(results), len(failures), out_json, out_xlsx, (out_fail if failures else None)

def main():
    os.makedirs(SAVE_DIR, exist_ok=True)
    codes = load_corp_codes(INPUT_JSON)
    total = len(codes)
    print("총 대상 수:", total)

    start = read_checkpoint(default_start=0)
    print("재개 시작 인덱스(0-base):", start)

    sess = make_session()

    for start_idx in range(start, total, BATCH_SIZE):
        end_idx = min(start_idx + BATCH_SIZE, total)
        print(f"배치 처리: {start_idx+1} ~ {end_idx} / {total}")

        ok, ng, jpath, xpath, fpath = process_batch(sess, codes, start_idx, end_idx, SAVE_DIR)
        print("성공:", ok, "실패:", ng)
        print("JSON:", jpath)
        if ok:
            print("XLSX:", xpath)
        if fpath:
            print("실패 로그:", fpath)

        # 체크포인트: 다음 배치 시작 인덱스 저장
        write_checkpoint(end_idx)

        # 배치 간 대기
        time.sleep(DELAY_SEC_BETWEEN_BATCH)

    print("전체 완료")

if __name__ == "__main__":
    main()
