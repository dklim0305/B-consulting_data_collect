import psycopg2
import pandas as pd
import os
import glob
import sys
from datetime import datetime

BASE_DIR = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\data_set_all"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "prdb"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_SCHEMA = "prdb"
DB_TABLE = "public_procurement_list"


def log_msg(msg):
    """로그 출력"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)


def check_validation(summary_path):
    """validation 확인"""
    if not os.path.exists(summary_path):
        return False

    try:
        with open(summary_path, 'r', encoding='utf-8') as f:
            content = f.read()
            return ("목표 달성" in content or "완성도: 100.0%" in content)
    except:
        return False


def load_merged_csv(csv_path):
    """merged.csv 읽기"""
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        log_msg(f"CSV 읽기 실패: {str(e)}")
        return None


def insert_to_postgres(df):
    """PostgreSQL에 데이터 INSERT"""
    if df is None or len(df) == 0:
        return 0, 0

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        cur.execute(f"SET search_path TO {DB_SCHEMA}, public;")

        insert_count = 0
        skip_count = 0

        for idx, row in df.iterrows():
            columns = ', '.join([f'"{col}"' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            values = tuple(row)

            sql = f'INSERT INTO {DB_SCHEMA}.{DB_TABLE} ({columns}) VALUES ({placeholders})'

            try:
                cur.execute(sql, values)
                insert_count += 1
            except psycopg2.IntegrityError:
                skip_count += 1
                conn.rollback()
            except Exception as e:
                log_msg(f"INSERT 오류: {str(e)}")
                conn.rollback()

        conn.commit()
        cur.close()
        conn.close()

        return insert_count, skip_count

    except Exception as e:
        log_msg(f"PostgreSQL 연결 오류: {str(e)}")
        return 0, 0


def process_month(year, month):
    """월별 데이터 처리"""
    folder_name = f"public_procurement_{year:04d}{month:02d}"
    folder_path = os.path.join(BASE_DIR, folder_name)

    if not os.path.exists(folder_path):
        log_msg(f"폴더 없음: {folder_name}")
        return False

    log_msg(f"처리 시작: {year}년 {month}월")

    summary_files = glob.glob(os.path.join(folder_path, "*_summary.txt"))

    if not summary_files:
        log_msg(f"summary.txt 없음: {folder_name}")
        return False

    summary_path = summary_files[0]

    if not check_validation(summary_path):
        log_msg(f"검증 실패: {folder_name}")
        return False

    log_msg(f"검증 성공: {folder_name}")

    merged_files = glob.glob(os.path.join(folder_path, "*_merged.csv"))

    if not merged_files:
        log_msg(f"merged.csv 없음: {folder_name}")
        return False

    csv_path = merged_files[0]
    df = load_merged_csv(csv_path)

    if df is None:
        return False

    log_msg(f"CSV 로드: {len(df)}행")

    insert_count, skip_count = insert_to_postgres(df)

    log_msg(f"INSERT: {insert_count}건, SKIP: {skip_count}건")
    log_msg(f"완료: {year}년 {month}월")

    return True


def main():
    """메인 프로그램"""

    log_msg("PostgreSQL 적재 시작")

    if len(sys.argv) >= 2:
        date_str = sys.argv[1]
        year = int(date_str[:4])
        month = int(date_str[4:6])
        process_month(year, month)
    else:
        folders = sorted(glob.glob(os.path.join(BASE_DIR, "public_procurement_*")))

        for folder_path in folders:
            folder_name = os.path.basename(folder_path)
            year = int(folder_name.split('_')[2][:4])
            month = int(folder_name.split('_')[2][4:6])
            process_month(year, month)

    log_msg("PostgreSQL 적재 완료")


if __name__ == "__main__":
    main()