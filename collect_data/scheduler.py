import time
import subprocess
from datetime import datetime


def run_crawling():
    """
    crawling.py 파일을 실행
    """
    crawling_path = r"/collect_data/read_text_folder.py"

    try:
        subprocess.run([
            "python",
            crawling_path
        ], check=True)
    except Exception as e:
        print(f"크롤링 실행 실패: {e}")


def scheduler():
    """
    10분마다 크롤링을 실행하는 스케줄러
    """
    print("=" * 50)
    print("스케줄러 시작!")
    print("=" * 50)

    start_time = datetime.now()
    print(f"[시작 시간] {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    interval = 600  # 10분 = 600초

    try:
        while True:  # 계속 반복
            current_time = datetime.now()

            print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] 크롤링 시작")
            run_crawling()
            print()

            print(f"다음 실행: 10분 후")

            # 10분 대기
            time.sleep(interval)

    except KeyboardInterrupt:
        print("스케줄러 종료됨")


if __name__ == "__main__":
    scheduler()