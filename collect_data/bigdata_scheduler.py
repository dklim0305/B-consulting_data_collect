import time
import subprocess
import os
import sys
from datetime import datetime

# 한글 깨짐 방지
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')



# 설정

BIGDATA_PROGRAM = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\bigdata_api_program_dk.py"
STATUS_FILE = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\data_set_all\status.json"
LOG_FILE = r"C:\Users\admin\PycharmProjects\PythonProject\collect_data\data_set_all\scheduler_log.txt"
INTERVAL = 28800  # 8시간


# 유틸리티

def print_log(msg):
    """콘솔 출력 + 로그 파일 저장"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    text = f"[{timestamp}] {msg}"
    print(text, flush=True)

    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(text + "\n")
            f.flush()
    except:
        pass


def run_collection(init_mode=False, init_month="202510"):
    """데이터 수집 실행"""

    try:
        cmd = [
            "python",
            "-X",
            "utf8",
            "-u",
            BIGDATA_PROGRAM,
            "--init",
            init_month
        ] if init_mode else [
            "python",
            "-X",
            "utf8",
            "-u",
            BIGDATA_PROGRAM
        ]

        mode_text = f"초기화 ({init_month})" if init_mode else "진행 중인 월"

        print_log(f"데이터 수집 시작 - {mode_text}")
        print("\n")

        # 프로세스 실행 (버퍼링 비활성화)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1
        )

        # 실시간 출력
        for line in process.stdout:
            line = line.rstrip()
            if line:  # 빈 줄 제외
                print(line)
                try:
                    with open(LOG_FILE, 'a', encoding='utf-8') as f:
                        f.write(line + "\n")
                except:
                    pass

        return_code = process.wait()

        if return_code == 0:
            print_log("성공!")
            return True
        else:
            print_log(f"실패 (코드: {return_code})")
            return False

    except Exception as e:
        print_log(f"오류: {str(e)}")
        return False


def scheduler(init_month="202510"):
    """메인 스케줄러"""

    print("공공데이터 포털 나라장터 API 자동 수집 스케줄러")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"주기: 8시간마다")
    print(f"로그: {LOG_FILE}\n")

    print_log("스케줄러 시작")

    count = 0
    is_first = not os.path.exists(STATUS_FILE)

    try:
        while True:
            count += 1
            print_log(f"【 실행 #{count} 】")

            # 첫 실행만 초기화
            init = (count == 1) and is_first
            success = run_collection(init_mode=init, init_month=init_month)

            # 다음 실행 시간
            next_time = datetime.fromtimestamp(datetime.now().timestamp() + INTERVAL)
            print_log(f"다음 실행: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print_log("")

            print("8시간 대기 중. (Ctrl+C로 종료)")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print_log("스케줄러 종료됨")
        print("종료되었습니다.")
        sys.exit(0)

    except Exception as e:
        print_log(f"오류: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    init_month = sys.argv[1] if len(sys.argv) > 1 else "202510"
    scheduler(init_month)