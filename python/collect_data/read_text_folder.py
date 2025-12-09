import os
from datetime import datetime

def count_char_in_folder():
    """test_text 폴더에 이쓴ㄴ 파일을 읽어서 글자 수 count"""
    folder_path = r"C:\Users\admin\Desktop\test_text"
    log_path = r"C:\Users\admin\Desktop\test_text\log.txt"

    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if not os.path.exists(folder_path):
            message = f"[{current_time}] 폴더를 찾을 수 없습니다: {folder_path}"
            print(message)
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(message)
            return

        total_char = 0
        file_count = 0

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            if os.path.isfile(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        char_cnt = len(content)
                        total_char += char_cnt
                        file_count += 1
                        print(f"  - {filename}: {char_cnt}글자")
                except Exception as e:
                    print(f"  - {filename}: 읽기 실패 ({e})")

        # 결과 메시지
        message = f"[{current_time}] ✓ 완료 | 파일: {file_count}개 | 총 글자수: {total_char}\n"
        print(message)

        # log.txt에 저장
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(message)

    except Exception as e:
        error_message = f"[{current_time}]  오류 발생: {str(e)}\n"
        print(error_message)
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(error_message)


if __name__ == "__main__":
    count_char_in_folder()
