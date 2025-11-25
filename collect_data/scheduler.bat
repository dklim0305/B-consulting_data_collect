@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1

echo [%date% %time%] 자동수집 프로그램 시작
python -X utf8 "C:\Users\admin\PycharmProjects\PythonProject\collect_data\bigdata_scheduler.py"


if %errorlevel% neq 0 (
    echo [%date% %time%] 수집 실패. DB 적재하지 않음
    pause
    exit /b 1
)

echo [%date% %time%] 수집 완료, DB 적재 시작
python -X utf8 "C:\Users\admin\PycharmProjects\PythonProject\collect_data\load_to_db.py"

echo [%date% %time%] DB 적재 완료

pause