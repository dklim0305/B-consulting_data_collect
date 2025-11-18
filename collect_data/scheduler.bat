@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1
python -X utf8 "C:\Users\admin\PycharmProjects\PythonProject\collect_data\bigdata_scheduler.py"
pause