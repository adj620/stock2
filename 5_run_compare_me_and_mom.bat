@echo off
setlocal
chcp 65001 > nul
cd /d "%~dp0"

echo 🚀 내 계좌 vs 맘 계좌 비교 시작...
venv311\Scripts\python.exe src\compare_me_and_mom.py

if %ERRORLEVEL% neq 0 (
    echo.
    echo ❌ 오류가 발생했습니다.
    pause
) else (
    echo.
    echo ✅ 완료되었습니다. 5초 후 종료합니다.
    timeout /t 5 > nul
)
