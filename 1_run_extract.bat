@echo off
setlocal
chcp 65001 > nul
cd /d "%~dp0"

:: ================================================
:: [설정] 작업 날짜 (YYYYMMDD 형식)
:: 날짜를 수정하려면 아래 TARGET_DATE 값을 변경하세요.
:: 비워두면 오늘 날짜가 자동으로 설정됩니다.
:: ================================================
set "TARGET_DATE="

:: 명령줄 인수(%1)가 있는 경우 해당 날짜를 우선 사용합니다.
if not "%1"=="" set "TARGET_DATE=%1"

:: 날짜가 비어있으면 오늘 날짜로 설정 (PowerShell 사용)
if "%TARGET_DATE%"=="" (
    for /f "tokens=*" %%a in ('powershell -Command "Get-Date -Format 'yyyyMMdd'"') do set "TARGET_DATE=%%a"
)

echo.
echo ================================================
echo   매매일지 자동 추출 스크립트
echo   날짜: %TARGET_DATE%
echo ================================================
echo.

.\venv311\Scripts\python src/extract_trades.py --date %TARGET_DATE%

pause
