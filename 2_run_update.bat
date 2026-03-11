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

:: [설정] 특정 종목 가격 강제 지정 (메가젠임플란트 등)
set "MEGA_PRICE=58900"

:: 명령줄 인수(%1)가 있는 경우 해당 날짜를 우선 사용합니다.
if not "%1"=="" set "TARGET_DATE=%1"

:: 날짜가 비어있으면 오늘 날짜로 설정 (PowerShell 사용)
if "%TARGET_DATE%"=="" (
    for /f "tokens=*" %%a in ('powershell -Command "Get-Date -Format 'yyyyMMdd'"') do set "TARGET_DATE=%%a"
)

echo [정보] 날짜 %TARGET_DATE% 기준으로 잔고 업데이트를 시작합니다. (메가젠: %MEGA_PRICE%원)

.\venv311\Scripts\python src/update_holdings.py --date %TARGET_DATE% --override-price "메가젠임플란트=%MEGA_PRICE%" --gs-holdings-key 1oCLjuqYxGc-RG4ArhF3GeXvQhN_sQDC1qMkh0YBk_lc --gs-trades-key 1oCLjuqYxGc-RG4ArhF3GeXvQhN_sQDC1qMkh0YBk_lc --gs-trades-gid 722753069 --gs-cred "config/stock-holding-log-db46e6d87dd6.json"

pause
