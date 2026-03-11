@echo off
setlocal
chcp 65001 > nul
cd /d "%~dp0"

:: ================================================
:: [설정] 비교 기준 날짜 (YYYYMMDD 형식)
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

:: 스프레드시트 키 (수정 불필요)
:: 기준계좌 파일 (update_holdings.py가 사용하는 파일)
set "BASE_KEY=1oCLjuqYxGc-RG4ArhF3GeXvQhN_sQDC1qMkh0YBk_lc"

:: 내 계좌 파일 (compare_holdings.py가 사용하는 파일)
set "MINE_KEY=1OluYqwosyYzWLXYh_iGfoMKsbaM6WjVlAiXImDfGWdA"

echo [정보] 기준계좌(잔고_%TARGET_DATE%)와 내 계좌 비교를 시작합니다.

.\venv311\Scripts\python src/compare_holdings_light.py --base-key %BASE_KEY% --base-tab 잔고_%TARGET_DATE% --mine-key %MINE_KEY% --mine-tab 내계좌 --target-type mine

pause
