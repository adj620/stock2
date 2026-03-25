@echo off
setlocal
cd /d "%~dp0"

:: Set Default Date
:: Leave empty to auto-detect today's date
set "TARGET_DATE="

:: Override with CLI argument if provided
if not "%1"=="" set "TARGET_DATE=%1"

:: Auto-detect date removed - let python script handle its own default (Yesterday/Last Business Day)
:: if "%TARGET_DATE%"=="" (
::     for /f "tokens=*" %%a in ('powershell -Command "Get-Date -Format 'yyyyMMdd'"') do set "TARGET_DATE=%%a"
:: )

echo [INFO] Running update_holdings_us.py (Default date will be used if not specified)

if "%TARGET_DATE%"=="" (
    uv run python src/update_holdings_us.py --gs-holdings-key 1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ --gs-trades-key 1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ --gs-trades-gid 1422325845 --gs-cred "config/stock-holding-log-db46e6d87dd6.json"
) else (
    echo [INFO] Target Date: %TARGET_DATE%
    uv run python src/update_holdings_us.py --date %TARGET_DATE% --gs-holdings-key 1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ --gs-trades-key 1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ --gs-trades-gid 1422325845 --gs-cred "config/stock-holding-log-db46e6d87dd6.json"
)

pause
