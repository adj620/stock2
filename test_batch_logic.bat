@echo off
echo Testing Batch Logic...

.\venv311\Scripts\python mock_extract_trades.py
if errorlevel 1 (
    echo.
    echo Exiting batch due to errorlevel.
    goto :eof
)

.\venv311\Scripts\python mock_update_holdings.py
echo Batch completed.
