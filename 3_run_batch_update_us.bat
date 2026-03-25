@echo off
pushd "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp03_run_batch_update_us.ps1"
popd
pause
