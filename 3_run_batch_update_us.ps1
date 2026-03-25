# 3_run_batch_update_us.ps1
# Use ASCII for logic to prevent encoding issues in CMD/PS
$OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "`n============================================================"
Write-Host "  US Stock Holdings Batch Update (Catch-up)"
Write-Host "  미국 주식 잔고 일괄 업데이트 (밀린 일자 처리용)"
Write-Host "============================================================`n"

# Default Values (User Requested)
$DEFAULT_S = "20260225"
$DEFAULT_E = "20260324"
$DEFAULT_M = "t"
$DEFAULT_F = "y"

$S_INPUT = Read-Host "Start Date (YYYYMMDD) [Default: $DEFAULT_S]"
if ([string]::IsNullOrWhitespace($S_INPUT)) { $S_DATE = $DEFAULT_S } else { $S_DATE = $S_INPUT }

$E_INPUT = Read-Host "End Date (YYYYMMDD) [Default: $DEFAULT_E]"
if ([string]::IsNullOrWhitespace($E_INPUT)) { $E_DATE = $DEFAULT_E } else { $E_DATE = $E_INPUT }

$M_INPUT = Read-Host "Update mode? (t: Trade days only [Default], a: All business days)"
if ([string]::IsNullOrWhitespace($M_INPUT)) { $MODE = $DEFAULT_M } else { $MODE = $M_INPUT }
$ALL_DAYS = ""
if ($MODE -eq 'a') {
    $ALL_DAYS = "--all-days"
}

$F_INPUT = Read-Host "Allow empty previous holdings? (y/n) [Default: $DEFAULT_F]"
if ([string]::IsNullOrWhitespace($F_INPUT)) { $FORCE = $DEFAULT_F } else { $FORCE = $F_INPUT }
$ALLOW_EMPTY = ""
if ($FORCE -eq 'y') {
    $ALLOW_EMPTY = "--allow-empty-prev"
}

Write-Host "`n[INFO] Running multi-day update... ($S_DATE ~ $E_DATE)"
Write-Host "[INFO] Mode: $(if ($MODE -eq 't') { 'Trade days only' } else { 'All days' })"
Write-Host "[INFO] Allow Empty: $FORCE`n"

$ARGS_LIST = @("--start", $S_DATE, "--end", $E_DATE, "--gs-holdings-key", "1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ", "--gs-trades-key", "1er6DKTvf4He3RudThmuv1WpszVUKLdpwDvUjqTOrZDQ", "--gs-trades-gid", "1422325845", "--gs-cred", "config/stock-holding-log-db46e6d87dd6.json")

if (![string]::IsNullOrWhitespace($ALLOW_EMPTY)) {
    $ARGS_LIST += @($ALLOW_EMPTY)
}

if (![string]::IsNullOrWhitespace($ALL_DAYS)) {
    $ARGS_LIST += @($ALL_DAYS)
}

# Explicitly call uv run
& uv run --project . python src/batch_update_holdings_us.py $ARGS_LIST

Write-Host "`nOperation Completed. 모든 작업이 완료되었습니다."
Read-Host "Press Enter to exit..."
