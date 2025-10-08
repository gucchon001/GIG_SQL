# Task Scheduler Setup - Fixed Version
# Run as Administrator

$ProjectRoot = "C:\DEV\jukust_mysql_sync_stmin"
$PythonExe = "$ProjectRoot\venv\Scripts\python.exe"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

# Check admin rights
$IsAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $IsAdmin) {
    Write-Host "ERROR: Administrator rights required" -ForegroundColor Red
    Write-Host "Please run PowerShell as Administrator" -ForegroundColor Yellow
    exit 1
}

Write-Host "Creating scheduled tasks..." -ForegroundColor Yellow
Write-Host ""

# Task 1: Hourly Error Monitoring (Fixed - using Daily trigger with repetition)
Write-Host "[1/4] Error Monitoring (Every hour)" -ForegroundColor Cyan
try {
    $Action1 = New-ScheduledTaskAction -Execute $PythonExe -Argument "$ProjectRoot\scripts\python\notify_error.py" -WorkingDirectory $ProjectRoot
    # 毎日0時から開始し、1時間ごとに繰り返し、期間は1日
    $Trigger1 = New-ScheduledTaskTrigger -Daily -At "00:00"
    $Trigger1.Repetition = (New-ScheduledTaskTrigger -Once -At "00:00" -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 1)).Repetition
    
    Get-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring" -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false -ErrorAction SilentlyContinue
    
    Register-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring" `
        -Description "GIG SQL Error Log Monitoring (Hourly)" `
        -Action $Action1 `
        -Trigger $Trigger1 `
        -User "SYSTEM" `
        -RunLevel Highest | Out-Null
    
    Write-Host "  Status: OK" -ForegroundColor Green
} catch {
    Write-Host "  Status: FAILED - $($_.Exception.Message)" -ForegroundColor Red
}

# Task 2: Daily Health Check (8:00 AM)
Write-Host "[2/4] Health Check (Daily 08:00)" -ForegroundColor Cyan
try {
    $Action2 = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ProjectRoot\scripts\powershell\scheduled_health_check.ps1`" -Verbose" -WorkingDirectory $ProjectRoot
    $Trigger2 = New-ScheduledTaskTrigger -Daily -At "08:00"
    
    Get-ScheduledTask -TaskName "GIG_SQL_HealthCheck" -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false -ErrorAction SilentlyContinue
    
    Register-ScheduledTask -TaskName "GIG_SQL_HealthCheck" `
        -Description "GIG SQL Daily Health Check" `
        -Action $Action2 `
        -Trigger $Trigger2 `
        -User "SYSTEM" `
        -RunLevel Highest | Out-Null
    
    Write-Host "  Status: OK" -ForegroundColor Green
} catch {
    Write-Host "  Status: FAILED - $($_.Exception.Message)" -ForegroundColor Red
}

# Task 3: Update Failure Check (11:00 PM)
Write-Host "[3/4] Update Failure Check (Daily 23:00)" -ForegroundColor Cyan
try {
    $Action3 = New-ScheduledTaskAction -Execute $PythonExe -Argument "$ProjectRoot\scripts\python\check_update_failure.py" -WorkingDirectory $ProjectRoot
    $Trigger3 = New-ScheduledTaskTrigger -Daily -At "23:00"
    
    Get-ScheduledTask -TaskName "GIG_SQL_UpdateFailureCheck" -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false -ErrorAction SilentlyContinue
    
    Register-ScheduledTask -TaskName "GIG_SQL_UpdateFailureCheck" `
        -Description "GIG SQL Update Failure Detection" `
        -Action $Action3 `
        -Trigger $Trigger3 `
        -User "SYSTEM" `
        -RunLevel Highest | Out-Null
    
    Write-Host "  Status: OK" -ForegroundColor Green
} catch {
    Write-Host "  Status: FAILED - $($_.Exception.Message)" -ForegroundColor Red
}

# Task 4: Daily Batch (5:00 AM)
Write-Host "[4/4] Daily Batch (Daily 05:00)" -ForegroundColor Cyan
try {
    $Action4 = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ProjectRoot\scripts\powershell\create_datasets.ps1`"" -WorkingDirectory $ProjectRoot
    $Trigger4 = New-ScheduledTaskTrigger -Daily -At "05:00"
    
    Get-ScheduledTask -TaskName "GIG_SQL_DailyBatch" -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false -ErrorAction SilentlyContinue
    
    Register-ScheduledTask -TaskName "GIG_SQL_DailyBatch" `
        -Description "GIG SQL Daily Dataset Update" `
        -Action $Action4 `
        -Trigger $Trigger4 `
        -User "SYSTEM" `
        -RunLevel Highest | Out-Null
    
    Write-Host "  Status: OK" -ForegroundColor Green
} catch {
    Write-Host "  Status: FAILED - $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Registered Tasks" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Get-ScheduledTask | Where-Object {$_.TaskName -like "GIG_SQL_*"} | ForEach-Object {
    $Info = Get-ScheduledTaskInfo -TaskName $_.TaskName
    Write-Host ""
    Write-Host "$($_.TaskName)" -ForegroundColor Green
    Write-Host "  Description: $($_.Description)" -ForegroundColor Gray
    Write-Host "  Next Run: $($Info.NextRunTime)" -ForegroundColor Gray
    Write-Host "  State: $($_.State)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "To remove tasks, run:" -ForegroundColor Yellow
Write-Host "  Get-ScheduledTask | Where-Object {`$_.TaskName -like 'GIG_SQL_*'} | Unregister-ScheduledTask -Confirm:`$false" -ForegroundColor White

