# create_datasets_simple.ps1 - Dataset creation script (PowerShell version)

$ErrorActionPreference = "Stop"

# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host "Project Root: $ProjectRoot" -ForegroundColor Cyan

# Change to project root
Push-Location $ProjectRoot

try {
    # Activate virtual environment if exists
    $VenvPath = Join-Path $ProjectRoot "venv\Scripts\Activate.ps1"
    if (Test-Path $VenvPath) {
        & $VenvPath
        Write-Host "Virtual environment activated" -ForegroundColor Green
    } else {
        Write-Host "Virtual environment not found. Using system Python" -ForegroundColor Yellow
    }
    
    # Execute Python script
    $PythonScript = Join-Path $ProjectRoot "scripts\python\run_create_datesets.py"
    Write-Host "Executing Python script: $PythonScript" -ForegroundColor Cyan
    
    # Set UTF-8 encoding for Python output and console
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONLEGACYSTDIO = "utf-8"
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::InputEncoding = [System.Text.Encoding]::UTF8
    $PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
    
    # Retry configuration
    $MaxRetries = 3
    $RetryDelaySeconds = 60
    $RetryCount = 0
    $Success = $false
    
    # Execute Python script with retry logic
    while (-not $Success -and $RetryCount -lt $MaxRetries) {
        if ($RetryCount -gt 0) {
            Write-Host "----------------------------------------" -ForegroundColor Yellow
            Write-Host "リトライ試行 $RetryCount/$MaxRetries" -ForegroundColor Yellow
            Write-Host "${RetryDelaySeconds}秒待機してから再試行します..." -ForegroundColor Yellow
            Start-Sleep -Seconds $RetryDelaySeconds
        }
        
        chcp 65001 > $null
        python "$PythonScript"
        
        if ($LASTEXITCODE -eq 0) {
            $Success = $true
            Write-Host "Dataset creation completed successfully" -ForegroundColor Green
        } else {
            $RetryCount++
            if ($RetryCount -lt $MaxRetries) {
                Write-Host "Python script error occurred (ExitCode=$LASTEXITCODE)" -ForegroundColor Red
                Write-Host "エラーが発生しました。リトライします..." -ForegroundColor Yellow
            } else {
                Write-Host "Python script error occurred" -ForegroundColor Red
                throw "Python execution error: ExitCode=$LASTEXITCODE (最大リトライ回数に達しました)"
            }
        }
    }
    
} catch {
    Write-Host "Error occurred: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    # Return to original directory
    Pop-Location
    Write-Host "Script execution finished" -ForegroundColor Green
}