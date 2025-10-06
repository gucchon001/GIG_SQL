# Individual Table Update Script
# Update only specified table

param(
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    [string]$Mode = "test",
    [string]$Config = "config/settings.ini"
)

Write-Host "========================================" -ForegroundColor Green
Write-Host "Individual Table Update Tool" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Parameter validation
if ([string]::IsNullOrWhiteSpace($TableName)) {
    Write-Host "Error: Table name not specified." -ForegroundColor Red
    Write-Host "Usage: .\update_individual_table.ps1 -TableName 'users'" -ForegroundColor Yellow
    exit 1
}

# Execution mode setting
if ($Mode.ToLower() -eq "production") {
    Write-Host "Production Mode" -ForegroundColor Yellow
    $ExecutionColumn = "実行対象"
} elseif ($Mode.ToLower() -eq "test") {
    Write-Host "Test Mode" -ForegroundColor Yellow
    $ExecutionColumn = "テスト実行"
} else {
    Write-Host "Invalid mode: $Mode" -ForegroundColor Red
    Write-Host "Available modes: production, test" -ForegroundColor Red
    exit 1
}

Write-Host "Config file: $Config" -ForegroundColor Cyan
Write-Host "Table name: $TableName" -ForegroundColor Cyan
Write-Host "Execution column: $ExecutionColumn" -ForegroundColor Cyan
Write-Host "----------------------------------------" -ForegroundColor Green

try {
    # Activate Python virtual environment
    Write-Host "Activating Python virtual environment..." -ForegroundColor Yellow
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
    $venvPath = Join-Path $ProjectRoot "venv"
    $activateScript = "$venvPath\Scripts\Activate.ps1"
    $activateBat = "$venvPath\Scripts\activate.bat"
    
    if (Test-Path $activateScript) {
        & $activateScript
        Write-Host "Virtual environment activated (PowerShell)" -ForegroundColor Green
    } elseif (Test-Path $activateBat) {
        Write-Host "Using batch file activation..." -ForegroundColor Yellow
        & $activateBat
        Write-Host "Virtual environment activated (Batch)" -ForegroundColor Green
    } else {
        Write-Warning "Virtual environment not found: $venvPath"
        Write-Host "Continuing without virtual environment..." -ForegroundColor Yellow
    }

    # Move to project root
    Push-Location $ProjectRoot
    Write-Host "Working directory: $PWD" -ForegroundColor Cyan
    
    # Execute individual table update Python script
    $PythonScript = Join-Path $ProjectRoot "scripts\python\run_create_datasets_individual.py"
    Write-Host "Execute script: $PythonScript" -ForegroundColor Cyan
    Write-Host "----------------------------------------" -ForegroundColor Green
    Write-Host "Starting individual table update..." -ForegroundColor Yellow
    
    # Set UTF-8 encoding for Python output and console
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONLEGACYSTDIO = "utf-8"
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::InputEncoding = [System.Text.Encoding]::UTF8
    $PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
    
    # Execute Python script with explicit UTF-8 encoding
    $Command = "chcp 65001 > nul & python `"$PythonScript`" `"$TableName`""
    cmd /c $Command
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "----------------------------------------" -ForegroundColor Green
        Write-Host "Individual table update completed successfully" -ForegroundColor Green
        Write-Host "Table: $TableName" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } else {
        Write-Host "----------------------------------------" -ForegroundColor Red
        Write-Host "Error occurred during individual table update" -ForegroundColor Red
        Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "Table: $TableName" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    
} catch {
    Write-Host "----------------------------------------" -ForegroundColor Red
    Write-Host "Exception occurred during execution: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
} finally {
    # Return to original directory
    Pop-Location
    
    # Wait time (optional)
    Write-Host ""
    Write-Host "Waiting 10 seconds before exit..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
}

Write-Host ""
Write-Host "Usage examples:" -ForegroundColor Cyan
Write-Host "  # Update users table in test mode" -ForegroundColor White
Write-Host "  .\update_individual_table.ps1 -TableName 'users'" -ForegroundColor White
Write-Host ""
Write-Host "  # Update orders table in production mode" -ForegroundColor White  
Write-Host "  .\update_individual_table.ps1 -TableName 'orders' -Mode production" -ForegroundColor White
Write-Host ""
Write-Host "  # Use custom config file" -ForegroundColor White
Write-Host "  .\update_individual_table.ps1 -TableName 'products' -Config 'config/custom.ini'" -ForegroundColor White
Write-Host ""
Write-Host "Script execution completed." -ForegroundColor Green