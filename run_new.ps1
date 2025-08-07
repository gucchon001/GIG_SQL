# New structure batch execution script
# PowerShell Execution Policy setting may be required
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

param(
    [string]$Mode = "production",
    [string]$Config = "config.ini",
    [string]$ExecutionColumn = $null
)

Write-Host "========================================" -ForegroundColor Green
Write-Host "Jukuste CSV Download Tool" -ForegroundColor Green
Write-Host "New Structure Batch Execution" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Python path setup
$env:PYTHONPATH = "$PWD;$PWD\src"

# Execution mode validation
switch ($Mode.ToLower()) {
    "production" {
        Write-Host "Production mode execution" -ForegroundColor Yellow
        $ExecutionCol = if ($ExecutionColumn) { $ExecutionColumn } else { "実行対象" }
    }
    "test" {
        Write-Host "Test mode execution" -ForegroundColor Yellow
        $ExecutionCol = if ($ExecutionColumn) { $ExecutionColumn } else { "テスト実行" }
    }
    "rawdata" {
        Write-Host "Raw data mode execution" -ForegroundColor Yellow
        $ExecutionCol = if ($ExecutionColumn) { $ExecutionColumn } else { "テスト実行" }
    }
    default {
        Write-Host "Invalid mode: $Mode" -ForegroundColor Red
        Write-Host "Available modes: production, test, rawdata" -ForegroundColor Red
        exit 1
    }
}

Write-Host "Config file: $Config" -ForegroundColor Cyan
Write-Host "Execution column: $ExecutionCol" -ForegroundColor Cyan
Write-Host "----------------------------------------" -ForegroundColor Green

try {
    # Execute batch processing
    if ($ExecutionColumn) {
        python -m src.batch_system.main --mode $Mode --config $Config --execution-column $ExecutionColumn
    } else {
        python -m src.batch_system.main --mode $Mode --config $Config
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "----------------------------------------" -ForegroundColor Green
        Write-Host "Batch processing completed successfully" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } else {
        Write-Host "----------------------------------------" -ForegroundColor Red
        Write-Host "Error occurred during batch processing" -ForegroundColor Red
        Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "----------------------------------------" -ForegroundColor Red
    Write-Host "Exception occurred during execution: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Usage examples:" -ForegroundColor Cyan
Write-Host "  .\run_new.ps1 -Mode production" -ForegroundColor White
Write-Host "  .\run_new.ps1 -Mode test" -ForegroundColor White
Write-Host "  .\run_new.ps1 -Mode rawdata" -ForegroundColor White
Write-Host "  .\run_new.ps1 -Mode production -Config custom_config.ini" -ForegroundColor White
Write-Host "  .\run_new.ps1 -Mode test -ExecutionColumn CustomExecution" -ForegroundColor White