# PowerShell script to run the Python application
# Error handling: stop script on error
$ErrorActionPreference = "Stop"

Write-Host "=== Python Application Execution Script ===" -ForegroundColor Green

try {
    # Update pyenv.cfg dynamically (currently disabled for local execution)
    # Write-Host "Updating pyenv.cfg..." -ForegroundColor Yellow
    # python "\\NAS\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode\update_pyenv_cfg.py"

    # Activate Python virtual environment
    Write-Host "Activating Python virtual environment..." -ForegroundColor Yellow
    $venvPath = "C:\dev\GIG塾STMYSQL\sourcecode\venv"
    $activateScript = "$venvPath\Scripts\Activate.ps1"
    $activateBat = "$venvPath\Scripts\activate.bat"
    
    if (Test-Path $activateScript) {
        & $activateScript
        Write-Host "Virtual environment activated successfully (PowerShell)." -ForegroundColor Green
    } elseif (Test-Path $activateBat) {
        Write-Host "Using batch file activation..." -ForegroundColor Yellow
        & $activateBat
        Write-Host "Virtual environment activated successfully (Batch)." -ForegroundColor Green
    } else {
        Write-Warning "Virtual environment not found at: $venvPath"
        Write-Host "Continuing without virtual environment activation..." -ForegroundColor Yellow
    }

    # Execute main Python script
    Write-Host "Executing main.py..." -ForegroundColor Yellow
    Write-Host "Current directory: $PWD" -ForegroundColor Cyan
    
    python main.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Python script completed successfully." -ForegroundColor Green
    } else {
        Write-Error "Python script failed with exit code: $LASTEXITCODE"
    }

} catch {
    Write-Error "Error during script execution: $($_.Exception.Message)"
    Write-Host "Error details:" -ForegroundColor Red
    Write-Host $_.Exception.ToString() -ForegroundColor Red
} finally {
    # Wait 60 seconds
    Write-Host "Waiting 60 seconds..." -ForegroundColor Yellow
    Start-Sleep -Seconds 60
}

Write-Host "Script completed." -ForegroundColor Green