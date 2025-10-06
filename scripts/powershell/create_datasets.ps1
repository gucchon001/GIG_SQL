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
    
    # Execute Python script with explicit UTF-8 encoding
    cmd /c "chcp 65001 > nul; python `"$PythonScript`""
    
    # Check exit code
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Python script error occurred" -ForegroundColor Red
        throw "Python execution error: ExitCode=$LASTEXITCODE"
    }
    
    Write-Host "Dataset creation completed successfully" -ForegroundColor Green
    
} catch {
    Write-Host "Error occurred: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    # Return to original directory
    Pop-Location
    Write-Host "Script execution finished" -ForegroundColor Green
}