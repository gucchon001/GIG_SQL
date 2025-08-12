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
    $PythonScript = Join-Path $ProjectRoot "scripts\python\run_create_datesets_test.py"
    Write-Host "Executing Python script: $PythonScript" -ForegroundColor Cyan
    
    python $PythonScript
    
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