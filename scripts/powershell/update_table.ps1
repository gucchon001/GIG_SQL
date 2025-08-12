param(
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    [string]$Mode = "test"
)

Write-Host "Table Update Tool" -ForegroundColor Green
Write-Host "Table: $TableName" -ForegroundColor Cyan
Write-Host "Mode: $Mode" -ForegroundColor Cyan

if ($Mode -eq "test") {
    $ExecutionColumn = "個別リスト"
} else {
    $ExecutionColumn = "個別リスト"
}

Write-Host "Execution Column: $ExecutionColumn" -ForegroundColor Yellow

try {
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
    Push-Location $ProjectRoot
    
    $PythonScript = "scripts\python\run_create_datasets_individual.py"
    Write-Host "Executing: python $PythonScript $TableName" -ForegroundColor Yellow
    
        python $PythonScript $TableName
    $ExitCode = $LASTEXITCODE

    if ($ExitCode -eq 0) {
        Write-Host "Success!" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "Failed with exit code: $ExitCode" -ForegroundColor Red
        exit $ExitCode
    }
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
} finally {
    Pop-Location
}
