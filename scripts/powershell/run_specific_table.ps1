# run_specific_table.ps1 - 指定テーブルのみを実行するスクリプト

param(
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = "data_Parquet"
)

$ErrorActionPreference = "Stop"

# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "指定テーブル実行スクリプト" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "テーブル名: $TableName" -ForegroundColor Yellow
Write-Host "出力先: $OutputPath" -ForegroundColor Yellow
Write-Host "プロジェクトルート: $ProjectRoot" -ForegroundColor Gray

# Change to project root
Push-Location $ProjectRoot

try {
    # Activate virtual environment if exists
    $VenvPath = Join-Path $ProjectRoot "venv\Scripts\Activate.ps1"
    if (Test-Path $VenvPath) {
        & $VenvPath
        Write-Host "[OK] 仮想環境をアクティベートしました" -ForegroundColor Green
    } else {
        Write-Host "[WARN] 仮想環境が見つかりません。システムのPythonを使用します" -ForegroundColor Yellow
    }
    
    # Set UTF-8 encoding for Python output and console
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONLEGACYSTDIO = "utf-8"
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::InputEncoding = [System.Text.Encoding]::UTF8
    $PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
    
    Write-Host "[INFO] 指定テーブルの実行を開始します..." -ForegroundColor Cyan
    
    # Execute Python script with table name parameter
    $PythonScript = Join-Path $ProjectRoot "scripts\python\run_specific_table.py"
    $Arguments = "`"$TableName`" `"$OutputPath`""
    
    Write-Host "[INFO] 実行コマンド: python $PythonScript $Arguments" -ForegroundColor Gray
    
    # Execute Python script with explicit UTF-8 encoding
    cmd /c "chcp 65001 > nul; python $PythonScript $Arguments"
    
    # Check exit code
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Pythonスクリプトでエラーが発生しました (終了コード: $LASTEXITCODE)" -ForegroundColor Red
        throw "Python execution error: ExitCode=$LASTEXITCODE"
    }
    
    Write-Host "===============================================" -ForegroundColor Green
    Write-Host "[SUCCESS] テーブル '$TableName' の実行が完了しました" -ForegroundColor Green
    Write-Host "===============================================" -ForegroundColor Green
    
} catch {
    Write-Host "===============================================" -ForegroundColor Red
    Write-Host "[ERROR] エラーが発生しました: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "===============================================" -ForegroundColor Red
    exit 1
} finally {
    # Return to original directory
    Pop-Location
    Write-Host "[INFO] スクリプト実行完了" -ForegroundColor Gray
}