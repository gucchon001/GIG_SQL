# データ更新状況チェックスクリプト
# ボタンを押したのに更新されていないケースを検知

param(
    [int]$Hours = 24,  # 何時間以上更新されていないファイルを検出するか
    [switch]$SendNotification  # Slack通知を送信するかどうか
)

$ErrorActionPreference = "Continue"

# プロジェクトルートに移動
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
Push-Location $ProjectRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "データ更新状況チェック" -ForegroundColor Cyan
Write-Host "対象期間: 過去 $Hours 時間" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

try {
    # 仮想環境をアクティベート
    $VenvPath = Join-Path $ProjectRoot "venv\Scripts\Activate.ps1"
    if (Test-Path $VenvPath) {
        & $VenvPath
    }
    
    # 更新失敗検知スクリプトを実行
    if ($SendNotification) {
        Write-Host "Slack通知を有効にして実行します..." -ForegroundColor Yellow
        python "$ProjectRoot\scripts\python\check_update_failure.py"
    } else {
        Write-Host "チェックのみ実行します（通知なし）..." -ForegroundColor Yellow
        python "$ProjectRoot\scripts\python\check_update_failure.py"
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "チェック完了" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    
} catch {
    Write-Host "エラーが発生しました: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    Pop-Location
}
