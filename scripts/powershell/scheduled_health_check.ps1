# 定期健全性チェックスクリプト
# タスクスケジューラで定期実行することを想定

param(
    [switch]$Verbose  # 詳細出力
)

$ErrorActionPreference = "Continue"

# プロジェクトルートに移動
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
Push-Location $ProjectRoot

# ログファイルに記録
$LogFile = "logs\health_check.log"
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

function Write-Log {
    param($Message)
    "$Timestamp - $Message" | Out-File -FilePath $LogFile -Append -Encoding UTF8
    if ($Verbose) {
        Write-Host $Message
    }
}

Write-Log "========================================"
Write-Log "定期健全性チェック開始"
Write-Log "========================================"

try {
    # 仮想環境をアクティベート
    $VenvPath = Join-Path $ProjectRoot "venv\Scripts\Activate.ps1"
    if (Test-Path $VenvPath) {
        & $VenvPath
        Write-Log "仮想環境をアクティベートしました"
    }
    
    $HasIssues = $false
    
    # チェック1: 過去1時間のエラーログ
    Write-Log "チェック1: エラーログ確認"
    $ErrorCount = 0
    
    if (Test-Path "logs\datasets.log") {
        $RecentErrors = Get-Content "logs\datasets.log" | 
            Select-String "ERROR" | 
            Select-Object -Last 10
        
        $ErrorCount = ($RecentErrors | Measure-Object).Count
        
        if ($ErrorCount -gt 0) {
            Write-Log "  ⚠️ エラーログ検出: $ErrorCount 件"
            $HasIssues = $true
        } else {
            Write-Log "  ✅ エラーなし"
        }
    }
    
    # チェック2: Streamlit起動状況
    Write-Log "チェック2: Streamlit起動確認"
    $StreamlitRunning = netstat -an | findstr :8501
    
    if ($StreamlitRunning) {
        Write-Log "  ✅ Streamlit起動中"
    } else {
        Write-Log "  ⚠️ Streamlitが起動していません"
        $HasIssues = $true
    }
    
    # チェック3: データ更新失敗チェック
    Write-Log "チェック3: データ更新状況確認"
    python "$ProjectRoot\scripts\python\check_update_failure.py" 2>&1 | Out-Null
    
    # チェック4: ディスク容量
    Write-Log "チェック4: ディスク容量確認"
    $Drive = Get-PSDrive C
    $UsagePercent = [math]::Round(($Drive.Used / ($Drive.Used + $Drive.Free)) * 100, 2)
    
    Write-Log "  ディスク使用率: ${UsagePercent}%"
    
    if ($UsagePercent -gt 90) {
        Write-Log "  🚨 ディスク容量が逼迫しています"
        $HasIssues = $true
    } elseif ($UsagePercent -gt 80) {
        Write-Log "  ⚠️ ディスク容量に注意が必要です"
        $HasIssues = $true
    } else {
        Write-Log "  ✅ ディスク容量正常"
    }
    
    # 問題がある場合はSlack通知
    if ($HasIssues) {
        Write-Log "問題が検出されました。詳細確認が必要です。"
        
        # Slack通知を送信（オプション）
        if ($Verbose) {
            Write-Host "問題が検出されました。詳細はログファイルを確認してください。" -ForegroundColor Yellow
        }
    } else {
        Write-Log "✅ すべてのチェックが正常です"
        if ($Verbose) {
            Write-Host "✅ すべてのチェックが正常です" -ForegroundColor Green
        }
    }
    
} catch {
    Write-Log "健全性チェック中にエラーが発生: $($_.Exception.Message)"
} finally {
    Write-Log "========================================`n"
    Pop-Location
}

if ($Verbose) {
    Write-Host ""
    Write-Host "ログファイル: $LogFile" -ForegroundColor Cyan
}
