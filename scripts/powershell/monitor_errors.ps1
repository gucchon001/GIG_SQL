# エラー監視スクリプト
# 最近のエラーを確認し、サマリーを表示

param(
    [int]$Hours = 24,  # 過去何時間分を確認するか
    [switch]$SendSlackNotification  # Slack通知を送信するかどうか
)

$ErrorActionPreference = "Continue"

# プロジェクトルートに移動
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
Push-Location $ProjectRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "エラー監視レポート" -ForegroundColor Cyan
Write-Host "対象期間: 過去 $Hours 時間" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

try {
    # 現在時刻
    $Now = Get-Date
    $CutoffTime = $Now.AddHours(-$Hours)
    
    # datasets.logのエラーをカウント
    Write-Host "📊 datasets.log の分析" -ForegroundColor Yellow
    Write-Host "----------------------------------------" -ForegroundColor Gray
    
    if (Test-Path "logs\datasets.log") {
        $datasetsErrors = Get-Content "logs\datasets.log" | 
            Where-Object { $_ -match "ERROR|エラー" } |
            Where-Object {
                if ($_ -match "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}") {
                    try {
                        $logDate = [DateTime]::ParseExact(
                            $_.Substring(0, 19),
                            "yyyy-MM-dd HH:mm:ss",
                            $null
                        )
                        return $logDate -gt $CutoffTime
                    } catch {
                        return $false
                    }
                }
                return $false
            }
        
        $datasetsErrorCount = ($datasetsErrors | Measure-Object).Count
        
        if ($datasetsErrorCount -eq 0) {
            Write-Host "✅ エラーなし" -ForegroundColor Green
        } else {
            Write-Host "⚠️  エラー件数: $datasetsErrorCount 件" -ForegroundColor Red
            
            # エラータイプ別にカウント
            $dbErrors = ($datasetsErrors | Select-String "データベース接続|MySQL Connection|Lost connection").Count
            $apiErrors = ($datasetsErrors | Select-String "APIError|API").Count
            $otherErrors = $datasetsErrorCount - $dbErrors - $apiErrors
            
            Write-Host ""
            Write-Host "  - データベース接続エラー: $dbErrors 件" -ForegroundColor $(if ($dbErrors -gt 0) { "Red" } else { "Gray" })
            Write-Host "  - Google API エラー: $apiErrors 件" -ForegroundColor $(if ($apiErrors -gt 0) { "Red" } else { "Gray" })
            Write-Host "  - その他のエラー: $otherErrors 件" -ForegroundColor $(if ($otherErrors -gt 0) { "Red" } else { "Gray" })
            
            # 最新のエラーを表示
            Write-Host ""
            Write-Host "最新のエラー (最大5件):" -ForegroundColor Yellow
            $datasetsErrors | Select-Object -Last 5 | ForEach-Object {
                Write-Host "  $_" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "⚠️  datasets.log が見つかりません" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "----------------------------------------" -ForegroundColor Gray
    
    # main.logのエラーをカウント
    Write-Host "📊 main.log の分析" -ForegroundColor Yellow
    Write-Host "----------------------------------------" -ForegroundColor Gray
    
    if (Test-Path "logs\main.log") {
        $mainErrors = Get-Content "logs\main.log" | 
            Where-Object { $_ -match "ERROR|失敗" } |
            Where-Object {
                if ($_ -match "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}") {
                    try {
                        $logDate = [DateTime]::ParseExact(
                            $_.Substring(0, 19),
                            "yyyy-MM-dd HH:mm:ss",
                            $null
                        )
                        return $logDate -gt $CutoffTime
                    } catch {
                        return $false
                    }
                }
                return $false
            }
        
        $mainErrorCount = ($mainErrors | Measure-Object).Count
        
        if ($mainErrorCount -eq 0) {
            Write-Host "✅ エラーなし" -ForegroundColor Green
        } else {
            Write-Host "⚠️  エラー件数: $mainErrorCount 件" -ForegroundColor Red
            
            # 失敗したSQLファイルを抽出
            $failedSqls = $mainErrors | 
                Select-String "★失敗★" | 
                ForEach-Object { $_ -replace ".*★失敗★\s+([^:]+):.*", '$1' } |
                Sort-Object -Unique
            
            if ($failedSqls) {
                Write-Host ""
                Write-Host "失敗したSQLファイル:" -ForegroundColor Yellow
                $failedSqls | ForEach-Object {
                    Write-Host "  - $_" -ForegroundColor Red
                }
            }
        }
    } else {
        Write-Host "⚠️  main.log が見つかりません" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    
    # サマリー
    $totalErrors = $datasetsErrorCount + $mainErrorCount
    
    if ($totalErrors -eq 0) {
        Write-Host "✅ 過去 $Hours 時間にエラーはありません" -ForegroundColor Green
    } elseif ($totalErrors -lt 5) {
        Write-Host "⚠️  過去 $Hours 時間に $totalErrors 件のエラーが発生しました" -ForegroundColor Yellow
        Write-Host "   自動リトライで解決している可能性があります" -ForegroundColor Yellow
    } elseif ($totalErrors -lt 20) {
        Write-Host "⚠️  過去 $Hours 時間に $totalErrors 件のエラーが発生しました" -ForegroundColor Yellow
        Write-Host "   詳細な調査が推奨されます" -ForegroundColor Yellow
    } else {
        Write-Host "🚨 過去 $Hours 時間に $totalErrors 件のエラーが発生しました" -ForegroundColor Red
        Write-Host "   早急な対応が必要です" -ForegroundColor Red
    }
    
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # 推奨アクション
    if ($totalErrors -gt 0) {
        Write-Host "📋 推奨アクション:" -ForegroundColor Cyan
        Write-Host ""
        
        if ($dbErrors -gt 5) {
            Write-Host "  1. データベース接続を確認してください" -ForegroundColor Yellow
            Write-Host "     - VPN接続の確認" -ForegroundColor Gray
            Write-Host "     - SSHトンネルの確認" -ForegroundColor Gray
            Write-Host "     - データベースサーバーの稼働確認" -ForegroundColor Gray
        }
        
        if ($apiErrors -gt 3) {
            Write-Host "  2. Google Sheets APIの状態を確認してください" -ForegroundColor Yellow
            Write-Host "     - セル数上限の確認" -ForegroundColor Gray
            Write-Host "     - APIレート制限の確認" -ForegroundColor Gray
        }
        
        if ($failedSqls) {
            Write-Host "  3. 失敗したSQLファイルを手動で再実行してください" -ForegroundColor Yellow
            Write-Host "     - .\scripts\powershell\create_datasets.ps1" -ForegroundColor Gray
        }
        
        Write-Host ""
        Write-Host "詳細は docs\ERROR_HANDLING_GUIDE.md を参照してください" -ForegroundColor Cyan
    }
    
    # Slack通知オプションが有効な場合
    if ($SendSlackNotification -and $totalErrors -gt 0) {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "Slack通知を送信しています..." -ForegroundColor Yellow
        
        try {
            python "$ProjectRoot\scripts\python\notify_error.py"
            Write-Host "✅ Slack通知を送信しました" -ForegroundColor Green
        } catch {
            Write-Host "❌ Slack通知の送信に失敗しました: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    
} catch {
    Write-Host "エラー監視中に例外が発生しました: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    Pop-Location
}

Write-Host ""

