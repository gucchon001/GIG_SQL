# 新しい構造でのStreamlit実行スクリプト

param(
    [string]$Port = "8501",
    [string]$ServerHost = "localhost"
)

Write-Host "========================================" -ForegroundColor Green
Write-Host "ストミンくん - Streamlit WebUI" -ForegroundColor Green
Write-Host "新しい構造での実行" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Pythonパスの設定
$env:PYTHONPATH = "$PWD;$PWD\src"

Write-Host "サーバー設定:" -ForegroundColor Cyan
Write-Host "  ホスト: $ServerHost" -ForegroundColor White
Write-Host "  ポート: $Port" -ForegroundColor White
Write-Host "  URL: http://${ServerHost}:${Port}" -ForegroundColor White
Write-Host "----------------------------------------" -ForegroundColor Green

try {
    Write-Host "Streamlitアプリを起動しています..." -ForegroundColor Yellow
    
    # Streamlitアプリ実行 - 正しいメインアプリを使用
    streamlit run streamlit_app.py --server.port $Port --server.address $ServerHost
    
} catch {
    Write-Host "----------------------------------------" -ForegroundColor Red
    Write-Host "Streamlit起動中に例外が発生しました: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "使用例:" -ForegroundColor Cyan
Write-Host "  .\run_streamlit_new.ps1" -ForegroundColor White
Write-Host "  .\run_streamlit_new.ps1 -Port 8502" -ForegroundColor White
Write-Host "  .\run_streamlit_new.ps1 -ServerHost 0.0.0.0 -Port 8501" -ForegroundColor White