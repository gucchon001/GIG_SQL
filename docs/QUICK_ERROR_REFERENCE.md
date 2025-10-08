# 🚨 クイックエラー対応リファレンス

## 📋 目次
- [よくあるエラーと即座の対処法](#よくあるエラーと即座の対処法)
- [緊急時のコマンド集](#緊急時のコマンド集)
- [連絡先・エスカレーション](#連絡先エスカレーション)

---

## ⚡ よくあるエラーと即座の対処法

### 1️⃣ データベース接続エラー

**エラー表示**:
```
データベース接続の取得に失敗しました
MySQL Connection not available
```

**即座の対処（60秒以内）**:
```powershell
# VPN接続を確認
# 再実行
.\scripts\powershell\create_datasets.ps1
```

**自動リトライ**: ✅ 有効（3回、60秒間隔）

---

### 2️⃣ Google Sheets セル数上限エラー

**エラー表示**:
```
APIError: [400]: This action would increase the number of cells above the limit of 10000000 cells
```

**即座の対処（5分以内）**:
1. Google Sheetsで該当シート（例: 提出_test）を開く
2. 古いデータを削除またはアーカイブ
3. スクリプトを再実行

**自動リトライ**: ❌ 無効（手動対応が必要）

---

### 3️⃣ ネットワーク接続エラー

**エラー表示**:
```
RetryError[<Future ... raised OperationalError>]
```

**即座の対処（2分以内）**:
```powershell
# ネットワーク確認
ping 8.8.8.8

# VPN再接続
# スクリプト再実行
.\scripts\powershell\create_datasets.ps1
```

**自動リトライ**: ✅ 有効（3回、60秒間隔）

---

### 4️⃣ PowerShell実行エラー

**エラー表示**:
```
このシステムではスクリプトの実行が無効になっているため...
```

**即座の対処（30秒以内）**:
```powershell
# 実行ポリシーを変更
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# または
powershell -ExecutionPolicy Bypass -File .\scripts\powershell\create_datasets.ps1
```

---

## 🔧 緊急時のコマンド集

### エラー状況の確認
```powershell
# 過去24時間のエラーサマリー
.\scripts\powershell\monitor_errors.ps1

# 過去1時間のエラーのみ
.\scripts\powershell\monitor_errors.ps1 -Hours 1

# 最新のログを確認
Get-Content logs\datasets.log -Tail 50
```

### 即座の再実行
```powershell
# カレントディレクトリをプロジェクトルートに変更
cd C:\DEV\jukust_mysql_sync_stmin

# 仮想環境をアクティベート
& venv\Scripts\Activate.ps1

# スクリプトを実行
.\scripts\powershell\create_datasets.ps1
```

### プロセスの強制終了
```powershell
# Python プロセスを終了
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force

# Streamlit プロセスを終了
Get-Process streamlit -ErrorAction SilentlyContinue | Stop-Process -Force
```

### ネットワーク診断
```powershell
# インターネット接続確認
Test-Connection -ComputerName 8.8.8.8 -Count 4

# データベースサーバー確認
Test-NetConnection -ComputerName ec2-18-183-205-36.ap-northeast-1.compute.amazonaws.com -Port 22
```

---

## 📞 連絡先・エスカレーション

### エスカレーションが必要な状況

#### 🔴 即座にエスカレーション（重大）
- 3回の自動リトライ後も失敗が継続
- データベースサーバーに30分以上接続できない
- データの欠損や不整合が発生
- ディスク容量が95%を超えている

#### 🟡 1時間以内にエスカレーション（重要）
- Google Sheets APIエラーが解決しない
- エラーが1時間に10回以上発生
- 特定のSQLファイルが繰り返し失敗

#### 🟢 24時間以内に報告（通常）
- 軽微なエラーが散発的に発生
- パフォーマンスの低下
- ログファイルサイズの急増

---

## 📊 健全性チェックコマンド

### 日次チェック（毎朝実行推奨）
```powershell
# エラー監視
.\scripts\powershell\monitor_errors.ps1 -Hours 24

# ディスク容量確認
Get-PSDrive C | Select-Object @{Name="使用率(%)";Expression={[math]::Round(($_.Used/($_.Used+$_.Free))*100,2)}}

# Streamlit起動確認
netstat -an | findstr :8501
```

### 週次チェック（毎週月曜実行推奨）
```powershell
# データベース接続テスト
.\scripts\powershell\run.ps1

# ログファイルサイズ確認
Get-ChildItem logs\ | Select-Object Name, @{Name="Size(MB)";Expression={[math]::Round($_.Length/1MB,2)}}

# 古いログの削除（30日以上前）
Get-ChildItem logs\ -Recurse | Where-Object {$_.LastWriteTime -lt (Get-Date).AddDays(-30)} | Remove-Item -Force
```

---

## 🔍 トラブルシューティングフローチャート

```
エラー発生
    ↓
[自動リトライが動作？]
    ↓ YES → 結果を待つ（最大3分）
    ↓ NO
[エラーログを確認]
    ↓
[エラータイプを特定]
    ├─ データベース接続エラー → VPN・SSH確認 → 再実行
    ├─ Google API エラー → シート確認 → データ削除 → 再実行
    ├─ ネットワークエラー → 接続確認 → 再実行
    └─ その他 → ログ詳細確認 → エスカレーション
```

---

## 💡 予防的メンテナンス

### 月次タスク（毎月1日実行推奨）
```powershell
# Google Sheets のメンテナンス
# 1. 大きいシート（提出_test等）の古いデータをアーカイブ
# 2. 不要なシートを削除
# 3. セル数を確認

# パフォーマンス確認
# 1. データベース接続速度
# 2. Parquetファイル書き込み速度
# 3. ログファイル分析
```

### 四半期タスク（3ヶ月ごと）
- システム全体のパフォーマンスレビュー
- エラー発生パターンの分析
- 設定値（リトライ回数、待機時間等）の見直し
- ドキュメントの更新

---

## 📚 詳細ドキュメント

詳しい情報は以下を参照:
- **エラー対応ガイド**: `docs/ERROR_HANDLING_GUIDE.md`
- **システムアーキテクチャ**: `docs/01_system_architecture.md`
- **運用ガイド**: `docs/03_deployment_operations_guide.md`

---

## 🔄 バージョン履歴

- **v1.0** (2025-10-08): 初版作成
  - 自動リトライ機能追加
  - エラー監視スクリプト追加
  - クイックリファレンス作成

---

**最終更新**: 2025年10月8日

