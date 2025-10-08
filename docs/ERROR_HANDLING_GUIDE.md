# エラー対応ガイド

## 📋 目次
1. [一時的なエラーへの対応](#一時的なエラーへの対応)
2. [エラータイプ別の対処法](#エラータイプ別の対処法)
3. [予防策](#予防策)
4. [監視とアラート](#監視とアラート)

---

## 🛡️ 一時的なエラーへの対応

### 1. 自動リトライ機能（実装済み）

**PowerShellスクリプト**: `create_datasets.ps1`

```powershell
# リトライ設定
$MaxRetries = 3              # 最大リトライ回数
$RetryDelaySeconds = 60      # リトライ間隔（秒）
```

**動作**:
- エラー発生時、60秒待機後に自動的に再試行
- 最大3回まで自動リトライ
- 3回失敗した場合はエラー終了

**カスタマイズ方法**:
```powershell
# リトライ回数を増やす場合
$MaxRetries = 5

# 待機時間を延長する場合
$RetryDelaySeconds = 120  # 2分
```

---

## 🔍 エラータイプ別の対処法

### A. データベース接続エラー

#### **エラーメッセージ例**:
```
データベース接続の取得に失敗しました
MySQL Connection not available
Lost connection to MySQL server during query
```

#### **原因**:
- SSHトンネルの切断
- ネットワークの一時的な不安定
- データベースサーバーのタイムアウト

#### **対処法**:

**1. 即座の対応**:
```powershell
# 手動でスクリプトを再実行
cd C:\DEV\jukust_mysql_sync_stmin
.\scripts\powershell\create_datasets.ps1
```

**2. SSH接続の確認**:
```powershell
# SSHサーバーへのPing確認
ping ec2-18-183-205-36.ap-northeast-1.compute.amazonaws.com

# ポート確認
Test-NetConnection -ComputerName ec2-18-183-205-36.ap-northeast-1.compute.amazonaws.com -Port 22
```

**3. 予防策**:
- VPN接続が確立されているか確認
- SSHキーの有効期限を確認
- データベース接続プールの設定を見直し

---

### B. Google Sheets APIエラー

#### **エラーメッセージ例**:
```
APIError: [400]: This action would increase the number of cells in the workbook above the limit of 10000000 cells.
```

#### **原因**:
- Google Sheetsのセル数上限（1,000万セル）に到達
- APIレート制限（1分あたりの書き込み回数制限）

#### **対処法**:

**1. セル数上限エラー**:
```python
# 該当するシートのデータをアーカイブ
# 例: 提出_testシート

# 対応手順:
1. Google Sheetsで該当シートを開く
2. 古いデータを別シートに移動
3. 元のシートをクリア
4. スクリプトを再実行
```

**2. APIレート制限エラー**:
```python
# settings.iniで処理間隔を延長
[Tuning]
delay = 1.0  # 0.5秒 → 1.0秒に延長
```

**3. 予防策**:
- 月次でデータをアーカイブ
- 必要最小限のデータのみ保持
- BigQueryへの移行を検討

---

### C. ネットワーク接続エラー

#### **エラーメッセージ例**:
```
RetryError[<Future ... raised OperationalError>]
```

#### **原因**:
- インターネット接続の不安定
- ファイアウォール設定
- プロキシ設定の問題

#### **対処法**:

**1. ネットワーク診断**:
```powershell
# インターネット接続確認
Test-Connection -ComputerName 8.8.8.8 -Count 4

# DNS確認
nslookup ec2-18-183-205-36.ap-northeast-1.compute.amazonaws.com
```

**2. VPN接続の再確立**:
```
1. VPNクライアントを開く
2. 接続を切断
3. 再接続
4. スクリプトを再実行
```

---

## 🔄 エラー発生時の標準対応フロー

### ステップ1: エラー内容の確認
```powershell
# 最新のログを確認
Get-Content logs\datasets.log -Tail 50

# main.logも確認
Get-Content logs\main.log -Tail 50
```

### ステップ2: エラータイプの特定
- データベース接続エラー？
- Google Sheets APIエラー？
- ネットワークエラー？

### ステップ3: 自動リトライの確認
- リトライ機能が動作したか確認
- 3回のリトライ後も失敗している場合は手動介入

### ステップ4: 手動での再実行
```powershell
# 仮想環境をアクティベート
& C:\DEV\jukust_mysql_sync_stmin\venv\Scripts\Activate.ps1

# スクリプトを再実行
.\scripts\powershell\create_datasets.ps1
```

### ステップ5: 結果の確認
```powershell
# 処理結果を確認
Get-Content logs\datasets.log -Tail 20 | Select-String "成功|失敗"
```

---

## 🚨 エラー発生時の緊急対応チェックリスト

- [ ] ログファイルでエラー内容を確認
- [ ] ネットワーク接続を確認
- [ ] VPN接続を確認
- [ ] データベースサーバーの稼働確認
- [ ] 自動リトライが実行されたか確認
- [ ] 必要に応じて手動再実行
- [ ] 実行結果をログで確認
- [ ] 問題が解決しない場合はエスカレーション

---

## 📊 予防策

### 1. 定期的な健全性チェック

**週次チェックリスト**:
```powershell
# データベース接続テスト
.\scripts\powershell\run.ps1

# ディスク容量確認
Get-PSDrive C | Select-Object Used, Free

# ログファイルサイズ確認
Get-ChildItem logs\ -Recurse | Measure-Object -Property Length -Sum
```

### 2. ログローテーション

**古いログの削除**:
```powershell
# 30日以上前のログを削除
Get-ChildItem logs\ -Recurse | Where-Object {
    $_.LastWriteTime -lt (Get-Date).AddDays(-30)
} | Remove-Item -Force
```

### 3. Google Sheetsのメンテナンス

**月次メンテナンス**:
1. セル数の多いシートを確認
2. 6ヶ月以上前のデータをアーカイブ
3. 不要なシートを削除

---

## 🔔 監視とアラート

### 推奨される監視項目

#### 1. ログ監視
```powershell
# エラーログの監視
Get-Content logs\datasets.log -Wait | Select-String "ERROR"
```

#### 2. 実行結果の監視
```powershell
# 失敗件数のカウント
Get-Content logs\datasets.log | Select-String "失敗" | Measure-Object
```

#### 3. ディスク容量の監視
```powershell
# ディスク使用率が80%を超えたらアラート
$disk = Get-PSDrive C
$usagePercent = ($disk.Used / ($disk.Used + $disk.Free)) * 100
if ($usagePercent -gt 80) {
    Write-Host "警告: ディスク使用率が${usagePercent}%です" -ForegroundColor Red
}
```

---

## 📞 エスカレーション基準

以下の場合は管理者にエスカレーション:

1. **3回の自動リトライ後も失敗が継続**
2. **データベースサーバーに接続できない（30分以上）**
3. **Google Sheets APIエラーが解決しない**
4. **データの欠損や不整合が発生**
5. **ディスク容量不足**

---

## 🔧 トラブルシューティング Tips

### Tip 1: ログの効率的な確認
```powershell
# エラーのみ抽出
Get-Content logs\datasets.log | Select-String "ERROR|エラー"

# 特定日時のログを確認
Get-Content logs\datasets.log | Select-String "2025-10-08"

# 最新100行を確認
Get-Content logs\datasets.log -Tail 100
```

### Tip 2: プロセスの強制終了
```powershell
# Pythonプロセスを終了
Get-Process python | Stop-Process -Force

# Streamlitプロセスを終了
Get-Process streamlit | Stop-Process -Force
```

### Tip 3: 環境のリセット
```powershell
# 仮想環境の再作成
Remove-Item venv -Recurse -Force
python -m venv venv
& venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 📝 変更履歴

- **2025-10-08**: 初版作成
  - 自動リトライ機能を追加
  - エラー対応フローを整備
  - トラブルシューティングガイドを追加

