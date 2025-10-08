# ⏰ タスクスケジューラ設定ガイド

## 📋 目次
1. [概要](#概要)
2. [登録済みタスク](#登録済みタスク)
3. [セットアップ手順](#セットアップ手順)
4. [タスクの管理](#タスクの管理)
5. [トラブルシューティング](#トラブルシューティング)

---

## 📢 概要

本システムでは、以下の定期タスクがWindowsタスクスケジューラに登録されています。

### ✅ 登録済みタスク（4件）

| タスク名 | 実行頻度 | 実行時刻 | 説明 |
|---|---|---|---|
| **GIG_SQL_ErrorMonitoring** | 毎時 | - | エラーログ監視+Slack通知 |
| **GIG_SQL_HealthCheck** | 毎日 | 08:00 | 健全性チェック |
| **GIG_SQL_UpdateFailureCheck** | 毎日 | 23:00 | 更新失敗検知 |
| **GIG_SQL_DailyBatch** | 毎日 | 05:00 | データ更新（全件） |

---

## 📊 登録済みタスク詳細

### 1. エラーログ監視（毎時）⏰

**タスク名**: `GIG_SQL_ErrorMonitoring`

**実行内容**:
```powershell
python scripts\python\notify_error.py
```

**機能**:
- 過去1時間のエラーログをチェック
- エラーがあればSlackに通知
- datasets.logとmain.logを監視

**次回実行**: 毎時0分（例: 17:00, 18:00, 19:00...）

---

### 2. 健全性チェック（毎朝8:00）🏥

**タスク名**: `GIG_SQL_HealthCheck`

**実行内容**:
```powershell
.\scripts\powershell\scheduled_health_check.ps1 -Verbose
```

**機能**:
- エラーログの確認
- Streamlit起動状況の確認
- ディスク容量の確認
- 問題があればログに記録

**次回実行**: 毎朝08:00

---

### 3. 更新失敗検知（毎日23:00）🔍

**タスク名**: `GIG_SQL_UpdateFailureCheck`

**実行内容**:
```powershell
python scripts\python\check_update_failure.py
```

**機能**:
- 24時間以上更新されていないParquetファイルを検知
- バッチ実行ログから失敗を検出
- 問題があればSlackに通知

**次回実行**: 毎日23:00

---

### 4. データ更新（全件）（毎朝5:00）📦

**タスク名**: `GIG_SQL_DailyBatch`

**実行内容**:
```powershell
.\scripts\powershell\create_datasets.ps1
```

**機能**:
- 全25テーブルのデータ更新
- 自動リトライ（最大3回）
- エラー時のSlack通知
- 実行時間: 約5〜10分

**次回実行**: 毎朝05:00

---

## 🔧 セットアップ手順

### 初回セットアップ

#### ステップ1: PowerShellを管理者として起動

```
1. スタートメニューで「PowerShell」を検索
2. 右クリック → 「管理者として実行」
```

#### ステップ2: プロジェクトディレクトリに移動

```powershell
cd C:\DEV\jukust_mysql_sync_stmin
```

#### ステップ3: セットアップスクリプトを実行

```powershell
.\scripts\powershell\setup_tasks_fixed.ps1
```

#### ステップ4: 登録確認

```powershell
Get-ScheduledTask | Where-Object {$_.TaskName -like "GIG_SQL_*"}
```

---

## 🎯 タスクの管理

### タスク一覧の確認

```powershell
# GIG_SQLで始まるタスクを表示
Get-ScheduledTask | Where-Object {$_.TaskName -like "GIG_SQL_*"} | Format-Table TaskName, State, @{Label="Next Run";Expression={(Get-ScheduledTaskInfo -TaskName $_.TaskName).NextRunTime}}
```

### 特定のタスクを手動実行

```powershell
# エラー監視を即座に実行
Start-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring"

# 健全性チェックを即座に実行
Start-ScheduledTask -TaskName "GIG_SQL_HealthCheck"

# 更新失敗チェックを即座に実行
Start-ScheduledTask -TaskName "GIG_SQL_UpdateFailureCheck"

# データ更新（全件）を即座に実行
Start-ScheduledTask -TaskName "GIG_SQL_DailyBatch"
```

### タスクの有効化/無効化

```powershell
# タスクを無効化
Disable-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring"

# タスクを有効化
Enable-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring"
```

### タスクの削除

```powershell
# 特定のタスクを削除
Unregister-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring" -Confirm:$false

# すべてのGIG_SQLタスクを削除
Get-ScheduledTask | Where-Object {$_.TaskName -like "GIG_SQL_*"} | Unregister-ScheduledTask -Confirm:$false
```

---

## 📅 実行スケジュール

### 1日のタイムライン

```
00:00 - エラー監視（以降毎時）
01:00 - エラー監視
02:00 - エラー監視
03:00 - エラー監視
04:00 - エラー監視
05:00 - 📦 データ更新（全件） ⭐
06:00 - エラー監視
07:00 - エラー監視
08:00 - 🏥 健全性チェック ⭐
09:00 - エラー監視
...
23:00 - 🔍 更新失敗検知 ⭐
```

---

## 🔍 トラブルシューティング

### タスクが実行されない

#### 確認1: タスクの状態
```powershell
Get-ScheduledTask -TaskName "GIG_SQL_*"
```

**State**が`Ready`であることを確認

#### 確認2: 実行履歴
```powershell
Get-ScheduledTaskInfo -TaskName "GIG_SQL_ErrorMonitoring"
```

**LastRunTime**と**LastTaskResult**を確認

#### 確認3: 手動実行テスト
```powershell
Start-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring"
```

エラーが出る場合はログを確認

---

### タスクが失敗する

#### 原因1: Python仮想環境のパス
```powershell
# Python実行ファイルが存在するか確認
Test-Path "C:\DEV\jukust_mysql_sync_stmin\venv\Scripts\python.exe"
```

#### 原因2: 実行権限
```powershell
# タスクがSYSTEMユーザーで実行されているか確認
Get-ScheduledTask -TaskName "GIG_SQL_ErrorMonitoring" | Select-Object Principal
```

#### 原因3: ネットワークドライブ
- NASへのアクセス権限を確認
- SYSTEMユーザーでNASにアクセス可能か確認

---

## 📝 ログの確認

### タスク実行ログ

```powershell
# 健全性チェックのログ
Get-Content logs\health_check.log -Tail 50

# エラー監視のログ
Get-Content logs\datasets.log -Tail 50 | Select-String "ERROR"

# Windowsイベントログ
Get-EventLog -LogName Application -Source "Task Scheduler" -Newest 20
```

---

## 🔄 タスクの再登録

設定を変更した場合や問題が発生した場合：

```powershell
# 管理者PowerShellで実行
cd C:\DEV\jukust_mysql_sync_stmin
.\scripts\powershell\setup_tasks_fixed.ps1
```

---

## 📊 現在の登録状況

**登録日時**: 2025年10月8日

| タスク | 状態 | 次回実行 |
|---|:---:|---|
| **ErrorMonitoring** | ✅ Ready | 毎時0分 |
| **HealthCheck** | ✅ Ready | 2025-10-09 08:00 |
| **UpdateFailureCheck** | ✅ Ready | 2025-10-08 23:00 |
| **DailyBatch** | ✅ Ready | 2025-10-09 05:00 |

**すべてのタスクが正常に登録され、実行準備が完了しています！** ✅

---

## 🎯 ベストプラクティス

### 定期的なメンテナンス

#### 週次（毎週月曜）
```powershell
# タスクの実行状況を確認
Get-ScheduledTask | Where-Object {$_.TaskName -like "GIG_SQL_*"} | ForEach-Object {
    $Info = Get-ScheduledTaskInfo -TaskName $_.TaskName
    Write-Host "$($_.TaskName): LastRun=$($Info.LastRunTime), Result=$($Info.LastTaskResult)"
}
```

#### 月次
- タスク実行ログの確認
- エラー発生パターンの分析
- 必要に応じてスケジュール調整

---

## 📚 関連ドキュメント

- **エラー対応ガイド**: `docs/ERROR_HANDLING_GUIDE.md`
- **Slack通知ガイド**: `docs/SLACK_NOTIFICATION_GUIDE.md`
- **クイックリファレンス**: `docs/QUICK_ERROR_REFERENCE.md`

---

**最終更新**: 2025年10月8日
**ステータス**: タスクスケジューラ設定完了、4タスク登録済み

