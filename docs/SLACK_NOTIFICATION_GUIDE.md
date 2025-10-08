# 📢 Slack通知ガイド

## 📋 目次
1. [概要](#概要)
2. [通知が送信されるタイミング](#通知が送信されるタイミング)
3. [通知内容](#通知内容)
4. [手動でSlack通知を送信](#手動でslack通知を送信)
5. [設定方法](#設定方法)
6. [トラブルシューティング](#トラブルシューティング)

---

## 📢 概要

本システムでは、エラー発生時に自動的にSlackへ通知を送信する機能が実装されています。

### ✅ 実装済みの機能

1. **自動エラー通知**: `common_exe_functions.py`内で重大なエラー発生時に自動送信
2. **手動エラー通知**: エラー監視スクリプトから手動送信
3. **バッチ完了後の通知**: `create_datasets.ps1`実行エラー時に自動送信

---

## ⏰ 通知が送信されるタイミング

### 🔴 自動通知（即座）

以下のエラー発生時に自動的にSlack通知が送信されます：

1. **設定ファイル読み込みエラー**
   ```
   設定ファイルの読み込み中にエラーが発生しました
   ```

2. **Google Sheetsアクセスエラー**
   ```
   GoogleスプレッドシートからSQLファイルリストをロード中にエラーが発生しました
   ```

3. **SSHトンネル作成エラー**
   ```
   SSHトンネルの作成中にエラーが発生しました
   ```

4. **SQLファイル処理エラー**
   ```
   SQLおよびCSVファイルの処理中にエラーが発生しました
   ```

5. **バッチ実行エラー（リトライ3回失敗後）**
   ```
   create_datasets.ps1の実行が3回のリトライ後も失敗
   ```

6. **データ更新失敗（定期チェック）** 🆕
   ```
   24時間以上更新されていないファイルを検出
   バッチ実行ログから失敗を検出
   ```

### 🟡 手動通知（任意）

以下のコマンドで手動送信可能：

```powershell
# エラー監視+Slack通知
.\scripts\powershell\monitor_errors.ps1 -SendSlackNotification

# 直接エラー通知を送信
python scripts\python\notify_error.py

# データ更新失敗チェック+通知 🆕
.\scripts\powershell\check_update_status.ps1 -SendNotification

# エラー通知のテスト 🆕
python scripts\python\test_error_notification.py
```

---

## 📝 通知内容

### 通知フォーマット

```
🚨 エラー通知

エラー件数: X件

最新のエラー（最大3件）:
```
エラーメッセージ1
エラーメッセージ2
エラーメッセージ3
```

詳細はログファイルを確認してください。
ログファイル: logs/datasets.log
```

### 通知される情報

- **エラー件数**: 過去1時間以内のエラー数
- **エラーメッセージ**: 最新の3件まで表示
- **対象ログファイル**: `datasets.log` または `main.log`
- **メンション**: 設定されたユーザーIDにメンション

---

## 🔧 手動でSlack通知を送信

### 方法1: エラー監視スクリプト経由

```powershell
# 過去24時間のエラーをチェック+通知
.\scripts\powershell\monitor_errors.ps1 -SendSlackNotification

# 過去1時間のエラーをチェック+通知
.\scripts\powershell\monitor_errors.ps1 -Hours 1 -SendSlackNotification
```

### 方法2: Pythonスクリプト直接実行

```powershell
# 過去1時間のエラーをSlackに通知
python scripts\python\notify_error.py
```

### 方法3: PowerShellから直接送信

```powershell
# テスト通知を送信
$webhook = $env:SLACK_WEBHOOK_URL
$body = @{
    text = "テスト通知: システムは正常に動作しています"
    username = "CSVダウンロードツール"
    icon_emoji = ":white_check_mark:"
} | ConvertTo-Json

Invoke-RestMethod -Uri $webhook -Method Post -Body $body -ContentType 'application/json'
```

---

## ⚙️ 設定方法

### 1. Webhook URLの確認

```powershell
# secrets.envを確認
Get-Content config\secrets.env | Select-String "SLACK_WEBHOOK_URL"
```

既に設定済み：
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T78MU5QM9/B06GSM7JC2H/...
```

### 2. 通知先ユーザーの設定

`config/settings.ini`の`[Slack]`セクション：

```ini
[Slack]
BOT_NAME = CSVダウンロードツール
USER_ID = U9GCBHDSS  # 通知先ユーザーのSlack ID
ICON_EMOJI = :laptop:
```

### 3. ユーザーIDの確認方法

Slackで以下の手順でユーザーIDを確認：
1. Slackでプロフィールを開く
2. 「...」メニューをクリック
3. 「メンバーIDをコピー」を選択

---

## 🔍 トラブルシューティング

### ❌ Slack通知が送信されない

#### 確認1: Webhook URLの確認
```powershell
# Webhook URLが設定されているか確認
$env:SLACK_WEBHOOK_URL
# または
Get-Content config\secrets.env | Select-String "SLACK"
```

#### 確認2: テスト送信
```powershell
# テスト通知を送信
python scripts\python\notify_error.py
```

#### 確認3: ログ確認
```powershell
# エラーログを確認
Get-Content logs\datasets.log -Tail 50 | Select-String "Slack"
```

### ⚠️ 「Slack通知の送信に失敗しました」エラー

**原因と対処法**:

1. **Webhook URLが無効**
   ```powershell
   # secrets.envを確認
   code config\secrets.env
   ```
   Webhook URLが正しいことを確認

2. **ネットワーク接続エラー**
   ```powershell
   # インターネット接続を確認
   Test-Connection -ComputerName hooks.slack.com -Count 2
   ```

3. **Pythonモジュールが不足**
   ```powershell
   # requestsモジュールを確認
   pip show requests
   
   # インストールされていない場合
   pip install requests
   ```

### 🔄 通知が重複して送信される

**原因**: 自動通知と手動通知の両方が実行されている

**対処法**:
- バッチ実行では自動通知のみに依存
- 手動通知は必要な場合のみ実行

---

## 📊 通知の頻度制御

### 推奨設定

#### 本番環境
- **自動通知**: 有効（エラー発生時のみ）
- **手動通知**: 日次健全性チェック時のみ

#### 開発環境
- **自動通知**: 必要に応じて無効化可能
- **手動通知**: テスト時に使用

### 通知を一時的に無効化する方法

```powershell
# 環境変数を一時的にクリア
$env:SLACK_WEBHOOK_URL = ""

# スクリプト実行
.\scripts\powershell\create_datasets.ps1

# 環境変数を元に戻す
$env:SLACK_WEBHOOK_URL = (Get-Content config\secrets.env | Select-String "SLACK_WEBHOOK_URL").ToString().Split('=')[1]
```

---

## 🎯 ベストプラクティス

### 1. 定期的な通知テスト（月次）
```powershell
# 月に1回、通知機能をテスト
python scripts\python\notify_error.py
```

### 2. エラー監視の自動化（推奨）
```powershell
# タスクスケジューラで毎朝実行
# 過去24時間のエラーをチェック+通知
.\scripts\powershell\monitor_errors.ps1 -Hours 24 -SendSlackNotification
```

### 3. 重要なエラーのみ通知
- データベース接続エラー: ✅ 通知
- 一時的なネットワークエラー: ⚠️ リトライ後も失敗なら通知
- 軽微なWarning: ❌ 通知不要

---

## 📞 サポート

通知が正常に動作しない場合：

1. **ログファイルを確認**
   ```powershell
   Get-Content logs\datasets.log -Tail 100 | Select-String "Slack|ERROR"
   ```

2. **設定ファイルを確認**
   ```powershell
   Get-Content config\settings.ini | Select-String "Slack" -Context 5
   Get-Content config\secrets.env | Select-String "SLACK"
   ```

3. **テスト送信を実行**
   ```powershell
   python scripts\python\notify_error.py
   ```

---

## 📝 変更履歴

- **2025-10-08**: 初版作成
  - 自動通知機能の追加
  - 手動通知スクリプトの追加
  - エラー監視との統合

---

**最終更新**: 2025年10月8日

