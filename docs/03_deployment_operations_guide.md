# デプロイメント・運用ガイド

## 環境構成

### 開発環境
- **場所**: ローカル開発マシン
- **Python**: 3.9+
- **仮想環境**: `venv`（`C:\dev\GIG塾STMYSQL\sourcecode\venv`）
- **データベース**: AWS RDS（読み取り専用接続）

### 本番環境
- **実行方式**: 定期バッチ + オンデマンド実行
- **スケジューラー**: Windows タスクスケジューラー / PowerShell
- **データ保存**: ローカルファイルシステム + Google Drive

## インストール・セットアップ

### 1. 前提条件
- Python 3.9 以上
- Git
- PowerShell 5.0 以上
- ネットワーク接続（AWS RDS, Google API）

### 2. リポジトリクローン
```bash
git clone <repository-url>
cd GIG塾STMYSQL/sourcecode
```

### 3. 仮想環境セットアップ
```bash
# 仮想環境作成
python -m venv venv

# 仮想環境アクティベート（PowerShell）
.\venv\Scripts\Activate.ps1

# 依存関係インストール
pip install -r requirements.txt
```

### 4. 設定ファイル配置
```ini
# config.ini の設定例
[SSH]
host = ec2-xxx.amazonaws.com
user = jump
ssh_key_path = path\to\private-key.pem

[MySQL]
host = rds-cluster.amazonaws.com
port = 3306
user = viewer
password = your-password
database = your-database

[Credentials]
json_keyfile_path = path\to\service-account.json

[Spreadsheet]
spreadsheet_id = your-spreadsheet-id
main_sheet = 実行シート
rawdata_sheet = rawdata実行シート
eachdata_sheet = 個別実行シート
```

### 5. Google認証設定
1. Google Cloud Console でサービスアカウント作成
2. Drive API, Sheets API の有効化
3. サービスアカウントキー（JSON）をダウンロード
4. `json_keyfile_path` にファイルパスを設定

## 実行方法

### 定期バッチシステム

#### PowerShell実行（推奨）
```powershell
# 本番バッチ実行
.\run.ps1

# テストバッチ実行  
.\run_test.ps1
```

#### 直接Python実行
```bash
# 本番データ処理
python main.py

# テストデータ処理
python main_test.py

# 生データ処理
python main_rawdata.py
```

### ストミン データソース生成

#### PowerShell実行（未実装）
```powershell
# 予定：ストミン用PowerShellスクリプト
.\run_streamlit_data.ps1
```

#### 直接Python実行
```bash
# 全データソース更新
python run_create_datesets.py

# 個別テーブル更新
python run_create_datasets_individual.py
```

### Streamlit WebUI起動
```bash
# WebUI起動
streamlit run streamlit_app.py

# アクセスURL: http://localhost:8501
```

## 定期実行設定

### Windows タスクスケジューラー設定例
```xml
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4">
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2024-01-01T02:00:00</StartBoundary>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Actions>
    <Exec>
      <Command>powershell.exe</Command>
      <Arguments>-File "C:\path\to\run.ps1"</Arguments>
      <WorkingDirectory>C:\path\to\sourcecode</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
```

## ログ管理

### ログファイル構成
```
sourcecode/
├── app.log          # 現在のログ
├── app.log.1        # ローテーションログ
├── app.log.2        
└── app.log.10       # 最大10ファイル保持
```

### ログレベル設定
```ini
[logging]
level = DEBUG          # DEBUG, INFO, WARNING, ERROR
logfile = app.log
```

### ログ監視コマンド
```powershell
# リアルタイム監視
Get-Content app.log -Wait -Tail 50

# エラーログ抽出
Select-String -Path app.log -Pattern "ERROR"

# 特定期間のログ
Get-Content app.log | Where-Object { $_ -match "2024-01-01" }
```

## トラブルシューティング

### よくある問題と解決方法

#### 1. MySQL接続エラー
```
Error: Lost connection to MySQL server during query
```
**原因**: ネットワーク不安定、長時間クエリ
**解決策**:
- SSH接続の確認
- クエリのチューニング
- リトライ機能の確認

#### 2. Google API認証エラー
```
Error: 403 Forbidden
```
**原因**: サービスアカウント権限不足
**解決策**:
- スプレッドシート共有設定確認
- サービスアカウントの権限確認
- APIクォータ制限確認

#### 3. Parquetファイル出力エラー
```
Error: [Errno 2] No such file or directory: 'data_Parquet/'
```
**原因**: 出力ディレクトリ不存在
**解決策**:
```python
import os
output_dir = 'data_Parquet'
os.makedirs(output_dir, exist_ok=True)
```

#### 4. 仮想環境アクティベートエラー
```
Error: Execution of scripts is disabled on this system
```
**原因**: PowerShell実行ポリシー制限
**解決策**:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## パフォーマンス最適化

### 1. データベース接続
- 接続プールの活用
- 適切なタイムアウト設定
- SSH接続の安定化

### 2. 大容量データ処理
```python
# チャンク処理の最適化
chunk_size = 10000  # レコード数
batch_size = 10000  # バッチサイズ
max_workers = 5     # 並列処理数
delay = 0.5         # 遅延時間（秒）
```

### 3. ファイルI/O最適化
- Parquet形式の活用
- 圧縮設定の調整
- 並列書き込み

## セキュリティ対策

### 1. 認証情報管理
- config.ini のアクセス権限制限
- サービスアカウントキーの安全な保管
- SSH秘密鍵の適切な管理

### 2. ネットワークセキュリティ
- SSH トンネル経由のDB接続
- VPN接続の推奨
- ファイアウォール設定

### 3. アクセス制御
- 読み取り専用DBユーザーの使用
- 最小権限の原則
- 監査ログの記録

## バックアップ・復旧

### 1. 設定ファイルバックアップ
```powershell
# 設定ファイルのバックアップ
Copy-Item config.ini "backup/config_$(Get-Date -Format 'yyyyMMdd').ini"
```

### 2. データバックアップ
- Parquetファイルの定期バックアップ
- Google Drive上のSQLファイル
- ログファイルのアーカイブ

### 3. 復旧手順
1. 設定ファイルの復元
2. 依存関係の再インストール
3. 認証情報の再設定
4. 動作確認テスト

## 監視・アラート

### 1. Slack通知設定
```ini
[Slack]
SLACK_WEBHOOK_URL = https://hooks.slack.com/services/xxx
BOT_NAME = CSVダウンロードツール
USER_ID = U9GCBHDSS
ICON_EMOJI = :laptop:
```

### 2. システム監視項目
- バッチ実行成功/失敗
- データベース接続状態
- ファイル出力状況
- ディスク使用量

### 3. アラート条件
- 連続実行失敗（3回以上）
- 実行時間異常（通常の2倍以上）
- ディスク容量不足（80%以上）
- ネットワーク接続断

## 定期メンテナンス

### 日次
- ログファイル確認
- 実行結果確認
- エラー有無確認

### 週次
- Parquetファイルサイズ確認
- ディスク使用量確認
- 長時間実行クエリ確認

### 月次
- ログファイルアーカイブ
- 不要ファイル削除
- パフォーマンス分析
- セキュリティパッチ適用