# 指定テーブル実行スクリプト

既存プログラムを流用して指定されたテーブルのみを実行するスクリプトです。

## ファイル構成

- `scripts/powershell/run_specific_table.ps1` - PowerShellラッパースクリプト
- `scripts/python/run_specific_table.py` - Python実行スクリプト

## 使用方法

### PowerShell経由での実行（推奨）

```powershell
# 基本的な使用方法
.\scripts\powershell\run_specific_table.ps1 -TableName "請求書DL状況"

# 出力パスを指定
.\scripts\powershell\run_specific_table.ps1 -TableName "請求書DL状況" -OutputPath "data_Parquet"

# 利用可能なテーブル一覧を表示
python scripts\python\run_specific_table.py --list
```

### Python直接実行

```bash
# 基本的な使用方法
python scripts\python\run_specific_table.py "請求書DL状況"

# 出力パスを指定
python scripts\python\run_specific_table.py "請求書DL状況" "data_Parquet"

# 利用可能なテーブル一覧を表示
python scripts\python\run_specific_table.py --list
```

## パラメータ

### PowerShellスクリプト

- `-TableName` (必須): 実行するテーブル名（CSVファイル呼称）
- `-OutputPath` (オプション): 出力先パス（デフォルト: "data_Parquet"）

### Pythonスクリプト

- `table_name` (必須): 実行するテーブル名（CSVファイル呼称）
- `output_path` (オプション): 出力先パス（デフォルト: "data_Parquet"）

## 機能

### 既存プログラムの流用

1. **設定管理**: `core.config.config_loader.load_config()` を使用
2. **データベース接続**: `core.utils.db_utils.get_connection()` を使用
3. **SQLファイル読み込み**: `core.data.subcode_loader.load_sql_from_file()` を使用
4. **データ型変換**: `core.data.subcode_loader.get_data_types()` と `apply_data_types_to_df()` を使用
5. **スプレッドシート連携**: `core.data.subcode_loader.load_sql_file_list_from_spreadsheet()` を使用

### 主な特徴

- ✅ 既存の設定ファイル（`config/settings.ini`）を使用
- ✅ Googleスプレッドシートからテーブル情報を自動取得
- ✅ 期間条件の自動適用
- ✅ データ型の自動変換
- ✅ ローカルパスまたはNASパスへの出力対応
- ✅ UTF-8エンコーディング対応
- ✅ エラーハンドリングとログ出力

## 利用可能なテーブル一覧

```bash
python scripts\python\run_specific_table.py --list
```

実行すると以下のような一覧が表示されます：

```
利用可能なテーブル一覧:
----------------------------------------
 1. 企業一覧 (1_companies.sql)
 2. 担当者一覧 (2_client_users_admin.sql)
 3. 教室グループ一覧 (3_brands.sql)
 4. 契約・契約更新管理一覧 (4-5_contracts.sql)
 ...
```

## 実行例

### 成功例

```powershell
PS> .\scripts\powershell\run_specific_table.ps1 -TableName "請求書DL状況"

===============================================
指定テーブル実行スクリプト
===============================================
テーブル名: 請求書DL状況
出力先: data_Parquet
[OK] 仮想環境をアクティベートしました
[INFO] 指定テーブルの実行を開始します...
============================================================
指定テーブル実行: 請求書DL状況
============================================================
[INFO] データベース接続中...
[OK] データベース接続成功
[INFO] スプレッドシートからテーブル情報を取得中...
[INFO] 対象テーブル情報:
  SQLファイル: invoices_dl.sql
  CSVファイル名: invoices_dl
  メインテーブル名: invoices
  カテゴリ: ストミン
[INFO] SQLファイル読み込み: invoices_dl.sql
[OK] SQLファイル読み込み成功 (650 文字)
[INFO] データベースクエリ実行中...
[OK] データ取得成功: 7965 件
[INFO] データ型変換中...
[OK] データ型変換完了
[INFO] 出力先: data_Parquet\invoices_dl.parquet
[INFO] Parquetファイル保存中...
[OK] Parquetファイル保存成功: data_Parquet\invoices_dl.parquet
[INFO] 保存件数: 7965 件
[INFO] カラム数: 10 列
[OK] データベース接続終了
============================================================
[SUCCESS] テーブル '請求書DL状況' の実行が完了しました
============================================================
===============================================
[SUCCESS] テーブル '請求書DL状況' の実行が完了しました
===============================================
```

### エラー例

```powershell
PS> .\scripts\powershell\run_specific_table.ps1 -TableName "存在しないテーブル"

[ERROR] テーブル '存在しないテーブル' が見つかりません
[INFO] 利用可能なテーブル名:
  - 企業一覧
  - 担当者一覧
  - 教室グループ一覧
  ...
```

## トラブルシューティング

### よくある問題

1. **テーブル名が見つからない**
   - `--list` オプションで正確なテーブル名を確認してください
   - テーブル名は「CSVファイル呼称」列の値を使用してください

2. **データベース接続エラー**
   - `config/settings.ini` の設定を確認してください
   - SSH接続とMySQL接続の設定が正しいか確認してください

3. **権限エラー**
   - 出力先ディレクトリの書き込み権限を確認してください
   - ローカルパス（`data_Parquet`）を使用することを推奨します

### ログ確認

実行時の詳細なログは以下で確認できます：
- `logs/app.log` - アプリケーションログ
- `logs/datasets.log` - データセット処理ログ

## 注意事項

- このスクリプトは既存のプログラムを流用しているため、設定ファイルの整合性が重要です
- 初回実行時は仮想環境のアクティベートに時間がかかる場合があります
- 大きなテーブルの処理には時間がかかる場合があります
