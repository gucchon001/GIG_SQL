# 設定ファイル説明

## 📂 ファイル構成

### `settings.ini`
- **用途**: 一般設定（非秘匿情報）
- **内容**: ホスト名、ポート、パス、パフォーマンス設定
- **Git管理**: ✅ コミット対象

### `secrets.env`
- **用途**: 秘匿情報（セキュリティ重要）
- **内容**: パスワード、APIキー、認証情報
- **Git管理**: ❌ .gitignoreに追加必須

## 🔧 環境変数の設定

```bash
# 環境変数として読み込み
source config/secrets.env
```

## ⚠️ セキュリティ注意事項

1. **secrets.envは絶対にGitにコミットしない**
2. **本番環境では環境変数で設定**
3. **定期的にパスワード・キーのローテーション**

## 🔄 移行作業

従来の`config.ini`から新構造への移行：

1. 秘匿情報を`secrets.env`に移動
2. 一般設定を`settings.ini`に移動
3. アプリケーションコードを新構造に対応
4. `.gitignore`に`config/secrets.env`を追加

## 📝 設定例

### 開発環境
```env
# config/secrets.env (開発用)
MYSQL_PASSWORD=dev_password
SLACK_WEBHOOK_URL=https://hooks.slack.com/dev/...
```

### 本番環境
```env
# 環境変数として設定
export MYSQL_PASSWORD="prod_password"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/prod/..."
```