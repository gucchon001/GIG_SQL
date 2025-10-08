"""
エラー通知スクリプト
ログファイルから最新のエラーを検出してSlackに通知
"""
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils import slack_notify
import configparser

def load_config():
    """設定ファイルとsecrets.envを読み込む"""
    config = configparser.ConfigParser()
    config_path = project_root / 'config' / 'settings.ini'
    secrets_path = project_root / 'config' / 'secrets.env'
    
    config.read(config_path, encoding='utf-8')
    
    # secrets.envから環境変数を読み込む
    if secrets_path.exists():
        with open(secrets_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    
    # Slack設定をconfigに追加
    if 'Slack' not in config:
        config.add_section('Slack')
    
    config['Slack']['SLACK_WEBHOOK_URL'] = os.environ.get('SLACK_WEBHOOK_URL', '')
    
    return config

def check_recent_errors(log_file, hours=1):
    """指定時間内のエラーをチェック"""
    if not os.path.exists(log_file):
        return []
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    errors = []
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                if 'ERROR' in line or 'エラー' in line or '失敗' in line:
                    # タイムスタンプを抽出
                    if line[:19].replace('-', '').replace(':', '').replace(' ', '').isdigit():
                        try:
                            log_time = datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                            if log_time > cutoff_time:
                                errors.append(line.strip())
                        except:
                            pass
    except Exception as e:
        print(f"ログファイル読み込みエラー: {e}")
    
    return errors

def send_error_summary(config, errors, log_name):
    """エラーサマリーをSlackに送信"""
    if not errors:
        return
    
    webhook_url = config['Slack']['SLACK_WEBHOOK_URL']
    bot_name = config['Slack'].get('BOT_NAME', 'エラー通知Bot')
    user_id = config['Slack'].get('USER_ID', '')
    icon_emoji = config['Slack'].get('ICON_EMOJI', ':warning:')
    
    error_count = len(errors)
    error_preview = '\n'.join(errors[:3])  # 最初の3件のみ表示
    
    message = f"""
🚨 *{log_name}でエラーを検出しました*

*エラー件数*: {error_count}件

*最新のエラー（最大3件）*:
```
{error_preview}
```

詳細はログファイルを確認してください。
ログファイル: `{log_name}`
"""
    
    if user_id:
        message = f"<@{user_id}>\n{message}"
    
    payload = {
        "text": message,
        "username": bot_name,
        "icon_emoji": icon_emoji
    }
    
    try:
        import requests
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print(f"✅ Slack通知送信成功: {error_count}件のエラーを通知しました")
        else:
            print(f"❌ Slack通知送信失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ Slack通知送信エラー: {e}")

def main():
    """メイン処理"""
    print("エラー通知スクリプト開始")
    print("-" * 50)
    
    # 設定読み込み
    config = load_config()
    
    # 過去1時間のエラーをチェック
    logs_dir = project_root / 'logs'
    
    # datasets.logをチェック
    datasets_log = logs_dir / 'datasets.log'
    datasets_errors = check_recent_errors(datasets_log, hours=1)
    if datasets_errors:
        print(f"⚠️  datasets.log: {len(datasets_errors)}件のエラー")
        send_error_summary(config, datasets_errors, 'logs/datasets.log')
    else:
        print("✅ datasets.log: エラーなし")
    
    # main.logをチェック
    main_log = logs_dir / 'main.log'
    main_errors = check_recent_errors(main_log, hours=1)
    if main_errors:
        print(f"⚠️  main.log: {len(main_errors)}件のエラー")
        send_error_summary(config, main_errors, 'logs/main.log')
    else:
        print("✅ main.log: エラーなし")
    
    print("-" * 50)
    print("エラー通知スクリプト完了")

if __name__ == '__main__':
    main()

