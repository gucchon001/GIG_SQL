"""
データ更新失敗検知スクリプト
ボタンを押したのに実際には更新されていないケースを検知してSlackに通知
"""
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import configparser

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils import slack_notify

def load_config():
    """設定ファイルとsecrets.envを読み込む"""
    config = configparser.ConfigParser()
    config_path = project_root / 'config' / 'settings.ini'
    secrets_path = project_root / 'config' / 'secrets.env'
    
    config.read(config_path, encoding='utf-8-sig')
    
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

def check_parquet_files_freshness(config, hours=24):
    """
    Parquetファイルの更新状況をチェック
    指定時間内に更新されていないファイルを検出
    """
    base_path = config['Paths']['csv_base_path']
    
    if not os.path.exists(base_path):
        print(f"❌ ベースパスが存在しません: {base_path}")
        return []
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    stale_files = []
    
    import glob
    parquet_files = glob.glob(os.path.join(base_path, '*.parquet'))
    
    for file_path in parquet_files:
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if mtime < cutoff_time:
                file_name = os.path.basename(file_path)
                hours_old = (datetime.now() - mtime).total_seconds() / 3600
                stale_files.append({
                    'file': file_name,
                    'last_modified': mtime.strftime('%Y-%m-%d %H:%M:%S'),
                    'hours_old': hours_old
                })
        except Exception as e:
            print(f"⚠️ ファイルチェックエラー: {file_path} - {e}")
    
    return stale_files

def check_batch_execution_log(hours=1):
    """
    バッチ実行ログから実行失敗を検出
    """
    log_file = project_root / 'logs' / 'datasets.log'
    
    if not log_file.exists():
        return []
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    failures = []
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                # 失敗ログを検出
                if '★失敗★' in line or '失敗' in line:
                    if line[:19].replace('-', '').replace(':', '').replace(' ', '').isdigit():
                        try:
                            log_time = datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                            if log_time > cutoff_time:
                                failures.append(line.strip())
                        except:
                            pass
    except Exception as e:
        print(f"ログファイル読み込みエラー: {e}")
    
    return failures

def send_update_failure_notification(config, stale_files, execution_failures):
    """
    更新失敗をSlackに通知
    """
    if not stale_files and not execution_failures:
        print("✅ 更新失敗は検出されませんでした")
        return
    
    webhook_url = config['Slack']['SLACK_WEBHOOK_URL']
    bot_name = config['Slack'].get('BOT_NAME', 'データ更新監視Bot')
    user_id = config['Slack'].get('USER_ID', '')
    
    message_parts = ["🚨 *データ更新の問題を検出しました*\n"]
    
    # 古いファイルの通知
    if stale_files:
        message_parts.append(f"\n*⏰ 長時間更新されていないファイル: {len(stale_files)}件*\n")
        for item in stale_files[:5]:  # 最大5件
            hours = int(item['hours_old'])
            message_parts.append(f"• `{item['file']}`: {hours}時間前 ({item['last_modified']})\n")
        
        if len(stale_files) > 5:
            message_parts.append(f"... 他 {len(stale_files) - 5}件\n")
    
    # 実行失敗の通知
    if execution_failures:
        message_parts.append(f"\n*❌ 実行失敗ログ: {len(execution_failures)}件*\n")
        for failure in execution_failures[:3]:  # 最大3件
            message_parts.append(f"```{failure}```\n")
    
    message_parts.append("\n*推奨アクション*:\n")
    message_parts.append("1. ログファイルを確認: `logs/datasets.log`\n")
    message_parts.append("2. データ更新（全件）を手動実行\n")
    message_parts.append("3. エラーが継続する場合はシステム管理者に連絡\n")
    
    message = ''.join(message_parts)
    
    if user_id:
        message = f"<@{user_id}>\n{message}"
    
    payload = {
        "text": message,
        "username": bot_name,
        "icon_emoji": ":warning:"
    }
    
    try:
        import requests
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"✅ 更新失敗通知をSlackに送信しました")
            return True
        else:
            print(f"❌ Slack通知送信失敗: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Slack通知送信エラー: {e}")
        return False

def main():
    """メイン処理"""
    print("データ更新失敗検知スクリプト開始")
    print("-" * 50)
    
    # 設定読み込み
    config = load_config()
    
    # 24時間以上更新されていないファイルをチェック
    print("📊 Parquetファイルの更新状況をチェック中...")
    stale_files = check_parquet_files_freshness(config, hours=24)
    
    if stale_files:
        print(f"⚠️  {len(stale_files)}件のファイルが24時間以上更新されていません")
        for item in stale_files[:5]:
            print(f"  - {item['file']}: {int(item['hours_old'])}時間前")
    else:
        print("✅ すべてのファイルが24時間以内に更新されています")
    
    # 過去1時間の実行失敗をチェック
    print("\n📊 バッチ実行ログをチェック中...")
    execution_failures = check_batch_execution_log(hours=1)
    
    if execution_failures:
        print(f"⚠️  {len(execution_failures)}件の実行失敗を検出")
    else:
        print("✅ 実行失敗なし")
    
    # Slack通知
    if stale_files or execution_failures:
        print("\n📢 Slack通知を送信中...")
        send_update_failure_notification(config, stale_files, execution_failures)
    
    print("-" * 50)
    print("データ更新失敗検知スクリプト完了")

if __name__ == '__main__':
    main()
