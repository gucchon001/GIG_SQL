"""
Streamlitアプリケーション用のエラー通知モジュール
"""
import os
import sys
from pathlib import Path
from datetime import datetime
import traceback

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

def send_streamlit_error_notification(error_message, error_type="ERROR", context=None):
    """
    StreamlitアプリのエラーをSlackに通知
    
    Args:
        error_message (str): エラーメッセージ
        error_type (str): エラータイプ（ERROR, WARNING, INFO）
        context (dict): 追加のコンテキスト情報
    """
    try:
        import requests
        import configparser
        
        # 設定ファイルを読み込む
        config = configparser.ConfigParser()
        config_path = project_root / 'config' / 'settings.ini'
        secrets_path = project_root / 'config' / 'secrets.env'
        
        config.read(config_path, encoding='utf-8')
        
        # secrets.envからWebhook URLを読み込む
        webhook_url = None
        if secrets_path.exists():
            with open(secrets_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('SLACK_WEBHOOK_URL='):
                        webhook_url = line.split('=', 1)[1]
                        break
        
        if not webhook_url:
            print("⚠️ SLACK_WEBHOOK_URLが設定されていません")
            return False
        
        # Slack設定を取得
        bot_name = config.get('Slack', 'BOT_NAME', fallback='Streamlit エラー通知')
        user_id = config.get('Slack', 'USER_ID', fallback='')
        
        # アイコン絵文字をエラータイプに応じて変更
        icon_emoji = {
            'ERROR': ':x:',
            'WARNING': ':warning:',
            'INFO': ':information_source:'
        }.get(error_type, ':exclamation:')
        
        # コンテキスト情報をフォーマット
        context_text = ""
        if context:
            context_text = "\n\n*追加情報:*\n"
            for key, value in context.items():
                context_text += f"• {key}: `{value}`\n"
        
        # メッセージを作成
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        message = f"""
🚨 *Streamlitアプリでエラーが発生しました*

*時刻:* {timestamp}
*エラータイプ:* {error_type}

*エラー内容:*
```
{error_message}
```
{context_text}
*アプリケーション:* Streamlit WebUI (ストミンくん v2.0)
"""
        
        if user_id:
            message = f"<@{user_id}>\n{message}"
        
        # Slackに送信
        payload = {
            "text": message,
            "username": bot_name,
            "icon_emoji": icon_emoji
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Streamlitエラー通知をSlackに送信しました: {error_type}")
            return True
        else:
            print(f"❌ Slack通知送信失敗: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ エラー通知の送信中に例外が発生: {e}")
        traceback.print_exc()
        return False

def notify_critical_error(error, additional_info=None):
    """
    重大なエラーを通知（データベース接続エラー、システムエラーなど）
    
    Args:
        error (Exception): 発生した例外
        additional_info (dict): 追加情報
    """
    error_message = f"{type(error).__name__}: {str(error)}"
    context = {
        "エラータイプ": type(error).__name__,
        "トレースバック": traceback.format_exc()[:500]  # 最初の500文字のみ
    }
    
    if additional_info:
        context.update(additional_info)
    
    return send_streamlit_error_notification(
        error_message,
        error_type="ERROR",
        context=context
    )

def notify_data_update_error(table_name, error):
    """
    データ更新エラーを通知
    
    Args:
        table_name (str): テーブル名
        error (Exception): 発生した例外
    """
    error_message = f"テーブル '{table_name}' の更新中にエラーが発生しました\n{str(error)}"
    context = {
        "テーブル名": table_name,
        "エラータイプ": type(error).__name__
    }
    
    return send_streamlit_error_notification(
        error_message,
        error_type="ERROR",
        context=context
    )

def notify_batch_execution_error(batch_type, error):
    """
    バッチ実行エラーを通知
    
    Args:
        batch_type (str): バッチタイプ（全件更新、個別更新）
        error (Exception): 発生した例外
    """
    error_message = f"{batch_type}の実行中にエラーが発生しました\n{str(error)}"
    context = {
        "バッチタイプ": batch_type,
        "エラータイプ": type(error).__name__
    }
    
    return send_streamlit_error_notification(
        error_message,
        error_type="ERROR",
        context=context
    )

def notify_warning(warning_message, context=None):
    """
    警告メッセージを通知（重要度は低い）
    
    Args:
        warning_message (str): 警告メッセージ
        context (dict): 追加情報
    """
    return send_streamlit_error_notification(
        warning_message,
        error_type="WARNING",
        context=context
    )

