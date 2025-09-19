"""
Google API認証モジュール

Google Sheets, Google Drive APIの認証を統一管理
"""
from oauth2client.service_account import ServiceAccountCredentials
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
import logging
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def authenticate_google_api(json_keyfile_path: str, scopes: list) -> ServiceAccountCredentials:
    """
    Google API認証処理
    
    Args:
        json_keyfile_path (str): JSONキーファイルのパス
        scopes (list): APIスコープのリスト
        
    Returns:
        ServiceAccountCredentials: 認証済みクレデンシャル
    """
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_keyfile_path, scopes
        )
        logger.info(f"Google API認証成功: {json_keyfile_path}")
        return credentials
    except Exception as e:
        logger.error(f"Google API認証失敗: {e}")
        raise


def retry_on_exception(func):
    """
    例外発生時のリトライデコレータ
    
    Args:
        func: リトライ対象の関数
        
    Returns:
        function: リトライ機能付き関数
    """
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


def before_sleep_log(retry_state):
    """リトライ前のログ出力"""
    logger.warning(f"リトライ実行: {retry_state.attempt_number}回目")