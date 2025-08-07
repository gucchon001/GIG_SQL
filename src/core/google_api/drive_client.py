"""
Google Drive API クライント

Google Drive APIを使用したファイル操作機能を提供
"""
import sys
import os
from typing import Optional, List, Dict, Any
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.logging.logger import get_logger


class GoogleDriveClient:
    """Google Drive API クライアントクラス"""
    
    def __init__(self, credentials_file: str):
        """
        Google Drive クライアントを初期化
        
        Args:
            credentials_file: 認証情報JSONファイルのパス
        """
        self.credentials_file = credentials_file
        self.logger = get_logger(__name__)
        self.scopes = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self._credentials = None
        self._service = None
    
    def _authenticate(self):
        """Google API認証を実行"""
        if self._credentials is None:
            try:
                self._credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    self.credentials_file,
                    self.scopes
                )
                self.logger.info("Google API認証が完了しました")
            except Exception as e:
                self.logger.error(f"Google API認証エラー: {e}")
                raise
    
    def _get_service(self):
        """Google Drive サービスオブジェクトを取得"""
        if self._service is None:
            self._authenticate()
            self._service = build('drive', 'v3', credentials=self._credentials)
        return self._service
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def download_file(self, file_id: str, file_name: str) -> Optional[str]:
        """
        Google DriveからファイルをダウンロードしてContent取得
        
        Args:
            file_id: ファイルID
            file_name: ファイル名
            
        Returns:
            str: ファイルの内容（失敗時はNone）
        """
        try:
            service = self._get_service()
            
            # ファイル情報を取得
            file_metadata = service.files().get(fileId=file_id).execute()
            self.logger.info(f"ファイル情報取得: {file_metadata.get('name')}")
            
            # ファイル内容を取得
            request = service.files().get_media(fileId=file_id)
            file_content = request.execute()
            
            # バイナリデータを文字列に変換
            content = file_content.decode('utf-8')
            
            self.logger.info(f"ファイルダウンロード完了: {file_name}")
            return content
            
        except Exception as e:
            self.logger.error(f"ファイルダウンロードエラー: {file_name}, {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def search_files(
        self,
        folder_id: str,
        file_name: Optional[str] = None,
        mime_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Google Drive内のファイルを検索
        
        Args:
            folder_id: 検索対象フォルダID
            file_name: ファイル名（部分一致）
            mime_type: MIMEタイプ
            
        Returns:
            List[Dict]: 検索結果のファイルリスト
        """
        try:
            service = self._get_service()
            
            # 検索クエリを構築
            query_parts = [f"'{folder_id}' in parents"]
            
            if file_name:
                query_parts.append(f"name contains '{file_name}'")
            
            if mime_type:
                query_parts.append(f"mimeType = '{mime_type}'")
            
            query = " and ".join(query_parts)
            
            # ファイル検索実行
            results = service.files().list(
                q=query,
                fields="files(id, name, mimeType, modifiedTime)"
            ).execute()
            
            files = results.get('files', [])
            self.logger.info(f"ファイル検索完了: {len(files)} 件見つかりました")
            
            return files
            
        except Exception as e:
            self.logger.error(f"ファイル検索エラー: {e}")
            return []
    
    def get_file_by_name(self, folder_id: str, file_name: str) -> Optional[str]:
        """
        ファイル名でファイルを検索してIDを取得
        
        Args:
            folder_id: 検索対象フォルダID
            file_name: ファイル名
            
        Returns:
            str: ファイルID（見つからない場合はNone）
        """
        try:
            files = self.search_files(folder_id, file_name)
            
            # 完全一致するファイルを検索
            for file in files:
                if file['name'] == file_name:
                    self.logger.info(f"ファイルを発見: {file_name} (ID: {file['id']})")
                    return file['id']
            
            self.logger.warning(f"ファイルが見つかりません: {file_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"ファイル検索エラー: {file_name}, {e}")
            return None
    
    def load_sql_file(self, folder_id: str, sql_file_name: str) -> Optional[str]:
        """
        SQLファイルを読み込み
        
        Args:
            folder_id: SQLファイルが格納されているフォルダID
            sql_file_name: SQLファイル名
            
        Returns:
            str: SQLファイルの内容（失敗時はNone）
        """
        try:
            # ファイルIDを取得
            file_id = self.get_file_by_name(folder_id, sql_file_name)
            if not file_id:
                return None
            
            # ファイル内容を取得
            sql_content = self.download_file(file_id, sql_file_name)
            
            if sql_content:
                self.logger.info(f"SQLファイル読み込み完了: {sql_file_name}")
                return sql_content
            else:
                self.logger.error(f"SQLファイル読み込み失敗: {sql_file_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"SQLファイル読み込みエラー: {sql_file_name}, {e}")
            return None


# 後方互換性のための関数
def authenticate_google_api(json_keyfile_path: str, scopes: List[str]):
    """
    旧式のGoogle API認証関数（後方互換性のため）
    
    Args:
        json_keyfile_path: 認証情報JSONファイルのパス
        scopes: 認証スコープのリスト
        
    Returns:
        認証情報オブジェクト
    """
    return ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scopes)


def load_sql_from_file(google_folder_id: str, sql_file_name: str, json_keyfile_path: str) -> Optional[str]:
    """
    旧式のSQLファイル読み込み関数（後方互換性のため）
    
    Args:
        google_folder_id: Google DriveフォルダID
        sql_file_name: SQLファイル名
        json_keyfile_path: 認証情報JSONファイルのパス
        
    Returns:
        str: SQLファイルの内容（失敗時はNone）
    """
    client = GoogleDriveClient(json_keyfile_path)
    return client.load_sql_file(google_folder_id, sql_file_name)