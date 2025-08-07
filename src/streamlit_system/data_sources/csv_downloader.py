"""
CSV ダウンロード機能

SQLクエリの結果をCSV形式でダウンロード提供
"""
import sys
import os
import io
import pandas as pd
from typing import Optional, Dict, Any

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.config.settings import AppConfig
from src.core.database.connection import DatabaseConnection
from src.core.database.ssh_tunnel import SSHTunnel
from src.core.logging.logger import get_logger


class CSVDownloader:
    """CSV ダウンロード管理クラス"""
    
    def __init__(self, config: AppConfig):
        """
        CSV ダウンローダーを初期化
        
        Args:
            config: アプリケーション設定
        """
        self.config = config
        self.logger = get_logger(__name__)
    
    def generate_csv(self, sql_file: str, conditions: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        SQLファイルを実行してCSVデータを生成
        
        Args:
            sql_file: SQLファイル名
            conditions: 追加条件
            
        Returns:
            str: CSV文字列データ（失敗時はNone）
        """
        try:
            # データを取得
            data = self._execute_sql_file(sql_file, conditions)
            
            if data is not None and not data.empty:
                # CSVに変換
                csv_buffer = io.StringIO()
                data.to_csv(csv_buffer, index=False, encoding='cp932', errors='replace')
                csv_data = csv_buffer.getvalue()
                
                self.logger.info(f"CSV生成完了: {sql_file}, {len(data)} 行")
                return csv_data
            else:
                self.logger.warning(f"データが空です: {sql_file}")
                return None
                
        except Exception as e:
            self.logger.error(f"CSV生成エラー: {sql_file}, {e}")
            return None
    
    def generate_csv_with_chunks(
        self,
        sql_file: str,
        chunk_size: int = 10000,
        conditions: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        大容量データをチャンク単位でCSV生成
        
        Args:
            sql_file: SQLファイル名
            chunk_size: チャンクサイズ
            conditions: 追加条件
            
        Returns:
            str: CSV文字列データ（失敗時はNone）
        """
        try:
            csv_buffer = io.StringIO()
            first_chunk = True
            total_rows = 0
            
            # チャンク単位でデータを処理
            for chunk in self._execute_sql_file_chunked(sql_file, chunk_size, conditions):
                if chunk is not None and not chunk.empty:
                    # 最初のチャンクの場合はヘッダーも含める
                    chunk.to_csv(
                        csv_buffer,
                        index=False,
                        header=first_chunk,
                        encoding='cp932',
                        errors='replace'
                    )
                    first_chunk = False
                    total_rows += len(chunk)
            
            if total_rows > 0:
                csv_data = csv_buffer.getvalue()
                self.logger.info(f"チャンク形式CSV生成完了: {sql_file}, {total_rows} 行")
                return csv_data
            else:
                self.logger.warning(f"データが空です: {sql_file}")
                return None
                
        except Exception as e:
            self.logger.error(f"チャンク形式CSV生成エラー: {sql_file}, {e}")
            return None
    
    def _execute_sql_file(
        self,
        sql_file: str,
        conditions: Optional[Dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """
        SQLファイルを実行してデータフレームを取得
        
        Args:
            sql_file: SQLファイル名
            conditions: 追加条件
            
        Returns:
            pd.DataFrame: 実行結果（失敗時はNone）
        """
        ssh_tunnel = None
        db_connection = None
        
        try:
            # SSH接続を確立
            ssh_config = {
                'host': self.config.ssh.host,
                'user': self.config.ssh.user,
                'ssh_key_path': self.config.ssh.ssh_key_path,
                'db_host': self.config.database.host,
                'db_port': self.config.database.port,
                'local_port': self.config.ssh.local_port
            }
            
            ssh_tunnel = SSHTunnel(ssh_config)
            if not ssh_tunnel.start():
                self.logger.error("SSH接続の確立に失敗")
                return None
            
            # データベース接続を確立
            db_config = {
                'host': self.config.database.host,
                'port': self.config.database.port,
                'user': self.config.database.user,
                'password': self.config.database.password,
                'database': self.config.database.database
            }
            
            db_connection = DatabaseConnection(db_config, ssh_tunnel.get_local_bind_port())
            conn = db_connection.create_connection()
            
            if not conn:
                self.logger.error("データベース接続の確立に失敗")
                return None
            
            # SQLクエリを読み込み（将来的にはGoogle Driveから）
            sql_query = self._load_sql_query(sql_file)
            if not sql_query:
                return None
            
            # 条件を追加（必要な場合）
            if conditions:
                sql_query = self._add_conditions_to_sql(sql_query, conditions)
            
            # SQLを実行
            data = pd.read_sql(sql_query, conn)
            
            self.logger.info(f"SQL実行完了: {sql_file}, {len(data)} 行取得")
            return data
            
        except Exception as e:
            self.logger.error(f"SQL実行エラー: {sql_file}, {e}")
            return None
            
        finally:
            # リソースをクリーンアップ
            if db_connection and hasattr(db_connection, '_current_connection'):
                try:
                    db_connection._current_connection.close()
                except:
                    pass
            
            if ssh_tunnel:
                ssh_tunnel.stop()
    
    def _execute_sql_file_chunked(
        self,
        sql_file: str,
        chunk_size: int,
        conditions: Optional[Dict[str, Any]] = None
    ):
        """
        SQLファイルをチャンク単位で実行
        
        Args:
            sql_file: SQLファイル名
            chunk_size: チャンクサイズ
            conditions: 追加条件
            
        Yields:
            pd.DataFrame: チャンクデータ
        """
        ssh_tunnel = None
        db_connection = None
        
        try:
            # 接続を確立（前回と同様）
            ssh_config = {
                'host': self.config.ssh.host,
                'user': self.config.ssh.user,
                'ssh_key_path': self.config.ssh.ssh_key_path,
                'db_host': self.config.database.host,
                'db_port': self.config.database.port,
                'local_port': self.config.ssh.local_port
            }
            
            ssh_tunnel = SSHTunnel(ssh_config)
            if not ssh_tunnel.start():
                return
            
            db_config = {
                'host': self.config.database.host,
                'port': self.config.database.port,
                'user': self.config.database.user,
                'password': self.config.database.password,
                'database': self.config.database.database
            }
            
            db_connection = DatabaseConnection(db_config, ssh_tunnel.get_local_bind_port())
            conn = db_connection.create_connection()
            
            if not conn:
                return
            
            # SQLクエリを読み込み
            sql_query = self._load_sql_query(sql_file)
            if not sql_query:
                return
            
            # 条件を追加
            if conditions:
                sql_query = self._add_conditions_to_sql(sql_query, conditions)
            
            # チャンク単位で実行
            for chunk in pd.read_sql(sql_query, conn, chunksize=chunk_size):
                yield chunk
                
        except Exception as e:
            self.logger.error(f"チャンク形式SQL実行エラー: {sql_file}, {e}")
            
        finally:
            # リソースをクリーンアップ
            if db_connection and hasattr(db_connection, '_current_connection'):
                try:
                    db_connection._current_connection.close()
                except:
                    pass
            
            if ssh_tunnel:
                ssh_tunnel.stop()
    
    def _load_sql_query(self, sql_file: str) -> Optional[str]:
        """
        SQLクエリファイルを読み込み
        
        Args:
            sql_file: SQLファイル名
            
        Returns:
            str: SQLクエリ（失敗時はNone）
        """
        try:
            # TODO: Google Driveから読み込み実装
            # 現在は一時的な実装
            from subcode_loader import load_sql_from_file
            
            sql_query = load_sql_from_file(
                self.config.google_api.drive_folder_id,
                sql_file,
                self.config.google_api.credentials_file
            )
            
            return sql_query
            
        except Exception as e:
            self.logger.error(f"SQLファイル読み込みエラー: {sql_file}, {e}")
            return None
    
    def _add_conditions_to_sql(self, sql_query: str, conditions: Dict[str, Any]) -> str:
        """
        SQLクエリに条件を追加
        
        Args:
            sql_query: 元のSQLクエリ
            conditions: 追加する条件
            
        Returns:
            str: 条件が追加されたSQLクエリ
        """
        try:
            # TODO: より柔軟な条件追加ロジックを実装
            # 現在は一時的な実装
            from subcode_loader import add_conditions_to_sql
            
            if 'period_condition' in conditions:
                sql_query = add_conditions_to_sql(sql_query, conditions['period_condition'])
            
            return sql_query
            
        except Exception as e:
            self.logger.error(f"SQL条件追加エラー: {e}")
            return sql_query