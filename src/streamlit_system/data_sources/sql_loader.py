"""
SQL ローダー機能

SQLファイルの管理と実行を提供
"""
import sys
import os
import pandas as pd
from typing import List, Optional, Dict, Any

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.config.settings import AppConfig
from src.core.database.connection import DatabaseConnection
from src.core.database.ssh_tunnel import SSHTunnel
from src.core.logging.logger import get_logger


class SQLLoader:
    """SQL ローダー管理クラス"""
    
    def __init__(self, config: Optional[AppConfig] = None):
        """
        SQL ローダーを初期化
        
        Args:
            config: アプリケーション設定
        """
        # 設定未指定時はデフォルトの設定ファイルから読み込み
        if config is None:
            try:
                self.config = AppConfig.from_config_file('config/settings.ini')
            except Exception:
                # 最低限のフォールバック（環境依存部は実行時に例外化する可能性あり）
                self.config = AppConfig.from_env()
        else:
            self.config = config
        self.logger = get_logger(__name__)
        self._cached_sql_files = None
    
    def get_sql_file_list(self, force_refresh: bool = False) -> List[str]:
        """
        利用可能なSQLファイルリストを取得
        
        Args:
            force_refresh: 強制的にキャッシュを更新するか
            
        Returns:
            List[str]: SQLファイル名のリスト
        """
        try:
            if self._cached_sql_files is None or force_refresh:
                self._cached_sql_files = self._load_sql_file_list()
            
            return self._cached_sql_files or []
            
        except Exception as e:
            self.logger.error(f"SQLファイルリスト取得エラー: {e}")
            return []
    
    def execute_sql_file(
        self,
        sql_file: str,
        conditions: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        SQLファイルを実行してデータを取得
        
        Args:
            sql_file: SQLファイル名
            conditions: 追加条件
            limit: 取得行数制限
            
        Returns:
            pd.DataFrame: 実行結果（失敗時はNone）
        """
        ssh_tunnel = None
        db_connection = None
        
        try:
            self.logger.info(f"SQL実行開始: {sql_file}")
            
            # 接続を確立
            ssh_tunnel, db_connection, conn = self._establish_connections()
            if not conn:
                return None
            
            # SQLクエリを読み込み
            sql_query = self._load_sql_query(sql_file)
            if not sql_query:
                return None
            
            # 条件を追加
            if conditions:
                sql_query = self._add_conditions_to_sql(sql_query, conditions)
            
            # LIMIT / OFFSET を追加
            if limit and limit > 0:
                if offset and offset > 0:
                    sql_query = f"{sql_query} LIMIT {limit} OFFSET {offset}"
                else:
                    sql_query = f"{sql_query} LIMIT {limit}"
            
            # SQLを実行
            data = pd.read_sql(sql_query, conn)
            
            self.logger.info(f"SQL実行完了: {sql_file}, {len(data)} 行取得")
            return data
            
        except Exception as e:
            self.logger.error(f"SQL実行エラー: {sql_file}, {e}")
            return None
            
        finally:
            self._cleanup_connections(db_connection, ssh_tunnel)

    def execute_sql_file_count(
        self,
        sql_file: str,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """
        対象SQL（条件適用後）の総件数を取得
        
        SELECT COUNT(*) FROM (<base_sql_with_conditions>) AS t 形式で集計
        """
        ssh_tunnel = None
        db_connection = None
        try:
            self.logger.info(f"件数取得開始: {sql_file}")
            ssh_tunnel, db_connection, conn = self._establish_connections()
            if not conn:
                return None
            base_sql = self._load_sql_query(sql_file)
            if not base_sql:
                return None
            if conditions:
                base_sql = self._add_conditions_to_sql(base_sql, conditions)
            count_sql = f"SELECT COUNT(*) AS cnt FROM ({base_sql}) AS t"
            df = pd.read_sql(count_sql, conn)
            return int(df.iloc[0]['cnt']) if not df.empty else 0
        except Exception as e:
            self.logger.error(f"件数取得エラー: {e}")
            return None
        finally:
            self._cleanup_connections(db_connection, ssh_tunnel)
    
    def execute_sql_query(
        self,
        sql_query: str,
        limit: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        SQLクエリを直接実行
        
        Args:
            sql_query: SQLクエリ文字列
            limit: 取得行数制限
            
        Returns:
            pd.DataFrame: 実行結果（失敗時はNone）
        """
        ssh_tunnel = None
        db_connection = None
        
        try:
            self.logger.info("SQLクエリ実行開始")
            
            # 接続を確立
            ssh_tunnel, db_connection, conn = self._establish_connections()
            if not conn:
                return None
            
            # 行数制限を追加
            if limit and limit > 0:
                sql_query = f"{sql_query} LIMIT {limit}"
            
            # SQLを実行
            data = pd.read_sql(sql_query, conn)
            
            self.logger.info(f"SQLクエリ実行完了: {len(data)} 行取得")
            return data
            
        except Exception as e:
            self.logger.error(f"SQLクエリ実行エラー: {e}")
            return None
            
        finally:
            self._cleanup_connections(db_connection, ssh_tunnel)
    
    def get_table_schema(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        テーブルスキーマ情報を取得
        
        Args:
            table_name: テーブル名
            
        Returns:
            pd.DataFrame: スキーマ情報（失敗時はNone）
        """
        schema_query = f"""
        SELECT 
            COLUMN_NAME as 'カラム名',
            DATA_TYPE as 'データ型',
            IS_NULLABLE as 'NULL許可',
            COLUMN_DEFAULT as 'デフォルト値',
            COLUMN_COMMENT as 'コメント'
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{self.config.database.database}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        return self.execute_sql_query(schema_query)
    
    def get_table_list(self) -> Optional[pd.DataFrame]:
        """
        データベース内のテーブル一覧を取得
        
        Returns:
            pd.DataFrame: テーブル一覧（失敗時はNone）
        """
        table_query = f"""
        SELECT 
            TABLE_NAME as 'テーブル名',
            TABLE_COMMENT as 'コメント',
            TABLE_ROWS as '推定行数'
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{self.config.database.database}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        return self.execute_sql_query(table_query)
    
    def _load_sql_file_list(self) -> List[str]:
        """
        Googleスプレッドシートから利用可能なSQLファイルリストを取得
        
        Returns:
            List[str]: SQLファイル名のリスト
        """
        try:
            # TODO: Google Sheets APIから取得実装
            # 現在は一時的な実装
            from subcode_loader import load_sql_file_list_from_spreadsheet
            
            sql_files = load_sql_file_list_from_spreadsheet(
                self.config.google_api.spreadsheet_id,
                self.config.google_api.eachdata_sheet,
                self.config.google_api.credentials_file,
                "個別リスト"
            )
            
            # SQLファイル名のみを抽出
            sql_file_names = [item[0] for item in sql_files if len(item) > 0]
            
            self.logger.info(f"SQLファイルリスト取得完了: {len(sql_file_names)} 件")
            return sql_file_names
            
        except Exception as e:
            self.logger.error(f"SQLファイルリスト取得エラー: {e}")
            return []
    
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
            
            self.logger.debug(f"SQLファイル読み込み完了: {sql_file}")
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
            from subcode_loader import add_conditions_to_sql, set_period_condition
            
            modified_query = sql_query
            
            if 'period_condition' in conditions:
                modified_query = add_conditions_to_sql(modified_query, conditions['period_condition'])
            
            if 'period_criteria' in conditions:
                modified_query = set_period_condition(modified_query, conditions['period_criteria'])
            
            return modified_query
            
        except Exception as e:
            self.logger.error(f"SQL条件追加エラー: {e}")
            return sql_query
    
    def _establish_connections(self):
        """
        SSH接続とデータベース接続を確立
        
        Returns:
            tuple: (ssh_tunnel, db_connection, mysql_connection)
        """
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
                return None, None, None
            
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
                ssh_tunnel.stop()
                return None, None, None
            
            return ssh_tunnel, db_connection, conn
            
        except Exception as e:
            self.logger.error(f"接続確立エラー: {e}")
            return None, None, None
    
    def _cleanup_connections(self, db_connection, ssh_tunnel):
        """
        接続をクリーンアップ
        
        Args:
            db_connection: データベース接続オブジェクト
            ssh_tunnel: SSHトンネルオブジェクト
        """
        try:
            if db_connection and hasattr(db_connection, '_current_connection'):
                try:
                    db_connection._current_connection.close()
                except:
                    pass
            
            if ssh_tunnel:
                ssh_tunnel.stop()
                
        except Exception as e:
            self.logger.error(f"接続クリーンアップエラー: {e}")