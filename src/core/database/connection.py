"""
データベース接続管理

MySQL接続とコネクションプールの管理を提供
旧構造との互換性も提供
"""
import mysql.connector
import traceback
import time
from typing import Optional, Dict, Any
from mysql.connector.pooling import MySQLConnectionPool
from src.core.logging.logger import get_logger


class DatabaseConnection:
    """データベース接続管理クラス"""
    
    def __init__(self, db_config: Dict[str, Any], local_bind_port: int = 3306):
        """
        データベース接続管理を初期化
        
        Args:
            db_config: データベース設定辞書
            local_bind_port: ローカルバインドポート
        """
        self.db_config = db_config
        self.local_bind_port = local_bind_port
        self.logger = get_logger(__name__)
        self._connection_pool: Optional[MySQLConnectionPool] = None
        
    def create_connection(self, max_retries: int = 3) -> Optional[mysql.connector.MySQLConnection]:
        """
        データベース接続を作成
        
        Args:
            max_retries: 最大再試行回数
            
        Returns:
            MySQLConnection: データベース接続オブジェクト（失敗時はNone）
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = mysql.connector.connect(
                    host='127.0.0.1',
                    port=self.local_bind_port,
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database'],
                    auth_plugin='mysql_native_password',
                    connection_timeout=300,  # 接続タイムアウト
                    autocommit=True,
                    use_pure=True
                )
                
                self.logger.info("データベースに接続しました。接続状態を確認します。")
                conn.ping(reconnect=True)
                self.logger.info("接続は有効です。")
                
                return conn
                
            except mysql.connector.Error as err:
                retry_count += 1
                self.logger.warning(f"データベース接続エラー (試行 {retry_count}/{max_retries}): {err}")
                if retry_count >= max_retries:
                    self.logger.error("最大試行回数に達しました。接続を確立できません。")
                    return None
                self.logger.info("10秒後に再試行します...")
                time.sleep(10)
                
            except Exception as e:
                self.logger.error(f"予期しないエラー: {e}")
                self.logger.error(traceback.format_exc())
                return None
        
        return None
    
    def create_connection_pool(self, pool_name: str = "mysql_pool", pool_size: int = 5) -> Optional[MySQLConnectionPool]:
        """
        コネクションプールを作成
        
        Args:
            pool_name: プール名
            pool_size: プールサイズ
            
        Returns:
            MySQLConnectionPool: コネクションプール（失敗時はNone）
        """
        try:
            self._connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                host='127.0.0.1',
                port=self.local_bind_port,
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                auth_plugin='mysql_native_password',
                connect_timeout=100,
                use_pure=True
            )
            
            self.logger.info(f"コネクションプールを作成しました: {pool_name} (size: {pool_size})")
            return self._connection_pool
            
        except mysql.connector.Error as err:
            self.logger.error(f"コネクションプール作成エラー: {err}")
            return None
    
    def get_connection_from_pool(self) -> Optional[mysql.connector.MySQLConnection]:
        """
        プールから接続を取得
        
        Returns:
            MySQLConnection: データベース接続オブジェクト（失敗時はNone）
        """
        if not self._connection_pool:
            self.logger.error("コネクションプールが初期化されていません")
            return None
            
        try:
            conn = self._connection_pool.get_connection()
            self.logger.debug("プールから接続を取得しました")
            return conn
            
        except mysql.connector.Error as err:
            self.logger.error(f"プールからの接続取得エラー: {err}")
            return None
    
    def close_connection(self, conn: mysql.connector.MySQLConnection) -> None:
        """
        データベース接続を閉じる
        
        Args:
            conn: 閉じる接続オブジェクト
        """
        if conn and conn.is_connected():
            try:
                conn.close()
                self.logger.debug("データベース接続を閉じました")
            except Exception as e:
                self.logger.error(f"接続クローズエラー: {e}")


# 設定管理連携
def create_database_connection_from_config(config_file: str = "config/settings.ini") -> Optional[mysql.connector.MySQLConnection]:
    """
    設定ファイルからデータベース接続を作成
    新構造の設定管理を使用
    
    Args:
        config_file: 設定ファイルのパス
        
    Returns:
        MySQLConnection: データベース接続オブジェクト（失敗時はNone）
    """
    try:
        from ..config.settings import AppConfig
        app_config = AppConfig.from_config_file(config_file)
        
        db_config = {
            'user': app_config.database.user,
            'password': app_config.database.password,
            'database': app_config.database.database,
            'host': app_config.database.host,
            'port': app_config.database.port
        }
        
        db_conn = DatabaseConnection(db_config, app_config.ssh.local_port)
        return db_conn.create_connection()
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"設定ファイルからのDB接続作成エラー: {e}")
        return None


# 後方互換性のための関数
def create_database_connection(db_config: Dict[str, Any], local_bind_port: int) -> Optional[mysql.connector.MySQLConnection]:
    """
    旧式のデータベース接続関数（後方互換性のため）
    
    Args:
        db_config: データベース設定辞書
        local_bind_port: ローカルバインドポート
        
    Returns:
        MySQLConnection: データベース接続オブジェクト（失敗時はNone）
    """
    db_conn = DatabaseConnection(db_config, local_bind_port)
    return db_conn.create_connection()