import mysql.connector
import traceback
import time
from .my_logging import setup_department_logger

LOGGER = setup_department_logger('main')

def create_database_connection(db_config, local_bind_port):
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conn = mysql.connector.connect(
                host='127.0.0.1',
                port=local_bind_port,
                user=db_config['user'],
                passwd=db_config['password'],
                database=db_config['database'],
                auth_plugin='mysql_native_password',
                connect_timeout=300,  # 接続タイムアウトを5分に延長
                read_timeout=1800,    # 読み取りタイムアウトを30分に設定
                write_timeout=1800,   # 書き込みタイムアウトを30分に設定
                autocommit=True,      # 自動コミットを有効化
                use_pure=True
            )
            
            LOGGER.info("データベースに接続しました。接続状態を確認します。")
            conn.ping(reconnect=True)
            LOGGER.info("接続は有効です。")
            
            return conn
        
        except mysql.connector.Error as err:
            retry_count += 1
            LOGGER.warning(f"データベース接続エラー (試行 {retry_count}/{max_retries}): {err}")
            if retry_count >= max_retries:
                LOGGER.error("最大試行回数に達しました。接続を確立できません。")
                return None
            LOGGER.info("10秒後に再試行します...")
            time.sleep(10)  # 固定の10秒間隔
        
        except Exception as e:
            LOGGER.error(f"予期しないエラー: {e}")
            LOGGER.error(traceback.format_exc())
            return None

    return None  # max_retriesを超えた場合