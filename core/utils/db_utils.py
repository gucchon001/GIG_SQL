import pandas as pd
from ..config.ssh_connection import create_ssh_tunnel
try:
    # 新構造のデータベース接続を優先使用
    from src.core.database.connection import create_database_connection
except ImportError:
    # フォールバック：旧構造
    from ..config.database_connection import create_database_connection
from ..config.my_logging import setup_department_logger
from ..data.subcode_loader import load_sql_from_file
from ..config.config_loader import load_config

# ロガーの設定
logger = setup_department_logger('db_utils')

# データベース接続を取得する関数
def get_connection(config_file):
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    ssh_config['db_host'] = db_config['host']
    ssh_config['db_port'] = db_config['port']
    ssh_config['local_port'] = local_port

    # SSHトンネルを確立
    tunnel = create_ssh_tunnel(ssh_config)
    if tunnel:
        # データベースに接続
        conn = create_database_connection(db_config, tunnel.local_bind_port)
        if conn:
            logger.info("データベース接続完了")
            return conn
        else:
            logger.error("データベース接続に失敗しました。")
            return None
    else:
        logger.error("SSHトンネルの開設に失敗しました。")
        return None

# SQL文を実行してデータを取得する関数
def execute_sql_query(sql_file_name, config_file):
    conn = get_connection(config_file)
    if conn:
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        try:
            sql_query = load_sql_from_file(sql_file_name, additional_config['google_folder_id'], additional_config['json_keyfile_path'])
            if sql_query is None:
                raise FileNotFoundError(f"SQLファイルが見つかりません: {sql_file_name}")
            
            df = pd.read_sql(sql_query, conn)
            return df
        except Exception as e:
            logger.error(f"SQL実行中にエラーが発生しました: {e}")
            return None
        finally:
            conn.close()
    else:
        logger.error("データベース接続が確立されていません。")
        return None