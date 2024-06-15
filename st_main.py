import threading
import time
import traceback
from config_loader import load_config
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from subcode_loader import load_sql_from_file
from my_logging import setup_department_logger

# ロガーの設定
logger = setup_department_logger('main')

# グローバル変数
global_tunnel = None
global_conn = None
last_activity_time = time.time()
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# 接続を管理するバックグラウンドスレッド
def manage_connections():
    global global_tunnel, global_conn, last_activity_time

    while True:
        current_time = time.time()
        if current_time - last_activity_time > 1800:  # 30分
            if global_conn:
                global_conn.close()
                global_conn = None
                logger.info("データベース接続を閉じました。")
            if global_tunnel:
                global_tunnel.stop()
                global_tunnel = None
                logger.info("SSHトンネルを閉じました。")
        time.sleep(60)

# データベース接続を取得する関数
def get_connection():
    global global_tunnel, global_conn, last_activity_time
    last_activity_time = time.time()

    if global_conn is not None and global_tunnel is not None:
        return global_conn

    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    ssh_config['db_host'] = db_config['host']
    ssh_config['db_port'] = db_config['port']
    ssh_config['local_port'] = local_port

    # SSHトンネルを確立
    global_tunnel = create_ssh_tunnel(ssh_config)
    if global_tunnel:
        # データベースに接続
        global_conn = create_database_connection(db_config, global_tunnel.local_bind_port)
        if global_conn:
            logger.info("データベース接続完了")
        else:
            raise Exception("データベース接続に失敗しました。")
    else:
        raise Exception("SSHトンネルの開設に失敗しました。")
    
    return global_conn

# 背景で接続を管理するスレッドを開始
connection_thread = threading.Thread(target=manage_connections, daemon=True)
connection_thread.start()

# SQLファイルを読み込み、条件を追加して準備する関数
def load_and_prepare_sql(sql_file_name, input_fields, input_fields_types):
    try:
        # SQLファイルの内容を読み込む
        sql_query = load_sql_from_file(sql_file_name, additional_config['google_folder_id'], additional_config['json_keyfile_path'])
        if sql_query:
            return sql_query
        else:
            error_message = f"SQLファイル {sql_file_name} の読み込みに失敗しました。"
            logger.error(error_message)
            return None
    except Exception as e:
        error_message = f"SQLファイルの読み込み中にエラーが発生しました: {e}\n{traceback.format_exc()}"
        logger.error(error_message)
        return None