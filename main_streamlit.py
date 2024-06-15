import logging
import configparser
from config_loader import load_config
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from subcode_loader import load_sql_from_file, csvfile_export_with_streamlit, copy_to_clipboard_streamlit, add_conditions_to_sql
import traceback
import threading
import time
from my_logging import setup_department_logger  # 追加

# ロガーの設定
logger = setup_department_logger('main')

# グローバル変数
global_tunnel = None
global_conn = None
last_activity_time = time.time()
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

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

def get_connection():
    global global_tunnel, global_conn, last_activity_time
    last_activity_time = time.time()

    if global_conn is None or global_tunnel is None:
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

def execute_sql_file(sql_file_name, input_fields, input_fields_types, action, include_header):
    try:
        conn = get_connection()
        
        if conn:
            try:
                # SQLファイルの内容を読み込む
                sql_query = load_sql_from_file(sql_file_name, additional_config['google_folder_id'], additional_config['json_keyfile_path'])
                if sql_query:
                    # 入力データに基づいてSQL文に条件を追加
                    sql_query_with_conditions = add_conditions_to_sql(sql_query, input_fields, input_fields_types, None, skip_deletion_exclusion=True)
                    csv_data = None
                    if action == 'download':
                        # CSVに保存
                        csv_data = csvfile_export_with_streamlit(conn, sql_query_with_conditions, sql_file_name, include_header, additional_config['chunk_size'], additional_config['batch_size'], additional_config['delay'], additional_config['max_workers'])
                    elif action == 'copy':
                        # クリップボードにコピー
                        copy_to_clipboard_streamlit(conn, sql_query_with_conditions, include_header)
                    
                    return True, csv_data, None
                else:
                    error_message = f"SQLファイル {sql_file_name} の読み込みに失敗しました。"
                    logger.error(error_message)
                    return False, None, error_message
            except Exception as e:
                error_message = f"SQLファイルの実行中にエラーが発生しました: {e}\n{traceback.format_exc()}"
                logger.error(error_message)
                return False, None, error_message
        else:
            error_message = "データベース接続に失敗しました。"
            logger.error(error_message)
            return False, None, error_message
    except Exception as e:
        error_message = f"設定の読み込みまたはSSHトンネルの確立中にエラーが発生しました: {e}\n{traceback.format_exc()}"
        logger.error(error_message)
        return False, None, error_message
