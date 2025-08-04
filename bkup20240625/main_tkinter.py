import logging
import configparser
from config_loader import load_config
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from subcode_loader import load_sql_from_file, csvfile_export_with_timestamp, copy_to_clipboard, add_conditions_to_sql
import traceback

# ロガーの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # ログレベルをDEBUGに設定

# コンソールハンドラの作成
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# ファイルハンドラの作成
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.DEBUG)

# フォーマッタの作成
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# ハンドラをロガーに追加
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def execute_sql_file(sql_file_name, input_values, input_fields_types, mode, include_header, root):
    config_file = 'config.ini'
    
    try:
        # 設定の読み込み
        logger.info("設定の読み込みを開始")
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        logger.info("設定の読み込み完了")

        # SSHトンネル設定
        ssh_config['db_host'] = db_config['host']
        ssh_config['db_port'] = db_config['port']
        ssh_config['local_port'] = local_port
        logger.info("SSHトンネル設定完了")

        # SSHトンネルを確立
        logger.info("SSHトンネルの確立を開始")
        tunnel = create_ssh_tunnel(ssh_config)
        logger.info("SSHトンネルの確立完了")

        if tunnel:
            try:
                # データベースに接続
                logger.info("データベース接続を開始")
                conn = create_database_connection(db_config, tunnel.local_bind_port)
                logger.info("データベース接続完了")

                if conn:
                    try:
                        # SQLファイルの内容を読み込む
                        logger.info(f"SQLファイル {sql_file_name} の読み込みを開始")
                        logger.info(f"GoogleフォルダID: {additional_config['google_folder_id']}")
                        logger.info(f"JSONキーのパス: {additional_config['json_keyfile_path']}")
                        sql_query = load_sql_from_file(sql_file_name, additional_config['google_folder_id'], additional_config['json_keyfile_path'])
                        logger.info(f"SQLファイル {sql_file_name} の読み込み完了")
                        
                        if sql_query:
                            # 入力データに基づいてSQL文に条件を追加
                            logger.debug(f"SQLファイルの内容: {sql_query}")
                            logger.info("SQL文への条件追加を開始")
                            sql_query_with_conditions = add_conditions_to_sql(sql_query, input_values, input_fields_types, None, skip_deletion_exclusion=True)
                            logger.info("SQL文への条件追加完了")
                            logger.debug(f"条件付きSQLクエリ: {sql_query_with_conditions}")

                            if mode == 'download':
                                logger.info("CSVファイルの保存を開始")
                                # CSVに保存
                                csvfile_export_with_timestamp(conn, sql_query_with_conditions, sql_file_name, additional_config['json_keyfile_path'], include_header, additional_config['chunk_size'], additional_config['batch_size'], additional_config['delay'], additional_config['max_workers'])
                                logger.info("CSVファイルの保存完了")
                            elif mode == 'copy':
                                logger.info("クリップボードへのコピーを開始")
                                # クリップボードにコピー
                                copy_to_clipboard(conn, sql_query_with_conditions, include_header)
                                logger.info("クリップボードへのコピー完了")
                            logger.info("execute_sql_file 関数の終了")
                            return True, None
                        else:
                            error_message = f"SQLファイル {sql_file_name} の読み込みに失敗しました。"
                            logger.error(error_message)
                            logger.info("execute_sql_file 関数の終了")
                            return False, error_message
                    except Exception as e:
                        error_message = f"SQLファイルの実行中にエラーが発生しました: {e}\n{traceback.format_exc()}"
                        logger.error(error_message)
                        return False, error_message
                    finally:
                        if conn:
                            conn.close()
                            logger.info("データベース接続を閉じました。")
                else:
                    error_message = "データベース接続に失敗しました。"
                    logger.error(error_message)
                    logger.info("execute_sql_file 関数の終了")
                    return False, error_message
            finally:
                tunnel.stop()
                logger.info("SSHトンネルを閉じました。")
        else:
            error_message = "SSHトンネルの開設に失敗しました。"
            logger.error(error_message)
            logger.info("execute_sql_file 関数の終了")
            return False, error_message
    except Exception as e:
        error_message = f"設定の読み込みまたはSSHトンネルの確立中にエラーが発生しました: {e}\n{traceback.format_exc()}"
        logger.error(error_message)
        return False, error_message

