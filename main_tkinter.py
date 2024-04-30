import logging
import configparser
from config_loader import load_config
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from subcode_loader import load_sql_from_file, csvfile_export_with_timestamp, copy_to_clipboard, add_conditions_to_sql
import tkinter as tk

logging.basicConfig(level=logging.INFO)

def execute_sql_file(sql_file_name, input_values, input_fields_types, mode, include_header, root):
    # 設定ファイルの読み込み
    config_file = 'config.ini'
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    # 設定の読み込み
    ssh_config, db_config, local_port = load_config(config_file)
    google_folder_id = config['GoogleDrive']['google_folder_id']
    json_keyfile_path = config['Credentials']['json_keyfile_path']

    # SSHトンネル設定
    ssh_config['db_host'] = db_config['host']
    ssh_config['db_port'] = db_config['port']
    ssh_config['local_port'] = local_port

    # SSHトンネルを確立
    tunnel = create_ssh_tunnel(ssh_config)

    if tunnel:
        try:
            # データベースに接続
            conn = create_database_connection(db_config, tunnel.local_bind_port)

            if conn:
                try:
                    # SQLファイルの内容を読み込む
                    sql_query = load_sql_from_file(sql_file_name, google_folder_id, json_keyfile_path)
                    if sql_query:
                        # 入力データに基づいてSQL文に条件を追加
                        sql_query_with_conditions = add_conditions_to_sql(sql_query, input_values, input_fields_types, None, skip_deletion_exclusion=True)

                        if mode == 'download':
                            # CSVに保存
                            csvfile_export_with_timestamp(conn, sql_query_with_conditions, sql_file_name, include_header)
                        elif mode == 'copy':
                            # クリップボードにコピー
                            copy_to_clipboard(conn, sql_query_with_conditions, include_header)
                        return True
                    else:
                        print(f"SQLファイル {sql_file_name} の読み込みに失敗しました。")
                        return False
                finally:
                    if conn:
                        conn.close()
                        print("データベース接続を閉じました。")
            else:
                print("データベース接続に失敗しました。")
                return False
        finally:
            tunnel.stop()
            print("SSHトンネルを閉じました。")
    else:
        print("SSHトンネルの開設に失敗しました。")
        return False