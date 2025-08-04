import argparse
import main
import traceback

if __name__ == '__main__':
    # コマンドライン引数のパーサーを作成
    parser = argparse.ArgumentParser(description='SQLファイルを実行するためのスクリプト')
    parser.add_argument('sql_file', type=str, help='実行対象のSQLファイル名')
    args = parser.parse_args()

    # 指定されたSQLファイル名
    target_sql_file = args.sql_file

    # main.pyから必要な設定を取得
    spreadsheet_id = main.spreadsheet_id
    sheet_name = main.sheet_name
    json_keyfile_path = main.json_keyfile_path

    # スプレッドシートからSQLファイルのリストを読み込む
    sql_and_csv_files = main.load_sql_file_list_from_spreadsheet(spreadsheet_id, sheet_name, json_keyfile_path)

    # 指定されたSQLファイルを検索
    target_file_info = None
    for file_info in sql_and_csv_files:
        if file_info[0] == target_sql_file:
            target_file_info = file_info
            break

    if target_file_info:
        # SSHトンネルとデータベース接続の設定を取得
        ssh_config, db_config, local_port = main.load_config(main.config_file)
        ssh_config['db_host'] = db_config['host']
        ssh_config['db_port'] = db_config['port']
        ssh_config['local_port'] = local_port

        # SSHトンネルを作成
        tunnel = main.create_ssh_tunnel(ssh_config)

        if tunnel:
            try:
                # データベースに接続
                conn = main.create_database_connection(db_config, tunnel.local_bind_port)
                if conn:
                    # 指定されたSQLファイルを実行
                    result = main.execute_sql_file(conn, target_file_info)
                    print(f"\n処理結果: {result}")
                else:
                    main.LOGGER.error("データベース接続に失敗しました。")
            except Exception as e:
                main.LOGGER.error(f"処理中にエラーが発生しました: {traceback.format_exc()}")
                main.send_slack_error_message(str(e), config=main.config)
            finally:
                if conn:
                    conn.close()
                    main.LOGGER.info("データベース接続を閉じました。")
                tunnel.stop()
                main.LOGGER.info("SSHトンネルを閉じました。")
        else:
            main.LOGGER.error("SSHトンネルの開設に失敗しました。")
    else:
        main.LOGGER.error(f"指定されたSQLファイル '{target_sql_file}' が見つかりませんでした。")