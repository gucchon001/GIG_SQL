import configparser
import os
import traceback
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from config_loader import load_config
from subcode_loader import load_sql_file_list_from_spreadsheet, load_sql_from_file, csvfile_export, add_conditions_to_sql, set_period_condition, export_to_spreadsheet, setup_test_environment
from my_logging import setup_department_logger
import slack_notify

base_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
config_file = os.path.join(base_dir, 'config.ini')
spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
sheet_name = config['Spreadsheet']['main_sheet']
json_keyfile_path = config['Credentials']['json_keyfile_path']
csv_base_path = config['Paths']['csv_base_path']
google_folder_id = config['GoogleDrive']['google_folder_id']

LOGGER = setup_department_logger('main')

sql_and_csv_files = load_sql_file_list_from_spreadsheet(spreadsheet_id, sheet_name, json_keyfile_path)

ssh_config, db_config, local_port = load_config(config_file)

ssh_config['db_host'] = db_config['host']
ssh_config['db_port'] = db_config['port']
ssh_config['local_port'] = local_port

tunnel = create_ssh_tunnel(ssh_config)

results = []

if tunnel:
    try:
        conn = create_database_connection(db_config, tunnel.local_bind_port)
        if conn:
            for file_info in sql_and_csv_files:
                try:
                    sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution, category = file_info
                except ValueError as e:
                    LOGGER.error(f"file_infoのアンパック中にエラーが発生しました: {file_info}")
                    results.append(f"{file_info}: 失敗 (file_infoのアンパック中にエラー)")
                    continue

                save_path_id, csv_file_name = setup_test_environment(test_execution, output_to_spreadsheet, save_path_id, csv_file_name, spreadsheet_id, json_keyfile_path)

                sql_query = load_sql_from_file(sql_file_name, google_folder_id, json_keyfile_path)
                if sql_query:
                    try:
                        input_values, input_fields_types = {}, {}
                        sql_query_with_period_condition = set_period_condition(period_condition, period_criteria, sql_query, category)
                        if category != 'マスタ':
                            sql_query_with_conditions = add_conditions_to_sql(sql_query_with_period_condition, input_values, input_fields_types, deletion_exclusion)
                        else:
                            sql_query_with_conditions = sql_query_with_period_condition

                        if output_to_spreadsheet == 'CSV':
                            if save_path_id and save_path_id.strip():
                                csv_file_path = os.path.join(save_path_id, csv_file_name)
                            else:
                                csv_file_path = os.path.join(csv_base_path, csv_file_name)
                            try:
                                csvfile_export(conn, sql_query_with_conditions, csv_file_path)
                            except Exception as e:
                                LOGGER.error(f"CSVファイルのエクスポート中にエラーが発生しました: {e}")
                                result = f"{sql_file_name}: 失敗 (CSVファイルのエクスポート中にエラー)"
                            else:
                                result = f"{sql_file_name}: 成功 (保存先: {csv_file_path})"
                        elif output_to_spreadsheet == 'スプシ':
                            print(f"Exporting to spreadsheet: save_path_id={save_path_id}, csv_file_name={csv_file_name}")
                            try:
                                export_to_spreadsheet(conn, sql_query_with_conditions, save_path_id, csv_file_name, json_keyfile_path, paste_format, sheet_name)
                            except Exception as e:
                                LOGGER.error(f"スプレッドシートへのエクスポート中にエラーが発生しました: {e}")
                                result = f"{sql_file_name}: 失敗 (スプレッドシートへのエクスポート中にエラー)"
                            else:
                                result = f"{sql_file_name}: 成功 (保存先: {save_path_id}, シート名: {csv_file_name})"
                        else:
                            LOGGER.error(f"無効な出力先が指定されました: {output_to_spreadsheet}")
                            result = f"{sql_file_name}: 失敗 (無効な出力先: {output_to_spreadsheet})"

                    except Exception as e:
                        LOGGER.error(f"SQLクエリの処理中にエラーが発生しました: {e}")
                        result = f"{sql_file_name}: 失敗 (SQLクエリの処理中にエラー)"
                    else:
                        results.append(result)
                else:
                    LOGGER.error(f"{sql_file_name} の読み込みに失敗しました。")
                    result = f"{sql_file_name}: 失敗 (SQLファイルの読み込みに失敗)"
                    results.append(result)

        else:
            LOGGER.error("データベース接続に失敗しました。")
    except Exception as e:
        LOGGER.error(f"処理中にエラーが発生しました: {traceback.format_exc()}")
        slack_notify.send_slack_error_message(e, config=config)
    finally:
        if conn:
            conn.close()
            LOGGER.info("データベース接続を閉じました。")
        tunnel.stop()
        LOGGER.info("SSHトンネルを閉じました。")
else:
    LOGGER.error("SSHトンネルの開設に失敗しました。")

print("\n処理結果一覧:")
for result in results:
    print(result)

# 個別実行
def execute_sql_file(conn, file_info):
    sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution, category = file_info

    save_path_id, csv_file_name = setup_test_environment(test_execution, output_to_spreadsheet, save_path_id, csv_file_name, spreadsheet_id, json_keyfile_path)

    sql_query = load_sql_from_file(sql_file_name, google_folder_id, json_keyfile_path)
    if sql_query:
        try:
            input_values, input_fields_types = {}, {}
            sql_query_with_period_condition = set_period_condition(period_condition, period_criteria, sql_query, category)
            if category != 'マスタ':
                sql_query_with_conditions = add_conditions_to_sql(sql_query_with_period_condition, input_values, input_fields_types, deletion_exclusion)
            else:
                sql_query_with_conditions = sql_query_with_period_condition

            if output_to_spreadsheet == 'CSV':
                if save_path_id and save_path_id.strip():
                    csv_file_path = os.path.join(save_path_id, csv_file_name)
                else:
                    csv_file_path = os.path.join(csv_base_path, csv_file_name)
                try:
                    csvfile_export(conn, sql_query_with_conditions, csv_file_path)
                except Exception as e:
                    LOGGER.error(f"CSVファイルのエクスポート中にエラーが発生しました: {e}")
                    return f"{sql_file_name}: 失敗 (CSVファイルのエクスポート中にエラー)"
                else:
                    return f"{sql_file_name}: 成功 (保存先: {csv_file_path})"
            elif output_to_spreadsheet == 'スプシ':
                print(f"Exporting to spreadsheet: save_path_id={save_path_id}, csv_file_name={csv_file_name}")
                try:
                    export_to_spreadsheet(conn, sql_query_with_conditions, save_path_id, csv_file_name, json_keyfile_path, paste_format, sheet_name)
                except Exception as e:
                    LOGGER.error(f"スプレッドシートへのエクスポート中にエラーが発生しました: {e}")
                    return f"{sql_file_name}: 失敗 (スプレッドシートへのエクスポート中にエラー)"
                else:
                    return f"{sql_file_name}: 成功 (保存先: {save_path_id}, シート名: {csv_file_name})"
            else:
                LOGGER.error(f"無効な出力先が指定されました: {output_to_spreadsheet}")
                return f"{sql_file_name}: 失敗 (無効な出力先: {output_to_spreadsheet})"

        except Exception as e:
            LOGGER.error(f"SQLクエリの処理中にエラーが発生しました: {e}")
            return f"{sql_file_name}: 失敗 (SQLクエリの処理中にエラー)"
    else:
        LOGGER.error(f"{sql_file_name} の読み込みに失敗しました。")
        return f"{sql_file_name}: 失敗 (SQLファイルの読み込みに失敗)"