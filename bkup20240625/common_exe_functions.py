import traceback
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from config_loader import load_config
from subcode_loader import load_sql_file_list_from_spreadsheet, load_sql_from_file, csvfile_export, add_conditions_to_sql, set_period_condition, export_to_spreadsheet, setup_test_environment
from my_logging import setup_department_logger
import slack_notify
import os

LOGGER = setup_department_logger('main')

def process_sql_and_csv_files(sql_and_csv_files, conn, config):
    results = []
    for file_info in sql_and_csv_files:
        try:
            sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution, category, main_table_name, csv_file_name_column, sheet_name = file_info
        except ValueError as e:
            LOGGER.error(f"file_infoのアンパック中にエラーが発生しました: {file_info}")
            results.append(f"★失敗★　{file_info}: file_infoのアンパック中にエラー")
            continue

        save_path_id, csv_file_name = setup_test_environment(test_execution, output_to_spreadsheet, save_path_id, csv_file_name, config['spreadsheet_id'], config['json_keyfile_path'])

        sql_query = load_sql_from_file(sql_file_name, config['google_folder_id'], config['json_keyfile_path'])
        if sql_query:
            try:
                input_values, input_fields_types = {}, {}
                sql_query_with_period_condition = set_period_condition(period_condition, period_criteria, sql_query, category)
                if category != 'マスタ':
                    sql_query_with_conditions = add_conditions_to_sql(sql_query_with_period_condition, input_values, input_fields_types, deletion_exclusion)
                else:
                    sql_query_with_conditions = sql_query_with_period_condition
            except Exception as e:
                LOGGER.error(f"SQLクエリの処理中にエラーが発生しました: {e}")
                result = f"★失敗★　{sql_file_name}: SQLクエリの処理中にエラー"
                results.append(result)
                continue
        else:
            LOGGER.warning(f"{sql_file_name} の読み込みに失敗しました。代わりに 'SELECT *' を実行します。")
            sql_query_with_conditions = f"SELECT * -- FROM clause\nFROM {main_table_name}"

        try:
            if output_to_spreadsheet == 'CSV':
                if save_path_id and save_path_id.strip():
                    csv_file_path = os.path.join(save_path_id, csv_file_name)
                else:
                    csv_file_path = os.path.join(config['csv_base_path'], csv_file_name)
                try:
                    csvfile_export(conn, sql_query_with_conditions, csv_file_path, main_table_name, category, config['json_keyfile_path'], config['spreadsheet_id'], csv_file_name, csv_file_name_column, sheet_name, config['chunk_size'], config['delay'])
                except Exception as e:
                    LOGGER.error(f"CSVファイルのエクスポート中にエラーが発生しました: {e}")
                    result = f"★失敗★　{sql_file_name}: CSVファイルのエクスポート中にエラー"
                else:
                    result = f"☆成功☆　{sql_file_name}: 保存先: {csv_file_path}"
            elif output_to_spreadsheet == 'スプシ':
                if not csv_file_name:
                    csv_file_name = csv_file_name_column
                try:
                     export_to_spreadsheet(conn, sql_query_with_conditions, save_path_id, csv_file_name, config['json_keyfile_path'], paste_format, sheet_name, csv_file_name_column, main_table_name, category, config['chunk_size'], config['delay'])
                except Exception as e:
                    LOGGER.error(f"スプレッドシートへのエクスポート中にエラーが発生しました: {e}")
                    result = f"★失敗★　{sql_file_name}: スプレッドシートへのエクスポート中にエラー"
                else:
                    result = f"☆成功☆　{sql_file_name}: 保存先: {save_path_id}, シート名: {csv_file_name}"
            else:
                LOGGER.error(f"無効な出力先が指定されました: {output_to_spreadsheet}")
                result = f"★失敗★　{sql_file_name}: 無効な出力先: {output_to_spreadsheet}"
        except Exception as e:
            LOGGER.error(f"出力処理中にエラーが発生しました: {e}")
            result = f"★失敗★　{sql_file_name}: 出力処理中にエラー"
        results.append(result)
    return results

def main(sheet_name, execution_column, config_file):
    LOGGER = setup_department_logger('main')
    
    ssh_config, db_config, local_port, config = load_config(config_file)
    
    sql_and_csv_files = load_sql_file_list_from_spreadsheet(config['spreadsheet_id'], sheet_name, config['json_keyfile_path'], execution_column)
    
    ssh_config['db_host'] = db_config['host']
    ssh_config['db_port'] = db_config['port']
    ssh_config['local_port'] = local_port
    
    tunnel = create_ssh_tunnel(ssh_config)
    
    results = []
    
    if tunnel:
        try:
            conn = create_database_connection(db_config, tunnel.local_bind_port)
            if conn:
                results = process_sql_and_csv_files(sql_and_csv_files, conn, config)
            else:
                LOGGER.error("データベース接続に失敗しました。")
        except Exception as e:
            LOGGER.error(f"処理中にエラーが発生しました: {traceback.format_exc()}")
            slack_notify.send_slack_error_message(e, config=config)
        finally:
            if conn:
                conn.close()
                LOGGER.info("データベース接続を閉じました。")  # 一度だけログ出力
            tunnel.stop()
            LOGGER.info("SSHトンネルを閉じました。")  # 一度だけログ出力
        
        print("\n処理結果一覧:")
        for result in results:
            print(result)