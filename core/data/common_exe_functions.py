import traceback
from core.config.ssh_connection import create_ssh_tunnel
try:
    # 新構造のデータベース接続を優先使用
    from src.core.database.connection import create_database_connection
except ImportError:
    # フォールバック：旧構造
    from core.config.database_connection import create_database_connection
from core.config.config_loader import load_config
from .subcode_loader import (
    load_sql_file_list_from_spreadsheet,
    load_sql_from_file,
    csvfile_export,
    add_conditions_to_sql,
    set_period_condition,
    export_to_spreadsheet,
    setup_test_environment,
    parquetfile_export
)
from core.config.my_logging import setup_department_logger
from src.utils import slack_notify
import os
from datetime import datetime, timedelta, date
import time
import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = setup_department_logger('main', app_type='main')


def process_sql_and_csv_files(sql_and_csv_files, conn, config):
    results = []
    for file_info in sql_and_csv_files:
        try:
            (
                sql_file_name,
                csv_file_name,
                period_condition,
                period_criteria,
                save_path_id,
                output_to_spreadsheet,
                deletion_exclusion,
                paste_format,
                test_execution,
                category,
                main_table_name,
                csv_file_name_column,
                sheet_name
            ) = file_info
        except ValueError as e:
            LOGGER.error(f"file_infoのアンパック中にエラーが発生しました: {file_info}")
            results.append(f"★失敗★　{file_info}: file_infoのアンパック中にエラー")
            continue

        try:
            save_path_id, csv_file_name = setup_test_environment(
                test_execution,
                output_to_spreadsheet,
                save_path_id,
                csv_file_name,
                config['spreadsheet_id'],
                config['json_keyfile_path']
            )
            LOGGER.info(f"テスト環境のセットアップが完了しました: save_path_id={save_path_id}, csv_file_name={csv_file_name}")
        except Exception as e:
            LOGGER.error(f"テスト環境のセットアップ中にエラーが発生しました: {e}")
            results.append(f"★失敗★　{sql_file_name}: テスト環境のセットアップ中にエラー")
            continue

        sql_query = load_sql_from_file(sql_file_name, config['google_folder_id'], config['json_keyfile_path'])
        if sql_query:
            try:
                LOGGER.info("=" * 100)
                LOGGER.info(f"SQLファイル処理開始: {sql_file_name}")
                LOGGER.info(f"パラメータ - period_condition: '{period_condition}'")
                LOGGER.info(f"パラメータ - period_criteria: '{period_criteria}'")
                LOGGER.info(f"パラメータ - category: '{category}'")
                LOGGER.info(f"パラメータ - deletion_exclusion: '{deletion_exclusion}'")
                
                input_values, input_fields_types = {}, {}
                LOGGER.info("set_period_condition関数を呼び出します")
                sql_query_with_period_condition = set_period_condition(
                    period_condition,
                    period_criteria,
                    sql_query,
                    category
                )
                LOGGER.info("set_period_condition関数の処理完了")
                if category != 'マスタ':
                    sql_query_with_conditions = add_conditions_to_sql(
                        sql_query_with_period_condition,
                        input_values,
                        input_fields_types,
                        deletion_exclusion
                    )
                else:
                    sql_query_with_conditions = sql_query_with_period_condition
                LOGGER.info(f"SQLクエリの条件追加が完了しました: {sql_file_name}（本文非表示, 長さ: {len(sql_query_with_conditions)} 文字）")
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
                    csvfile_export(
                        conn,
                        sql_query_with_conditions,
                        csv_file_path,
                        main_table_name,
                        category,
                        config['json_keyfile_path'],
                        config['spreadsheet_id'],
                        csv_file_name,
                        csv_file_name_column,
                        sheet_name,
                        config.get('chunk_size'),
                        config.get('delay')
                    )
                except Exception as e:
                    LOGGER.error(f"CSVファイルのエクスポート中にエラーが発生しました: {e}")
                    result = f"★失敗★　{sql_file_name}: CSVファイルのエクスポート中にエラー"
                else:
                    result = f"☆成功☆　{sql_file_name}: 保存先: {csv_file_path}"
            elif output_to_spreadsheet == 'スプシ':
                if not csv_file_name:
                    csv_file_name = csv_file_name_column
                try:
                    export_to_spreadsheet(
                        conn,
                        sql_query_with_conditions,
                        save_path_id,
                        csv_file_name,
                        config['json_keyfile_path'],
                        paste_format,
                        sheet_name,
                        csv_file_name_column,
                        main_table_name,
                        category,
                        config.get('chunk_size'),
                        config.get('delay')
                    )
                except Exception as e:
                    LOGGER.error(f"スプレッドシートへのエクスポート中にエラーが発生しました: {e}")
                    result = f"★失敗★　{sql_file_name}: スプレッドシートへのエクスポート中にエラー"
                else:
                    result = f"☆成功☆　{sql_file_name}: 保存先: {save_path_id}, シート名: {csv_file_name}"
            elif output_to_spreadsheet == 'parquet':
                if save_path_id and save_path_id.strip():
                    parquet_file_path = os.path.join(save_path_id, csv_file_name.replace('.csv', '.parquet'))
                else:
                    parquet_file_path = os.path.join(config['csv_base_path'], csv_file_name.replace('.csv', '.parquet'))
                try:
                    parquetfile_export(
                        conn,
                        sql_query_with_conditions,
                        parquet_file_path,
                        main_table_name,
                        category,
                        config['json_keyfile_path'],
                        config['spreadsheet_id'],
                        csv_file_name,
                        csv_file_name_column,
                        sheet_name,
                        config.get('chunk_size'),
                        config.get('delay')
                    )
                except Exception as e:
                    LOGGER.error(f"Parquetファイルのエクスポート中にエラーが発生しました: {e}")
                    result = f"★失敗★　{sql_file_name}: Parquetファイルのエクスポート中にエラー"
                else:
                    result = f"☆成功☆　{sql_file_name}: 保存先: {parquet_file_path}"
            else:
                LOGGER.error(f"無効な出力先が指定されました: {output_to_spreadsheet}")
                result = f"★失敗★　{sql_file_name}: 無効な出力先: {output_to_spreadsheet}"
        except Exception as e:
            LOGGER.error(f"出力処理中にエラーが発生しました: {e}")
            result = f"★失敗★　{sql_file_name}: 出力処理中にエラー"
        results.append(result)

        # 各反復後にスリープを追加
        sleep_time = config.get('sleep_time', 5)  # デフォルトは5秒
        if sleep_time > 0:
            LOGGER.info(f"{sleep_time}秒待機します。")
            time.sleep(sleep_time)

    return results


def main(sheet_name, execution_column, config_file):
    LOGGER = setup_department_logger('main', app_type='main')
    LOGGER.info(f"処理開始 - sheet: {sheet_name}, column: {execution_column}")

    try:
        ssh_config, db_config, local_port, config = load_config(config_file)
        LOGGER.info("設定ファイルの読み込みに成功しました。")
    except Exception as e:
        LOGGER.error(f"設定ファイルの読み込み中にエラーが発生しました: {e}")
        slack_notify.send_slack_error_message(e, config=None)
        return

    try:
        sql_and_csv_files = load_sql_file_list_from_spreadsheet(
            config['spreadsheet_id'],
            sheet_name,
            config['json_keyfile_path'],
            execution_column
        )
        LOGGER.info(f"SQLおよびCSVファイルリストをロードしました: {len(sql_and_csv_files)} 件")
    except Exception as e:
        LOGGER.error(f"GoogleスプレッドシートからSQLファイルリストをロード中にエラーが発生しました: {e}")
        slack_notify.send_slack_error_message(e, config=config)
        return

    ssh_config['db_host'] = db_config['host']
    ssh_config['db_port'] = db_config['port']
    ssh_config['local_port'] = local_port

    try:
        tunnel = create_ssh_tunnel(ssh_config)
        LOGGER.info("SSHトンネルを作成しました。")
    except Exception as e:
        LOGGER.error(f"SSHトンネルの作成中にエラーが発生しました: {e}")
        slack_notify.send_slack_error_message(e, config=config)
        return

    results = []

    if tunnel:
        try:
            conn = create_database_connection(db_config, tunnel.local_bind_port)
            if conn:
                LOGGER.info("データベースに接続しました。")
                try:
                    results = process_sql_and_csv_files(sql_and_csv_files, conn, config)
                except Exception as e:
                    LOGGER.error(f"SQLおよびCSVファイルの処理中にエラーが発生しました: {e}")
                    slack_notify.send_slack_error_message(e, config=config)
            else:
                LOGGER.error("データベース接続に失敗しました。")
        except Exception as e:
            LOGGER.error(f"処理中にエラーが発生しました: {traceback.format_exc()}")
            slack_notify.send_slack_error_message(e, config=config)
        finally:
            if 'conn' in locals() and conn:
                conn.close()
                LOGGER.info("データベース接続を閉じました。")
            if 'tunnel' in locals() and tunnel:
                tunnel.stop()
                LOGGER.info("SSHトンネルを閉じました。")

            LOGGER.info("\n処理結果一覧:")
            for result in results:
                LOGGER.info(result)


if __name__ == "__main__":
    # ここに実行時の引数処理などを追加できます
    # 例:
    # import argparse
    # parser = argparse.ArgumentParser(description='Process SQL and CSV files.')
    # parser.add_argument('sheet_name', type=str, help='The name of the sheet to process.')
    # parser.add_argument('execution_column', type=str, help='The name of the execution column.')
    # parser.add_argument('config_file', type=str, help='Path to the configuration file.')
    # args = parser.parse_args()
    # main(args.sheet_name, args.execution_column, args.config_file)
    pass
