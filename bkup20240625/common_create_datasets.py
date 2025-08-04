import os
import pandas as pd
from config_loader import load_config
from my_logging import setup_department_logger
from subcode_loader import load_sql_file_list_from_spreadsheet, get_data_types, apply_data_types_to_df, execute_sql_query_with_conditions
from subcode_streamlit_loader import load_sheet_from_spreadsheet, format_dates
from db_utils import get_connection

def main(sheet_name, execution_column, config_file):
    # 設定ファイルの読み込み
    ssh_config, db_config, local_port, additional_config = load_config(config_file)

    # ロガーの設定
    LOGGER = setup_department_logger('main')

    # カレントディレクトリを基準にしたディレクトリパスを作成
    output_dir = os.path.join(os.getcwd(), 'data_Parquet')
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            LOGGER.info(f"ディレクトリを作成しました: {output_dir}")
        except Exception as e:
            LOGGER.error(f"ディレクトリ作成中にエラーが発生しました: {e}")

    # SQLファイルリストの取得
    spreadsheet_id = additional_config['spreadsheet_id']
    json_keyfile_path = additional_config['json_keyfile_path']

    # ワークシートの読み込みと列名の確認
    worksheet = load_sheet_from_spreadsheet(sheet_name)
    if worksheet:
        headers = worksheet.row_values(1)
        LOGGER.info(f"スプレッドシートの列名: {headers}")
    else:
        LOGGER.error(f"ワークシート {sheet_name} の読み込みに失敗しました。")

    sql_files_list = load_sql_file_list_from_spreadsheet(spreadsheet_id, sheet_name, json_keyfile_path, execution_column=execution_column)

    # データをParquet形式で保存する関数
    def save_to_parquet(df, output_path):
        try:
            df.to_parquet(output_path, engine='pyarrow', index=False)
            LOGGER.info(f"データをParquet形式で保存しました: {output_path}")
        except Exception as e:
            LOGGER.error(f"Parquet保存中にエラーが発生しました: {e}")

    conn = get_connection(config_file)
    if conn:
        for entry in sql_files_list:
            sql_file_name, csv_file_name, period_condition, period_criteria, deletion_exclusion, category, main_table_name, *_ = entry
            display_name = csv_file_name
            try:
                sql_query = execute_sql_query_with_conditions(sql_file_name, additional_config, period_condition, period_criteria, deletion_exclusion, category, main_table_name)
                if sql_query:
                    df = pd.read_sql_query(sql_query, conn)
                    if not df.empty:
                        LOGGER.info(f"{display_name}のデータを取得しました。")
                        
                        # 型指定のためのシートを読み込む
                        sheet_name_for_data_types = os.path.splitext(sql_file_name)[0]  # SQLファイル名から拡張子を除去
                        try:
                            worksheet = load_sheet_from_spreadsheet(sheet_name_for_data_types)
                            if worksheet:
                                data_types = get_data_types(worksheet)
                                LOGGER.info(f"データ型の取得に成功しました: {data_types}")
                            else:
                                data_types = {}
                                LOGGER.warning(f"ワークシート '{sheet_name_for_data_types}' が見つかりませんでした。デフォルトの型を使用します。")
                        except Exception as e:
                            data_types = {}
                            LOGGER.error(f"ワークシート '{sheet_name_for_data_types}' の読み込み中にエラーが発生しました: {e}")
                        
                        # 日付のフォーマットを統一
                        df = format_dates(df, data_types)
                        
                        # データ型を適用
                        try:
                            df = apply_data_types_to_df(df, data_types, LOGGER)
                        except Exception as e:
                            LOGGER.error(f"型変換中にエラーが発生しました: {e}")
                            continue
                        
                        # SQLファイル名から拡張子を.parquetに変更
                        base_name = os.path.splitext(sql_file_name)[0]
                        output_file_path = os.path.join(output_dir, f"{base_name}.parquet")
                        save_to_parquet(df, output_file_path)
                    else:
                        LOGGER.error(f"{display_name}のデータが空です。")
                else:
                    LOGGER.error(f"{display_name}のSQLクエリの取得に失敗しました。")
            except Exception as e:
                LOGGER.error(f"SQLクエリの実行中にエラーが発生しました: {e}")
                continue
        conn.close()
    else:
        LOGGER.error("データベース接続の取得に失敗しました。")