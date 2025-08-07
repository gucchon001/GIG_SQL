# common_create_datasets.py

import os
import pandas as pd
from config_loader import load_config
try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('main')
except ImportError:
    # フォールバック：旧構造
    from my_logging import setup_department_logger
    # LOGGER は上記のtryブロックで設定済み
from subcode_loader import (
    load_sql_file_list_from_spreadsheet, 
    get_data_types, 
    apply_data_types_to_df, 
    execute_sql_query_with_conditions
)
from subcode_streamlit_loader import load_sheet_from_spreadsheet, format_dates
from db_utils import get_connection

def main(sheet_name, execution_column, config_file, selected_table=None):
    # 設定ファイルの読み込み
    ssh_config, db_config, local_port, additional_config = load_config(config_file)

    # ロガーの設定
    # LOGGER は上記のtryブロックで設定済み

    # config.iniのcsv_base_pathをそのまま使用
    output_dir = additional_config['csv_base_path']
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

    sql_files_list = load_sql_file_list_from_spreadsheet(
        spreadsheet_id, 
        sheet_name, 
        json_keyfile_path, 
        execution_column=execution_column
    )
    # sql_files_list の内容をログに出力
    LOGGER.debug("sql_files_list の内容:")
    for i, entry in enumerate(sql_files_list):
        LOGGER.debug(f"エントリー {i}:")
        LOGGER.debug(f"  sql_file_name: {entry[0]}")
        LOGGER.debug(f"  csv_file_name: {entry[1]}")
        LOGGER.debug(f"  period_condition: {entry[2]}")
        LOGGER.debug(f"  period_criteria: {entry[3]}")
        LOGGER.debug(f"  deletion_exclusion: {entry[4]}")
        LOGGER.debug(f"  category: {entry[5]}")
        LOGGER.debug(f"  main_table_name: {entry[6]}")
        if len(entry) > 7:
            LOGGER.debug(f"  その他の要素: {entry[7:]}")

    def save_to_parquet(df, output_path):
        try:
            # インデックスをリセットする前に、現在のインデックスを列として保存
            if df.index.name is None:
                df['original_index'] = df.index
            else:
                df[df.index.name] = df.index

            # インデックスをリセットし、降順でソート
            df_sorted = df.reset_index(drop=True).sort_values('original_index', ascending=False)
            
            # 'original_index'列を削除
            df_sorted = df_sorted.drop('original_index', axis=1)
            
            df_sorted.to_parquet(output_path, engine='pyarrow', index=False)
            LOGGER.info(f"データをParquet形式で降順で保存しました: {output_path}")
        except Exception as e:
            LOGGER.error(f"Parquet保存中にエラーが発生しました: {e}")

    conn = get_connection(config_file)
    if conn:
        for entry in sql_files_list:
            sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution, category, main_table_name, csv_file_name_column, sheet_name = entry

            # テーブル名が指定されている場合、選択されたテーブルのみを処理
            if selected_table and main_table_name != selected_table:
                LOGGER.info(f"スキップ: {main_table_name} は選択されたテーブルではありません。")
                continue

            LOGGER.debug(f"処理中のエントリー:")
            LOGGER.debug(f"  sql_file_name: {sql_file_name}")
            LOGGER.debug(f"  csv_file_name: {csv_file_name}")
            LOGGER.debug(f"  period_condition: {period_condition}")
            LOGGER.debug(f"  period_criteria: {period_criteria}")
            LOGGER.debug(f"  save_path_id: {save_path_id}")
            LOGGER.debug(f"  output_to_spreadsheet: {output_to_spreadsheet}")
            LOGGER.debug(f"  deletion_exclusion: {deletion_exclusion}")
            LOGGER.debug(f"  category: {category}")
            LOGGER.debug(f"  main_table_name: {main_table_name}")

            display_name = csv_file_name
            try:
                LOGGER.debug(f"main処理 - deletion_exclusion: {deletion_exclusion}")
                sql_query = execute_sql_query_with_conditions(
                    sql_file_name,
                    additional_config,
                    period_condition,
                    period_criteria,
                    deletion_exclusion,
                    category,
                    main_table_name
                )
                if sql_query:
                    LOGGER.info(sql_query)
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
