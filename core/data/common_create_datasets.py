# common_create_datasets.py

import os
import pandas as pd
from core.config.config_loader import load_config
try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('datasets')
except ImportError:
    # フォールバック：旧構造
    from core.config.my_logging import setup_department_logger
    LOGGER = setup_department_logger('datasets', app_type='datasets')
from .subcode_loader import (
    load_sql_file_list_from_spreadsheet, 
    get_data_types, 
    apply_data_types_to_df, 
    execute_sql_query_with_conditions,
    csvfile_export,
    parquetfile_export,
    export_to_spreadsheet,
    setup_test_environment
)
from ..utils.db_utils import get_connection
try:
    # 新構造のデータ処理を優先使用
    from src.utils.data_processing import format_dates
except ImportError:
    # フォールバック：旧構造
    def format_dates(df, data_types):
        """日付フォーマット処理のフォールバック実装"""
        for column, data_type in data_types.items():
            if column in df.columns:
                try:
                    if data_type == 'date':
                        df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d')
                    elif data_type == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
                except Exception as e:
                    LOGGER.error(f"日付フォーマット中にエラーが発生しました: {e} (列: {column})")
                    df[column] = pd.NaT
        return df

def main(sheet_name, execution_column, config_file, selected_table=None):
    # 設定ファイルの読み込み
    ssh_config, db_config, local_port, additional_config = load_config(config_file)

    # ロガーの設定
    # LOGGER は上記のtryブロックで設定済み

    # SQLファイルリストの取得
    spreadsheet_id = additional_config['spreadsheet_id']
    json_keyfile_path = additional_config['json_keyfile_path']

    # スプレッドシートの情報をログ出力
    LOGGER.info(f"処理対象シート: {sheet_name}")
    LOGGER.info(f"実行列: {execution_column}")

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
        processed_count = 0
        total_count = len([e for e in sql_files_list if not selected_table or e[10] == selected_table])
        
        for entry in sql_files_list:
            sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution, category, main_table_name, csv_file_name_column, sheet_name_record = entry

            # テーブル名が指定されている場合、選択されたテーブルのみを処理
            if selected_table and main_table_name != selected_table:
                LOGGER.info(f"スキップ: {main_table_name} は選択されたテーブルではありません。")
                continue
                
            processed_count += 1
            LOGGER.info(f"📊 処理中 ({processed_count}/{total_count}): {main_table_name} を開始します")

            # テスト環境のセットアップ
            try:
                save_path_id, csv_file_name = setup_test_environment(
                    test_execution,
                    output_to_spreadsheet,
                    save_path_id,
                    csv_file_name,
                    additional_config['spreadsheet_id'],
                    additional_config['json_keyfile_path']
                )
                LOGGER.info(f"テスト環境のセットアップが完了しました: save_path_id={save_path_id}, csv_file_name={csv_file_name}")
            except Exception as e:
                LOGGER.error(f"テスト環境のセットアップ中にエラーが発生しました: {e}")
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
                    
                    # スプレッドシートの設定に基づいて出力処理を分岐
                    if output_to_spreadsheet == 'CSV':
                        # CSV出力
                        if save_path_id and save_path_id.strip():
                            csv_file_path = os.path.join(save_path_id, csv_file_name)
                        else:
                            csv_file_path = os.path.join(additional_config['csv_base_path'], csv_file_name)
                        
                        try:
                            csvfile_export(
                                conn,
                                sql_query,
                                csv_file_path,
                                main_table_name,
                                category,
                                additional_config['json_keyfile_path'],
                                additional_config['spreadsheet_id'],
                                csv_file_name,
                                csv_file_name_column,
                                sheet_name_record,
                                additional_config.get('chunk_size'),
                                additional_config.get('delay')
                            )
                            LOGGER.info(f"✅ 完了 ({processed_count}/{total_count}): {main_table_name} -> {csv_file_path}")
                        except Exception as e:
                            LOGGER.error(f"❌ エラー ({processed_count}/{total_count}): {main_table_name} - {e}")
                            
                    elif output_to_spreadsheet == 'スプシ':
                        # スプレッドシート出力
                        if not csv_file_name:
                            csv_file_name = csv_file_name_column
                        try:
                            export_to_spreadsheet(
                                conn,
                                sql_query,
                                save_path_id,
                                csv_file_name,
                                additional_config['json_keyfile_path'],
                                paste_format,
                                sheet_name_record,
                                csv_file_name_column,
                                main_table_name,
                                category,
                                additional_config.get('chunk_size'),
                                additional_config.get('delay')
                            )
                            LOGGER.info(f"✅ 完了 ({processed_count}/{total_count}): {main_table_name} -> スプレッドシート: {csv_file_name}")
                        except Exception as e:
                            LOGGER.error(f"❌ エラー ({processed_count}/{total_count}): {main_table_name} - {e}")
                            
                    elif output_to_spreadsheet == 'parquet':
                        # Parquet出力
                        if save_path_id and save_path_id.strip():
                            parquet_file_path = os.path.join(save_path_id, csv_file_name.replace('.csv', '.parquet'))
                        else:
                            parquet_file_path = os.path.join(additional_config['csv_base_path'], csv_file_name.replace('.csv', '.parquet'))
                        
                        try:
                            parquetfile_export(
                                conn,
                                sql_query,
                                parquet_file_path,
                                main_table_name,
                                category,
                                additional_config['json_keyfile_path'],
                                additional_config['spreadsheet_id'],
                                csv_file_name,
                                csv_file_name_column,
                                sheet_name_record,
                                additional_config.get('chunk_size'),
                                additional_config.get('delay')
                            )
                            LOGGER.info(f"✅ 完了 ({processed_count}/{total_count}): {main_table_name} -> {parquet_file_path}")
                            LOGGER.info(f"🎉 データ処理完了: {main_table_name} - 正常に保存されました")
                        except Exception as e:
                            LOGGER.error(f"❌ エラー ({processed_count}/{total_count}): {main_table_name} - {e}")
                    else:
                        # デフォルト処理（従来のParquet出力）
                        df = pd.read_sql_query(sql_query, conn)
                        if not df.empty:
                            LOGGER.info(f"{display_name}のデータを取得しました。")
                            
                            # データ型を指定（デフォルト処理）
                            data_types = {}
                            LOGGER.info("デフォルトのデータ型処理を使用します。")
                            
                            # 日付のフォーマットを統一
                            df = format_dates(df, data_types)
                            
                            # データ型を適用
                            try:
                                df = apply_data_types_to_df(df, data_types, LOGGER)
                            except Exception as e:
                                LOGGER.error(f"型変換中にエラーが発生しました: {e}")
                                continue
                            
                            # 保存先の決定
                            if save_path_id and save_path_id.strip():
                                output_dir = save_path_id
                            else:
                                output_dir = additional_config['csv_base_path']
                            
                            # ディレクトリが存在しない場合は作成
                            if not os.path.exists(output_dir):
                                try:
                                    os.makedirs(output_dir)
                                    LOGGER.info(f"ディレクトリを作成しました: {output_dir}")
                                except Exception as e:
                                    LOGGER.error(f"ディレクトリ作成中にエラーが発生しました: {e}")
                            
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
        LOGGER.info("=" * 50)
        LOGGER.info("🎉 全ての処理が正常に完了しました - SUCCESS")
        LOGGER.info("=" * 50)
    else:
        LOGGER.error("データベース接続の取得に失敗しました。")
