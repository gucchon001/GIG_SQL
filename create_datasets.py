import os
import pandas as pd
from config_loader import load_config
from my_logging import setup_department_logger
from subcode_loader import load_sql_file_list_from_spreadsheet, get_data_types, apply_data_types_to_df,format_dates
from subcode_streamlit_loader import load_sheet_from_spreadsheet
from db_utils import execute_sql_query
import streamlit as st

# 設定ファイルの読み込み
config_file = 'config.ini'
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
sheet_name = additional_config['main_sheet']
json_keyfile_path = additional_config['json_keyfile_path']

# ワークシートの読み込みと列名の確認
worksheet = load_sheet_from_spreadsheet(sheet_name)
if worksheet:
    headers = worksheet.row_values(1)
    LOGGER.info(f"スプレッドシートの列名: {headers}")
else:
    LOGGER.error(f"ワークシート {sheet_name} の読み込みに失敗しました。")

sql_files_list = load_sql_file_list_from_spreadsheet(spreadsheet_id, sheet_name, json_keyfile_path, execution_column='個別リスト')

# データをParquet形式で保存する関数
def save_to_parquet(df, output_path):
    try:
        df.to_parquet(output_path, engine='pyarrow', index=False)
        LOGGER.info(f"データをParquet形式で保存しました: {output_path}")
    except Exception as e:
        LOGGER.error(f"Parquet保存中にエラーが発生しました: {e}")

# フィルタリング処理の修正
def load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types):
    try:
        df = pd.read_parquet(parquet_file_path)

        for field, value in input_fields.items():
            if input_fields_types[field] == 'FA' and value:
                df = df[df[field].str.contains(value, na=False)]
            elif input_fields_types[field] == 'プルダウン' and value != '-':
                df = df[df[field] == value]
            elif input_fields_types[field] == 'ラジオボタン' and value:
                df = df[df[field] == value]
            elif input_fields_types[field] == 'チェックボックス':
                for subfield, subvalue in value.items():
                    if subvalue:
                        df = df[df[field] == subfield]
            elif input_fields_types[field] == 'date' and value:
                start_date = pd.to_datetime(value['start_date'], format='%Y/%m/%d')
                end_date = pd.to_datetime(value['end_date'], format='%Y/%m/%d')
                LOGGER.info(f"Filtering date field: {field} with start date: {start_date} and end date: {end_date}")
                df = df[(pd.to_datetime(df[field], errors='coerce').dt.date >= start_date.date()) &
                        (pd.to_datetime(df[field], errors='coerce').dt.date <= end_date.date())]
            elif input_fields_types[field] == 'datetime' and value:
                start_datetime = pd.to_datetime(value['start_date'], format='%Y/%m/%d')
                end_datetime = pd.to_datetime(value['end_date'], format='%Y/%m/%d')
                LOGGER.info(f"Filtering datetime field: {field} with start datetime: {start_datetime} and end datetime: {end_datetime}")
                df = df[(pd.to_datetime(df[field], errors='coerce').dt.date >= start_datetime.date()) & 
                        (pd.to_datetime(df[field], errors='coerce').dt.date <= end_datetime.date())]

        return df
    except Exception as e:
        st.error(f"データフィルタリング中にエラーが発生しました: {e}")
        LOGGER.error(f"データフィルタリング中にエラーが発生しました: {e}")
        return pd.DataFrame()

# メイン処理
if __name__ == "__main__":
    for entry in sql_files_list:
        sql_file_name, csv_file_name, *_ = entry
        display_name = csv_file_name
        df = execute_sql_query(sql_file_name, config_file)
        if df is not None:
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
            LOGGER.error(f"{display_name}のデータの読み込みに失敗しました。")