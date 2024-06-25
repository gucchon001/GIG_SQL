import os
import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser
from datetime import date, datetime
from my_logging import setup_department_logger
import traceback
import numpy as np

# CSSファイルを読み込む関数
def load_css(file_name):
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSSファイル '{file_name}' が見つかりませんでした。")

# CSSファイルを読み込む
load_css("styles.css")

LOGGER = setup_department_logger('main')

# グローバル変数 df を宣言
df = None

# データをParquetに保存する前にフォーマットを統一する
def format_dates(df, data_types):
    for column, data_type in data_types.items():
        if column in df.columns:
            try:
                if data_type == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d')
                elif data_type == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
            except pd.errors.OutOfBoundsDatetime as e:
                LOGGER.error(f"OutOfBoundsDatetimeエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # エラー発生時にはNaTに変換
            except Exception as e:
                LOGGER.error(f"日付フォーマット中にエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # その他のエラー発生時にもNaTに変換
    return df

# Google Sheets APIへの認証処理を共通化
def get_google_sheets_client():
    config_file = 'config.ini'
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    json_keyfile_path = config['Credentials']['json_keyfile_path']
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    return client

# SQLファイルリストをスプレッドシートから読み込む関数
@st.cache_resource(ttl=3600)
def load_sql_list_from_spreadsheet():
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    sheet_name = config['Spreadsheet']['eachdata_sheet']
    
    client = get_google_sheets_client()

    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    data = sheet.get_all_values()

    header = data[0]
    target_index = header.index('個別リスト')
    sql_file_name_index = header.index('sqlファイル名')
    csv_file_name_index = header.index('CSVファイル呼称')

    records = {
        row[csv_file_name_index]: row[sql_file_name_index]
        for row in data[1:]
        if row[target_index].lower() == 'true'
    }

    return records

# 指定されたプルダウン選択肢に対応するSQLファイル名から.sql拡張子を除去する関数
def get_sql_file_name(selected_option):
    records = load_sql_list_from_spreadsheet()
    sql_file_name = records.get(selected_option)

    if sql_file_name:
        return sql_file_name.replace('.sql', '')
    else:
        return None

# スプレッドシートからデータを読み込む処理を共通化
def load_sheet_from_spreadsheet(sheet_name):
    client = get_google_sheets_client()

    config_file = 'config.ini'
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        return sheet
    except gspread.exceptions.WorksheetNotFound:
        return None
    except Exception as e:
        return None

# 選択シートの条件を取得する関数
def get_filtered_data_from_sheet(sheet):
    try:
        header_row = sheet.row_values(1)
        cleaned_header_row = [h.strip().lower() for h in header_row]

        if len(cleaned_header_row) != len(set(cleaned_header_row)):
            raise ValueError("ヘッダ行に重複する項目があります。")

        records = sheet.get_all_records()

        filtered_data = []
        for record in records:
            if record['絞込'] == 'TRUE':
                data = {
                    'db_item': record['DB項目'],
                    'table_name': record['TABLE_NAME'],
                    'data_item': record['DATA_ITEM'],
                    'input_type': record['入力方式'],
                    'options': [option.split(' ') for option in record['選択項目'].split('\n') if option.strip()]
                }
                filtered_data.append(data)

        return filtered_data
    except Exception as e:
        return []

# 動的な入力フィールドを作成する関数内のチェックボックス部分
def create_dynamic_input_fields(data):
    input_fields = {}
    input_fields_types = {}
    options_dict = {}

    if not data:
        st.error("指定されている項目がありません")
        return input_fields, input_fields_types, options_dict

    num_columns = 3
    num_items = len(data)
    items_per_column = (num_items + num_columns - 1) // num_columns

    columns = st.columns(num_columns)

    for i, item in enumerate(data):
        column_index = i // items_per_column
        with columns[column_index]:
            label_text = item['db_item']

            if item['input_type'] == 'FA':
                input_fields[item['db_item']] = st.text_input(label_text, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'FA'

            elif item['input_type'] == 'プルダウン':
                options = ['-'] + list(set([option[1] for option in item['options'] if len(option) > 1]))
                input_fields[item['db_item']] = st.selectbox(label_text, options, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'プルダウン'
                options_dict[item['db_item']] = item['options']

            elif item['input_type'] == 'ラジオボタン':
                options = [option[1] for option in item['options'] if len(option) > 1]
                radio_key = f"radio_{item['db_item']}"
                clear_key = f"clear_radio_{item['db_item']}"

                if st.session_state.get(clear_key, False):
                    st.session_state[radio_key] = None
                    st.session_state[clear_key] = False

                st.text(label_text)  # フリーワードのタイトルと同じサイズと文字タイプに統一
                radio_index = st.radio("", range(len(options)), format_func=lambda i: options[i], index=st.session_state.get(radio_key, None), key=radio_key)
                input_fields[item['db_item']] = options[radio_index] if radio_index is not None else None
                input_fields_types[item['db_item']] = 'ラジオボタン'
                options_dict[item['db_item']] = item['options']

            elif item['input_type'] == 'チェックボックス':
                st.text(label_text)  # フリーワードのタイトルと同じサイズと文字タイプに統一
                checkbox_values = {}
                for option in item['options']:
                    if len(option) > 1:
                        checkbox_values[option[1]] = st.checkbox(option[1], key=f"checkbox_{item['db_item']}_{option[0]}")  # ラベルをキーとして使用
                input_fields[item['db_item']] = checkbox_values
                input_fields_types[item['db_item']] = 'チェックボックス'
                options_dict[item['db_item']] = item['options']  # オプションを保存

            elif item['input_type'] == 'Date':
                start_date = st.date_input(f"{label_text} 開始日", key=f"start_date_{item['db_item']}", value=None)
                end_date = st.date_input(f"{label_text} 終了日", key=f"end_date_{item['db_item']}", value=None)
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'date'

            elif item['input_type'] == 'Datetime':
                start_date = st.date_input(f"{label_text} 開始日", key=f"start_datetime_{item['db_item']}", value=None)
                end_date = st.date_input(f"{label_text} 終了日", key=f"end_datetime_{item['db_item']}", value=None)
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'datetime'

    st.session_state['input_fields'] = input_fields
    st.session_state['input_fields_types'] = input_fields_types
    st.session_state['options_dict'] = options_dict

    return input_fields, input_fields_types, options_dict

# セッションステートを初期化する関数
def initialize_session_state():
    if 'selected_sql_file' not in st.session_state:
        st.session_state['selected_sql_file'] = None
    if 'df' not in st.session_state:
        st.session_state['df'] = None
    if 'limit' not in st.session_state:
        st.session_state['limit'] = 20
    if 'total_records' not in st.session_state:
        st.session_state['total_records'] = 0
    if 'selected_rows' not in st.session_state:
        st.session_state['selected_rows'] = 20
    if 'input_fields' not in st.session_state:
        st.session_state['input_fields'] = []
    if 'input_fields_types' not in st.session_state:
        st.session_state['input_fields_types'] = []
    if 'options_dict' not in st.session_state:
        st.session_state['options_dict'] = {}

# スタイルを適用する関数
def apply_styles(df):
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'

    styled_df = df.style.apply(highlight_header, axis=0).applymap(white_background, subset=pd.IndexSlice[:, :])

    return styled_df

# テキストを省略表示する関数
def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

def load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types, options_dict):
    try:
        df = pd.read_parquet(parquet_file_path)
        # None または nan 値を各列のデータ型に応じた値に置換
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].fillna('')
            elif df[column].dtype == 'Int64':
                df[column] = df[column].fillna(0)  # Int64型の列はNaNを0に置換
            elif df[column].dtype == 'float64':
                df[column] = df[column].fillna(np.nan)  # float64型の列はNaNをnp.nanに置換
            else:
                df[column] = df[column].fillna(pd.NA)  # その他の型はpd.NAに置換

        # DataFrameのデータ型をロギング
        LOGGER.info(f"DataFrame dtypes: {df.dtypes}")

        for field, value in input_fields.items():
            LOGGER.info(f"Filter condition for {field}: {value}")  # フィルター条件をロギング

            if input_fields_types[field] == 'FA' and value:
                if df[field].dtype == 'Int64':
                    try:
                        df = df[df[field] == int(value)]
                    except ValueError:
                        pass  # 無効な値の場合はフィルタリングをスキップ
                else:
                    df = df[df[field].astype(str).str.contains(value, na=False)]
            elif input_fields_types[field] == 'プルダウン' and value != '-':
                df = df[df[field] == value]
            elif input_fields_types[field] == 'ラジオボタン' and value:
                df = df[df[field] == value]
            elif input_fields_types[field] == 'チェックボックス':
                selected_labels = [label for label, selected in value.items() if selected]
                if selected_labels:
                    df[field] = df[field].astype(str)  # 文字列に変換
                    df = df[df[field].isin(selected_labels)]
            elif input_fields_types[field] == 'date' or input_fields_types[field] == 'datetime':
                default_start_date = '2010-01-01'
                default_end_date = '2100-12-31'
                start_date = value['start_date'] if value['start_date'] else default_start_date
                end_date = value['end_date'] if value['end_date'] else default_end_date
                LOGGER.info(f"Date/Datetime range filter for {field}: Start date: {start_date}, End date: {end_date}")
                LOGGER.info(f"Data type of {field} before conversion: {df[field].dtype}")
                
                # データフレーム内の日付列をdatetime型に変換
                df[field] = pd.to_datetime(df[field], errors='coerce')
                LOGGER.info(f"Data type of {field} after conversion: {df[field].dtype}")
                
                # 入力された日付をdatetime型に変換
                start_datetime = pd.to_datetime(start_date).floor('D')
                end_datetime = pd.to_datetime(end_date).ceil('D') - pd.Timedelta(seconds=1)
                
                LOGGER.info(f"Converted start_datetime: {start_datetime}, end_datetime: {end_datetime}")
                
                try:
                    df = df[(df[field] >= start_datetime) & (df[field] <= end_datetime)]
                except Exception as e:
                    LOGGER.error(f"フィルタリング中にエラーが発生しました: {e}")
                    LOGGER.error(f"Field: {field}, dtype: {df[field].dtype}")
                    LOGGER.error(f"Start datetime: {start_datetime}, End datetime: {end_datetime}")
                    LOGGER.error(f"Sample data: {df[field].head()}")
                    raise

        if df.empty:
            return pd.DataFrame()
        else:
            return df.sort_index(ascending=False)  # フィルタリング後に降順に並べ替え
    except Exception as e:
        LOGGER.error(f"データフィルタリング中にエラーが発生しました: {e}")
        LOGGER.error(traceback.format_exc())
        return None

# Parquetファイルの選択時の処理
def on_sql_file_change(sql_files_dict):
    try:
        selected_display_name = st.session_state['selected_display_name']
        st.session_state['selected_sql_file'] = sql_files_dict[selected_display_name]
        sql_file_name = get_sql_file_name(selected_display_name)
        parquet_file_path = f"data_parquet/{sql_file_name}.parquet"

        if os.path.exists(parquet_file_path):
            df = pd.read_parquet(parquet_file_path)
            df = df.sort_index(ascending=False)  # インデックスの降順で並べ替え
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
        else:
            st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
    except Exception as e:
        st.error(f"Error in on_sql_file_change: {e}")

def calculate_offset(page_number, page_size):
    return (page_number - 1) * page_size

def on_limit_change():
    st.session_state['limit'] = st.session_state['rows_selectbox']
    st.session_state['selected_rows'] = st.session_state['limit']
    df = st.session_state['df']
    if df is not None:
        page_number = st.session_state.get('current_page', 1)  # 現在のページ番号を取得（デフォルトは1）
        st.session_state['df_view'] = load_and_prepare_data(df, page_number, st.session_state['selected_rows'])

# データフレームを制限して準備する関数
def load_and_prepare_data(df, page_number, page_size):
    if df is None:
        return pd.DataFrame()  # 空のDataFrameを返す
    offset = calculate_offset(page_number, page_size)
    limited_df = df.sort_index(ascending=False).iloc[offset:offset + page_size]  # インデックスの降順で並べ替えてからオフセットを考慮して行数を制限
    return limited_df

# 検索ボタンがクリックされた場合の処理
def on_search_click():
    input_values = st.session_state['input_fields']
    input_fields_types = st.session_state['input_fields_types']
    sql_file_name = get_sql_file_name(st.session_state['selected_display_name'])
    parquet_file_path = f"data_parquet/{sql_file_name}.parquet"

    if os.path.exists(parquet_file_path):
        df = load_and_filter_parquet(parquet_file_path, input_values, input_fields_types)
        if df is None:
            st.error("該当の検索結果はありませんでした。")
        elif df.empty:
            st.error("該当の検索結果はありませんでした。")
        else:
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
    else:
        st.error(f"データの読み込みまたはフィルタリングに失敗しました: {parquet_file_path}")

def get_parquet_file_last_modified(parquet_file_path):
    if os.path.exists(parquet_file_path):
        last_modified_timestamp = os.path.getmtime(parquet_file_path)
        last_modified_datetime = datetime.fromtimestamp(last_modified_timestamp)
        last_modified_str = last_modified_datetime.strftime("%Y-%m-%d %H:%M:%S")
        LOGGER.info(f"Parquet file {parquet_file_path} last modified: {last_modified_str}")
        return last_modified_str
    else:
        LOGGER.warning(f"Parquet file {parquet_file_path} does not exist")
        return None