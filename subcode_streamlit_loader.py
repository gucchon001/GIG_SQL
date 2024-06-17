import os
import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser
from datetime import date,datetime
from my_logging import setup_department_logger
import re

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
@st.cache_data(ttl=3600)
def load_sql_list_from_spreadsheet():
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    sheet_name = config['Spreadsheet']['main_sheet']

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
                options = ['-'] + [option[1] for option in item['options'] if len(option) > 1]
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
                start_date = st.date_input(f"{label_text} 開始日", key=f"start_date_{item['db_item']}")
                end_date = st.date_input(f"{label_text} 終了日", key=f"end_date_{item['db_item']}")
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'date'

            elif item['input_type'] == 'Datetime':
                start_datetime = st.date_input(f"{label_text} 開始日時", value=datetime.now(), key=f"start_datetime_{item['db_item']}")
                end_datetime = st.date_input(f"{label_text} 終了日時", value=datetime.now(), key=f"end_datetime_{item['db_item']}")
                input_fields[item['db_item']] = {'start_datetime': start_datetime, 'end_datetime': end_datetime}
                input_fields_types[item['db_item']] = 'datetime'

    st.session_state['input_fields'] = input_fields
    st.session_state['input_fields_types'] = input_fields_types
    st.session_state['options_dict'] = options_dict

    LOGGER.info(f"Options dict after setting input fields: {options_dict}")  # ロギング追加

    return input_fields, input_fields_types, options_dict

# セッションステートを初期化する関数
def initialize_session_state():
    if 'selected_sql_file' not in st.session_state:
        st.session_state['selected_sql_file'] = None
    if 'df' not in st.session_state:
        st.session_state['df'] = None
    if 'limit' not in st.session_state:
        st.session_state['limit'] = 100
    if 'total_records' not in st.session_state:
        st.session_state['total_records'] = 0
    if 'selected_rows' not in st.session_state:
        st.session_state['selected_rows'] = 100
    if 'input_fields' not in st.session_state:
        st.session_state['input_fields'] = []
    if 'input_fields_types' not in st.session_state:
        st.session_state['input_fields_types'] = []
    if 'options_dict' not in st.session_state:
        st.session_state['options_dict'] = {}

# スタイルを適用する関数
def apply_styles(df):
    df.columns = [truncate_text(col, 20) for col in df.columns]  # ヘッダ行は20文字まで
    df = df.applymap(lambda x: truncate_text(x, 1000) if isinstance(x, str) else x)  # データ行は35文字まで

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
        LOGGER.info(f"Initial DataFrame loaded, total rows: {len(df)}")

        for field, value in input_fields.items():
            LOGGER.info(f"Filtering field: {field}, Value: {value}, Type: {input_fields_types[field]}")
            if input_fields_types[field] == 'FA' and value:
                df = df[df[field].str.contains(value, na=False)]
                LOGGER.info(f"After filtering FA field {field}, total rows: {len(df)}")
            elif input_fields_types[field] == 'プルダウン' and value != '-':
                df = df[df[field] == value]
                LOGGER.info(f"After filtering プルダウン field {field}, total rows: {len(df)}")
            elif input_fields_types[field] == 'ラジオボタン' and value:
                df = df[df[field] == value]
                LOGGER.info(f"After filtering ラジオボタン field {field}, total rows: {len(df)}")
            elif input_fields_types[field] == 'チェックボックス':
                selected_labels = [label for label, selected in value.items() if selected]
                LOGGER.info(f"Selected labels for {field}: {selected_labels}")
                if selected_labels:
                    df[field] = df[field].astype(str)  # Convert to string
                    df = df[df[field].isin(selected_labels)]
                    LOGGER.info(f"After filtering チェックボックス field {field}, total rows: {len(df)}")
            elif input_fields_types[field] == 'date' and value:
                start_date = value['start_date']
                end_date = value['end_date']
                LOGGER.info(f"Start date: {start_date}, End date: {end_date}")
                df[field] = pd.to_datetime(df[field], format='%Y/%m/%d %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d')
                if start_date and end_date:
                    df = df[(df[field] >= start_date.strftime('%Y-%m-%d')) & (df[field] <= end_date.strftime('%Y-%m-%d'))]
                elif start_date:
                    df = df[df[field] >= start_date.strftime('%Y-%m-%d')]
                elif end_date:
                    df = df[df[field] <= end_date.strftime('%Y-%m-%d')]
                LOGGER.info(f"After filtering date field {field}, total rows: {len(df)}")
            elif input_fields_types[field] == 'datetime' and value:
                start_datetime = value['start_datetime']
                end_datetime = value['end_datetime']
                LOGGER.info(f"Start datetime: {start_datetime}, End datetime: {end_datetime}")
                df[field] = pd.to_datetime(df[field], format='%Y/%m/%d %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d')
                if start_datetime and end_datetime:
                    df = df[(df[field] >= start_datetime.strftime('%Y-%m-%d')) & (df[field] <= end_datetime.strftime('%Y-%m-%d'))]
                elif start_datetime:
                    df = df[df[field] >= start_datetime.strftime('%Y-%m-%d')]
                elif end_datetime:
                    df = df[df[field] <= end_datetime.strftime('%Y-%m-%d')]
                LOGGER.info(f"After filtering datetime field {field}, total rows: {len(df)}")

        LOGGER.info(f"DataFrame loaded, total rows: {len(df)}")
        return df
    except Exception as e:
        st.error(f"データフィルタリング中にエラーが発生しました: {e}")
        LOGGER.error(f"load_and_filter_parquet: {e}")
        return pd.DataFrame()

# Parquetファイルの選択時の処理
def on_sql_file_change(sql_files_dict):
    try:
        selected_display_name = st.session_state['selected_display_name']
        st.session_state['selected_sql_file'] = sql_files_dict[selected_display_name]
        sql_file_name = get_sql_file_name(selected_display_name)
        parquet_file_path = f"data_parquet/{sql_file_name}.parquet"

        LOGGER.info(f"Selected Display Name: {selected_display_name}")
        LOGGER.info(f"SQL File Name: {sql_file_name}")
        LOGGER.info(f"Parquet File Path: {parquet_file_path}")

        if os.path.exists(parquet_file_path):
            df = pd.read_parquet(parquet_file_path)
            df = df.sort_index(ascending=False)  # インデックスの降順で並べ替え
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
            LOGGER.info(f"Loaded Parquet File: {parquet_file_path}")
            LOGGER.info(f"df after on_sql_file_change: {df.head()}")
        else:
            st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
            LOGGER.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
    except Exception as e:
        LOGGER.error(f"Error in on_sql_file_change: {e}")

def calculate_offset(page_number, page_size):
    return (page_number - 1) * page_size

# 行数の変更時の処理
def on_limit_change():
    LOGGER.info(f"Limit changed to: {st.session_state['rows_selectbox_top']}")
    st.session_state['limit'] = st.session_state['rows_selectbox_top']
    st.session_state['selected_rows'] = st.session_state['limit']
    df = st.session_state['df']
    if df is not None:
        page_number = st.session_state.get('current_page', 1)  # 現在のページ番号を取得（デフォルトは1）
        st.session_state['df_view'] = load_and_prepare_data(df, page_number, st.session_state['selected_rows'])
    LOGGER.info(f"on_limit_change_current_page?Selected rows set to: {st.session_state['selected_rows']}")

# データフレームを制限して準備する関数
def load_and_prepare_data(df, page_number, page_size):
    LOGGER.info("Entering load_and_prepare_data function.")
    if df is None:
        LOGGER.error("DataFrame is None inside load_and_prepare_data.")
        return pd.DataFrame()  # 空のDataFrameを返す
    offset = calculate_offset(page_number, page_size)
    limited_df = df.sort_index(ascending=False).iloc[offset:offset+page_size]  # インデックスの降順で並べ替えてからオフセットを考慮して行数を制限
    LOGGER.info(f"Limiting DataFrame to {page_size} rows with offset {offset}.")
    LOGGER.info(f"Limited DataFrame: {limited_df.head()}")
    return limited_df

# 検索ボタンがクリックされた場合の処理
def on_search_click():
    input_values = st.session_state['input_fields']
    input_fields_types = st.session_state['input_fields_types']
    sql_file_name = get_sql_file_name(st.session_state['selected_display_name'])
    parquet_file_path = f"data_parquet/{sql_file_name}.parquet"
    LOGGER.info(f"Search button clicked, input values: {input_values}")
    LOGGER.info(f"Input fields types: {input_fields_types}")
    LOGGER.info(f"SQL file name: {sql_file_name}")
    LOGGER.info(f"Parquet file path: {parquet_file_path}")

    if os.path.exists(parquet_file_path):
        df = load_and_filter_parquet(parquet_file_path, input_values, input_fields_types)
        st.session_state['df'] = df
        st.session_state['total_records'] = len(df)
        LOGGER.info(f"DataFrame filtered, total rows: {len(df)}")
    else:
        st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
        LOGGER.error(f"Parquet file not found: {parquet_file_path}")