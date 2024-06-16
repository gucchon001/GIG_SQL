import os
import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser
from datetime import date,datetime
from my_logging import setup_department_logger

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

# 動的な入力フィールドを作成する関数
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
                radio_index = st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")
                input_fields[item['db_item']] = options[radio_index] if radio_index is not None else None
                input_fields_types[item['db_item']] = 'ラジオボタン'
                options_dict[item['db_item']] = item['options']

                clear_radio = st.checkbox("選択肢を外す", key=f"clear_radio_{item['db_item']}")
                if clear_radio:
                    input_fields[item['db_item']] = None
                    st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")

            elif item['input_type'] == 'チェックボックス':
                checkbox_values = {}
                for option in item['options']:
                    if len(option) > 1:
                        checkbox_values[option[0]] = st.checkbox(option[1], key=f"checkbox_{item['db_item']}_{option[0]}")
                input_fields[item['db_item']] = checkbox_values
                input_fields_types[item['db_item']] = 'チェックボックス'

            elif item['input_type'] == 'Date':
                start_datetime = st.date_input(f"開始日時", value=datetime.now(), key=f"start_datetime_{item['db_item']}")
                end_datetime = st.date_input(f"終了日時", value=datetime.now(), key=f"end_datetime_{item['db_item']}")
                input_fields[item['db_item']] = {'start_datetime': start_datetime, 'end_datetime': end_datetime}
                input_fields_types[item['db_item']] = 'Datetime'

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
    df = df.applymap(lambda x: truncate_text(x, 35) if isinstance(x, str) else x)  # データ行は35文字まで

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

# Parquetファイルを読み込み、条件に基づいてフィルタリングする関数
def load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types):
    LOGGER.info("Entering load_and_filter_parquet function.")
    try:
        df = pd.read_parquet(parquet_file_path)
        LOGGER.info(f"Parquet file loaded successfully: {parquet_file_path}, DataFrame shape: {df.shape}")
        
        # フィルタリングロジックをここに追加する
        for field, value in input_fields.items():
            if input_fields_types[field] == 'FA' and value:
                LOGGER.info(f"Filtering FA field: {field} with value: {value}")
                df = df[df[field].str.contains(value, na=False)]
            elif input_fields_types[field] == 'プルダウン' and value != '-':
                LOGGER.info(f"Filtering プルダウン field: {field} with value: {value}")
                df = df[df[field] == value]
            elif input_fields_types[field] == 'ラジオボタン' and value:
                LOGGER.info(f"Filtering ラジオボタン field: {field} with value: {value}")
                df = df[df[field] == value]
            elif input_fields_types[field] == 'チェックボックス' and isinstance(value, dict):
                for subfield, subvalue in value.items():
                    if subvalue:
                        LOGGER.info(f"Filtering チェックボックス field: {field} with subfield: {subfield} and value: {subvalue}")
                        df = df[df[field] == subfield]
            elif input_fields_types[field] == 'Datetime' and isinstance(value, dict):
                start_datetime = value.get('start_datetime')
                end_datetime = value.get('end_datetime')
                if start_datetime and end_datetime:
                    LOGGER.info(f"Filtering Datetime field: {field} with start datetime: {start_datetime} and end datetime: {end_datetime}")
                    df = df[(df[field] >= pd.to_datetime(start_datetime)) & (df[field] <= pd.to_datetime(end_datetime))]
                elif start_datetime:
                    LOGGER.info(f"Filtering Datetime field: {field} with start datetime: {start_datetime}")
                    df = df[df[field] >= pd.to_datetime(start_datetime)]
                elif end_datetime:
                    LOGGER.info(f"Filtering Datetime field: {field} with end datetime: {end_datetime}")
                    df = df[df[field] <= pd.to_datetime(end_datetime)]
        LOGGER.info(f"DataFrame after filtering, shape: {df.shape}")
        return df
    except Exception as e:
        LOGGER.error(f"Error during loading and filtering Parquet file: {parquet_file_path}, Error: {e}")
        st.error(f"データフィルタリング中にエラーが発生しました: {e}")
        return pd.DataFrame()  # 空のデータフレームを返す

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
    LOGGER.info(f"Selected rows set to: {st.session_state['selected_rows']}")

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

# ページネーションの設定
if 'limit' in st.session_state:
    page_size = st.session_state['limit']
else:
    page_size = 100  # デフォルト値を設定
current_page = st.number_input('Current Page', min_value=1, value=1, step=1)

# データフレームの行数を制限して取得
df = load_and_prepare_data(df, current_page, page_size)  # オフセットとページサイズを考慮してデータを準備
styled_df = apply_styles(df)

# テーブルの高さを行数に応じて動的に設定
table_height = min(600, 24 * page_size)  # 1行あたり約24pxの高さを確保
st.dataframe(styled_df, height=table_height, use_container_width=True)
LOGGER.info(f"Table displayed with limit: {page_size} and offset: {calculate_offset(current_page, page_size)}")

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

# Parquetファイルを読み込み、条件に基づいてフィルタリングする関数
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
            elif input_fields_types[field] == 'Date' and value:
                start_date = value['start_date']
                end_date = value['end_date']
                df = df[(df[field] >= start_date) & (df[field] <= end_date)]

        return df
    except Exception as e:
        st.error(f"データフィルタリング中にエラーが発生しました: {e}")
        return pd.DataFrame()