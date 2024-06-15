import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser
import streamlit as st
from datetime import date, datetime
from my_logging import setup_department_logger
import pandas as pd
import traceback
from st_main import load_and_prepare_sql, get_connection
import mysql.connector
from mysql.connector.errors import OperationalError
from subcode_loader import add_conditions_to_sql, extract_columns_mapping, find_table_alias, preprocess_sql_query, detect_and_remove_group_by, check_and_prepare_where_clause

# ロガーの設定
logger = setup_department_logger('main')

# Google Sheets APIへの認証処理を共通化
def get_google_sheets_client():
    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # config.iniから情報を取得
    json_keyfile_path = config['Credentials']['json_keyfile_path']

    # Google Sheets APIへの認証
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    return client

# SQLファイルリストをスプレッドシートから読み込む関数
# キャッシュデコレーターを使用してデータをキャッシュ
@st.cache_data(ttl=3600)
def load_sql_list_from_spreadsheet():
    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # config.iniから情報を取得
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    sheet_name = config['Spreadsheet']['main_sheet']

    # Google Sheets APIクライアントを取得
    client = get_google_sheets_client()

    # スプレッドシートとシートを選択
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # スプレッドシートからのデータ読み込み処理
    data = sheet.get_all_values()

    # ヘッダ行から「個別リスト」、「SQLファイル名」、「CSVファイル呼称」の列番号を取得
    header = data[0]
    target_index = header.index('個別リスト')
    sql_file_name_index = header.index('sqlファイル名')
    csv_file_name_index = header.index('CSVファイル呼称')

    # チェックボックスがONのレコードのSQLファイル名とCSVファイル呼称を取得
    records = {
        row[csv_file_name_index]: row[sql_file_name_index]
        for row in data[1:]
        if row[target_index].lower() == 'true'
    }

    return records

# 指定されたプルダウン選択肢に対応するSQLファイル名から.sql拡張子を除去する関数
def get_sql_file_name(selected_option):
    # スプレッドシートからデータを再度読み込む
    records = load_sql_list_from_spreadsheet()
    sql_file_name = records.get(selected_option)
    
    if sql_file_name:
        # .sql拡張子を除去して返す
        return sql_file_name.replace('.sql', '')
    else:
        # 該当する項目がない場合はNoneを返す
        return None

# スプレッドシートからデータを読み込む処理を共通化
def load_sheet_data(sheet_name):
    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # config.iniから情報を取得
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']

    # Google Sheets APIクライアントを取得
    client = get_google_sheets_client()

    # スプレッドシートとシートを選択
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # スプレッドシートからのデータ読み込み処理
    data = sheet.get_all_values()

    return data

# 選択シートの条件を取得する関数
def get_filtered_data_from_sheet(sheet):
    try:
        header_row = sheet.row_values(1)
        # 大文字小文字を無視して比較し、不要なスペースをトリムする
        cleaned_header_row = [h.strip().lower() for h in header_row]
        print("Cleaned Header Row:", cleaned_header_row)  # クリーンなヘッダ行を出力

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
                    'options': [option.split(' ') for option in record['選択項目'].split('\n') if option.strip()]  # オプションを設定値と名称のペアのリストに変換
                }
                filtered_data.append(data)
                #print("Filtered data item:", data)  # フィルタリングされたデータを出力

        return filtered_data
    except Exception as e:
        print("Exception in get_filtered_data_from_sheet:", e)  # デバッグ用
        return []

# 動的な入力フィールドを作成する関数
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
                options = ['-'] + [option[1] for option in item['options']]
                input_fields[item['db_item']] = st.selectbox(label_text, options, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'プルダウン'
                options_dict[item['db_item']] = item['options']

            elif item['input_type'] == 'ラジオボタン':
                options = [option[1] for option in item['options']]
                radio_index = st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")
                input_fields[item['db_item']] = options[radio_index] if radio_index != None else None
                input_fields_types[item['db_item']] = 'ラジオボタン'
                options_dict[item['db_item']] = item['options']
    
                clear_radio = st.checkbox("選択肢を外す", key=f"clear_radio_{item['db_item']}")
                if clear_radio:
                    input_fields[item['db_item']] = None
                    st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")

            elif item['input_type'] == 'チェックボックス':
                checkbox_values = {}
                for option in item['options']:
                    checkbox_values[option[0]] = st.checkbox(option[1], key=f"checkbox_{item['db_item']}_{option[0]}")
                input_fields[item['db_item']] = checkbox_values
                input_fields_types[item['db_item']] = 'チェックボックス'

            elif item['input_type'] == 'Date':
                start_date = st.date_input(f"開始日", value=date.today(), key=f"start_date_{item['db_item']}")
                end_date = st.date_input(f"終了日", value=date.today(), key=f"end_date_{item['db_item']}")
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'Date'

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

# SQL文のシート名を呼び出す関数
def load_sheet_from_spreadsheet(sheet_name):
    # Google Sheets APIクライアントを取得
    client = get_google_sheets_client()

    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # config.iniからスプレッドシートIDを取得
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']

    try:
        # スプレッドシートとシートを選択
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        print(f"Loaded sheet: {sheet_name}")
        return sheet
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet not found: {sheet_name}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
sql_files_dict = load_sql_list_from_spreadsheet()

# サイドバーで選択されたファイル名が変更された場合、テーブルの表示をリセットする関数
def on_sql_file_change():
    st.session_state['selected_sql_file'] = sql_files_dict[st.session_state['selected_display_name']]
    sql_file_name = get_sql_file_name(st.session_state['selected_display_name'])
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)

    if data:
        # SQL文を読み込み、ロギング
        sql_query = load_and_log_sql(st.session_state['selected_sql_file'], {}, {}, st.session_state['limit'])
        st.session_state['sql_query'] = sql_query

# 検索ボタンがクリックされた場合の処理を行う関数
def on_search_click():
    input_values = st.session_state['input_fields']
    input_fields_types = st.session_state['input_fields_types']
    deletion_exclusion = False  # 削除除外条件を適切に設定してください
    limit_value = st.session_state['limit']  # 選択された行数を使用

    try:
        # SQLファイルの内容を読み込む
        sql_query = load_and_prepare_sql(st.session_state['selected_sql_file'], input_values, input_fields_types)
        sql_query_with_conditions = add_conditions_to_sql(sql_query, input_values, input_fields_types, deletion_exclusion)
        
        # SQL文の最後のセミコロンを削除
        sql_query_with_conditions = sql_query_with_conditions.rstrip(';')
        
        # LIMIT句を追加
        sql_query_with_conditions += f" LIMIT {limit_value};"
        
        st.session_state['sql_query_with_conditions'] = sql_query_with_conditions
        logger.info(f"絞込検索後のSQL文: {sql_query_with_conditions}")

        df = execute_sql_query(sql_query_with_conditions, limit_value)
        st.session_state['df'] = df
        
        # データ件数を取得
        total_records = len(df)
        st.session_state['total_records'] = total_records
        
        if total_records <= limit_value:
            st.session_state['selected_rows'] = total_records
        else:
            st.session_state['selected_rows'] = limit_value
        
        st.experimental_rerun()
    except Exception as e:
        logger.error(f"絞込検索中にエラーが発生しました: {e}")
        st.error("絞込検索中にエラーが発生しました。")

# 選択された行数が変更された場合の処理を行う関数
def on_limit_change():
    # 選択された行数をセッションステートに反映
    st.session_state['limit'] = st.session_state['rows_selectbox_top']
    st.session_state['selected_rows'] = st.session_state['limit']
    logger.info(f"Limit changed to: {st.session_state['limit']}")
    logger.info(f"Session state before SQL re-execution: {st.session_state}")
    sql_query = load_and_log_sql(st.session_state['selected_sql_file'], st.session_state['input_fields'], st.session_state['input_fields_types'], st.session_state['limit'])
    st.session_state['sql_query'] = sql_query
    logger.info(f"Session state after SQL re-execution: {st.session_state}")

# SQL文を読み込み、ロギングを行う関数
def load_and_log_sql(sql_file_name, input_fields, input_fields_types, limit):
    sql_query = load_and_prepare_sql(sql_file_name, input_fields, input_fields_types)
    if sql_query:
        # 入力フィールドの値に基づいてSQLクエリを絞り込む
        conditions = []
        for field, value in input_fields.items():
            if value:
                conditions.append(f"{field} = '{value}'")
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            sql_query += where_clause

        limit_clause = f" LIMIT {limit}"
        if ';' in sql_query:
            sql_query_parts = sql_query.split(';')
            sql_query_parts[-2] = sql_query_parts[-2] + limit_clause
            limited_sql_query = ';'.join(sql_query_parts)
        else:
            limited_sql_query = sql_query + limit_clause

        df = execute_sql_query(limited_sql_query, limit)
        return limited_sql_query
    return None

# SQLクエリの実行とデータフレームの処理を共通化
def execute_sql_query(sql_query, limit):
    try:
        logger.info(f"実行するSQLクエリ: {sql_query}")
        conn = get_connection()
        if conn is None:
            raise OperationalError("MySQL Connection not available")
        
        df = pd.read_sql(sql_query, conn)
        st.session_state['df'] = df
        st.session_state['total_records'] = len(df)
        logger.info("SQLクエリのヘッダ行: %s", df.columns.tolist())
        logger.info("SQLクエリのデータ: %s", df.head(limit).to_dict(orient='records'))
        logger.info(f"Session state after SQL execution: {st.session_state}")
        return df
    except Exception as e:
        logger.error(f"SQLクエリの実行中にエラーが発生しました: {e}\n{traceback.format_exc()}")
        return None

# テキストを省略表示する関数
def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

# テーブルにスタイルを適用する関数
def apply_styles(df, selected_rows):
    # ヘッダ行とデータ行の文字数を制限し、省略表示にする
    df.columns = [truncate_text(col, 20) for col in df.columns]  # ヘッダ行は20文字まで
    df = df.applymap(lambda x: truncate_text(x, 35) if isinstance(x, str) else x)  # データ行は35文字まで
    
    # スタイル設定
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'
    
    styled_df = df.head(selected_rows).style.apply(highlight_header, axis=0).applymap(white_background, subset=pd.IndexSlice[:, :])
    
    return styled_df

# データを準備し、スタイルを適用する関数
def load_and_prepare_data(df, selected_rows):
    df.index = df.index + 1
    styled_df = apply_styles(df, selected_rows)
    return styled_df
