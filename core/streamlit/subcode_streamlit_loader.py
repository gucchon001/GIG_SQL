import os
import pandas as pd
# Streamlitインポートを条件付きに変更（メモリエラー回避）
try:
    import streamlit as st
    ST_AVAILABLE = True
except (MemoryError, ImportError) as e:
    ST_AVAILABLE = False
    print(f"Streamlit読み込みスキップ: {e}")
    
    # st.cache_resourceの代替クラス
    class MockCache:
        def __call__(self, func):
            return func
    
    # Streamlitモジュールのモック
    class MockStreamlit:
        cache_resource = MockCache()
    
    st = MockStreamlit()
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser
from datetime import datetime
try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('streamlit')
except ImportError:
    # フォールバック：旧構造
    from ..config.my_logging import setup_department_logger
    LOGGER = setup_department_logger('streamlit', app_type='streamlit')
import traceback
import numpy as np

# CSSファイルを読み込む関数
def load_css(file_name):
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            if ST_AVAILABLE:
                st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
        LOGGER.info(f"CSSファイル '{file_name}' を正常に読み込みました。")
    except FileNotFoundError:
        LOGGER.error(f"CSSファイル '{file_name}' が見つかりませんでした。")
        if ST_AVAILABLE:
            st.error(f"CSSファイル '{file_name}' が見つかりませんでした。")

# CSSファイルを読み込む
load_css("styles.css")

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
                LOGGER.info(f"列 '{column}' の日付フォーマットを '{data_type}' に変換しました。")
            except pd.errors.OutOfBoundsDatetime as e:
                LOGGER.error(f"OutOfBoundsDatetimeエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # エラー発生時にはNaTに変換
            except Exception as e:
                LOGGER.error(f"日付フォーマット中にエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # その他のエラー発生時にもNaTに変換
    return df

# Google Sheets APIへの認証処理を共通化
def get_google_sheets_client():
    config_file = 'config/settings.ini'
    # 環境変数からJSON認証ファイルパスを取得
    from dotenv import load_dotenv
    import os
    secrets_path = os.path.join(os.getcwd(), 'config', 'secrets.env')
    load_dotenv(secrets_path)
    
    json_keyfile_path = os.getenv('JSON_KEYFILE_PATH')
    if not json_keyfile_path:
        # フォールバック：設定ファイルから読み込み
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')
        json_keyfile_path = config.get('Credentials', {}).get('json_keyfile_path', '')
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)
    LOGGER.info("Google Sheets APIへの認証に成功しました。")
    return client

# SQLファイルリストをスプレッドシートから読み込む関数
@st.cache_resource(ttl=300)  # 5分に短縮してキャッシュ問題を軽減
def load_sql_list_from_spreadsheet():
    """
    Google スプレッドシートからSQLファイル一覧を読み込む
    
    Returns:
        dict: {CSVファイル呼称: SQLファイル名} の辞書
    """
    config = configparser.ConfigParser()
    config.read('config/settings.ini', encoding='utf-8')

    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    sheet_name = config['Spreadsheet']['eachdata_sheet']
    
    client = get_google_sheets_client()

    try:
        LOGGER.debug(f"スプレッドシート '{spreadsheet_id}' に接続中...")
        spreadsheet = client.open_by_key(spreadsheet_id)
        LOGGER.debug(f"シート '{sheet_name}' を開いています...")
        sheet = spreadsheet.worksheet(sheet_name)
        LOGGER.debug("データを取得中...")
        data = sheet.get_all_values()
        LOGGER.info(f"スプレッドシート '{spreadsheet_id}' のシート '{sheet_name}' からデータを正常に取得しました。")
    except gspread.exceptions.WorksheetNotFound as e:
        LOGGER.error(f"シート '{sheet_name}' がスプレッドシート '{spreadsheet_id}' に存在しません: {e}")
        if ST_AVAILABLE:
            st.error(f"シート '{sheet_name}' がスプレッドシート '{spreadsheet_id}' に存在しません。")
        return {}
    except gspread.exceptions.APIError as e:
        error_message = str(e)
        if "429" in error_message and "Quota exceeded" in error_message:
            LOGGER.warning(f"Google Sheets API制限に達しました。5分後に再試行してください: {e}")
            if ST_AVAILABLE:
                st.warning("⚠️ Google Sheets APIの制限に達しました。5-10分後に再試行してください。")
                st.info("💡 **対処方法**:\n- ページを再読み込みする前に5分待つ\n- 他のタブでスプレッドシートを開いている場合は閉じる")
        else:
            LOGGER.error(f"Google Sheets API エラーが発生しました: {e}")
            if ST_AVAILABLE:
                st.error(f"Google Sheets API エラーが発生しました: {e}")
        return {}
    except Exception as e:
        LOGGER.error(f"スプレッドシートからデータを取得中にエラーが発生しました: {e}")
        LOGGER.debug(f"エラーの詳細: {type(e).__name__}: {str(e)}")
        if ST_AVAILABLE:
            st.error(f"スプレッドシートからデータを取得中にエラーが発生しました: {e}")
        return {}

    if not data:
        LOGGER.warning(f"スプレッドシート '{spreadsheet_id}' のシート '{sheet_name}' は空です。")
        if ST_AVAILABLE:
            st.warning(f"スプレッドシート '{spreadsheet_id}' のシート '{sheet_name}' は空です。")
        return {}

    header = data[0]
    try:
        target_index = header.index('個別リスト')
        sql_file_name_index = header.index('sqlファイル名')
        csv_file_name_index = header.index('CSVファイル呼称')
    except ValueError as e:
        LOGGER.error(f"必要なヘッダがスプレッドシートに存在しません: {e}")
        if ST_AVAILABLE:
            st.error(f"必要なヘッダがスプレッドシートに存在しません: {e}")
        return {}

    records = {
        row[csv_file_name_index]: row[sql_file_name_index]
        for row in data[1:]
        if len(row) > max(target_index, sql_file_name_index, csv_file_name_index) and row[target_index].strip().lower() == 'true'
    }

    LOGGER.info(f"フィルタリングされたSQLファイルリストを取得しました。件数: {len(records)}")
    return records

# 指定されたプルダウン選択肢に対応するSQLファイル名から.sql拡張子を除去する関数
def get_sql_file_name(selected_option):
    records = load_sql_list_from_spreadsheet()
    sql_file_name = records.get(selected_option)

    if sql_file_name:
        LOGGER.info(f"選択されたオプション '{selected_option}' に対応するSQLファイル名: {sql_file_name}")
        return sql_file_name.replace('.sql', '')
    else:
        LOGGER.warning(f"選択されたオプション '{selected_option}' に対応するSQLファイルが見つかりません。")
        return None

# スプレッドシートからデータを読み込む処理を共通化（キャッシュ付き）
@st.cache_data(ttl=600, show_spinner=False)  # 10分間キャッシュ
def load_sheet_data_cached(sheet_name, spreadsheet_id):
    """スプレッドシートデータをキャッシュ付きで取得"""
    client = get_google_sheets_client()
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        data = sheet.get_all_values()
        LOGGER.info(f"スプレッドシート '{spreadsheet_id}' のシート '{sheet_name}' を正常にロードしました。")
        return data
    except gspread.exceptions.WorksheetNotFound:
        LOGGER.error(f"シート '{sheet_name}' がスプレッドシート '{spreadsheet_id}' に存在しません。")
        if ST_AVAILABLE:
            st.error(f"シート '{sheet_name}' がスプレッドシート '{spreadsheet_id}' に存在しません。")
        return None
    except gspread.exceptions.APIError as e:
        error_message = str(e)
        if "429" in error_message and "Quota exceeded" in error_message:
            LOGGER.warning(f"Google Sheets API制限に達しました: {e}")
            if ST_AVAILABLE:
                st.warning("⏰ Google Sheets APIの利用制限に達しました。しばらく待ってから再度お試しください。")
        else:
            LOGGER.error(f"Google Sheets APIエラー: {e}")
            if ST_AVAILABLE:
                st.error(f"Google Sheets APIエラー: {e}")
        return None

def load_sheet_from_spreadsheet(sheet_name):
    config_file = 'config/settings.ini'
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    
    # キャッシュ付きデータ取得
    data = load_sheet_data_cached(sheet_name, spreadsheet_id)
    if data is None:
        return None
    
    # データをシートオブジェクト風に変換
    class MockSheet:
        def __init__(self, data):
            self.data = data
        
        def get_all_values(self):
            return self.data
        
        def get_all_records(self):
            """データをレコード形式で返す"""
            if not self.data or len(self.data) < 2:
                return []
            
            headers = self.data[0]
            records = []
            for row in self.data[1:]:
                # 行の長さをヘッダーに合わせる
                padded_row = row + [''] * (len(headers) - len(row))
                record = dict(zip(headers, padded_row))
                records.append(record)
            return records
        
        def row_values(self, row_num):
            if row_num <= len(self.data):
                return self.data[row_num - 1]
            return []
    
    return MockSheet(data)

# 選択シートの条件を取得する関数
def get_filtered_data_from_sheet(sheet):
    try:
        header_row = sheet.row_values(1)
        cleaned_header_row = [h.strip().lower() for h in header_row]

        if len(cleaned_header_row) != len(set(cleaned_header_row)):
            raise ValueError("ヘッダ行に重複する項目があります。")

        records = sheet.get_all_records()
        LOGGER.info(f"シートから取得した全レコード数: {len(records)}")

        filtered_data = []
        for record in records:
            if record.get('絞込', '').strip().upper() == 'TRUE':
                data = {
                    'db_item': record.get('DB項目', '').strip(),
                    'table_name': record.get('TABLE_NAME', '').strip(),
                    'data_item': record.get('DATA_ITEM', '').strip(),
                    'input_type': record.get('入力方式', '').strip(),
                    'options': [option.split(' ') for option in record.get('選択項目', '').split('\n') if option.strip()]
                }
                filtered_data.append(data)

        LOGGER.info(f"絞込条件に一致するレコード数: {len(filtered_data)}")
        return filtered_data
    except Exception as e:
        LOGGER.error(f"選択シートから条件を取得中にエラーが発生しました: {e}")
        st.error(f"選択シートから条件を取得中にエラーが発生しました: {e}")
        return []

# 動的な入力フィールドを作成する関数内のチェックボックス部分
def create_dynamic_input_fields(data):
    input_fields = {}
    input_fields_types = {}
    options_dict = {}

    if not data:
        st.error("指定されている項目がありません")
        LOGGER.warning("指定されている項目がありません。")
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
                LOGGER.debug(f"FA入力フィールドを作成しました: {item['db_item']}")

            elif item['input_type'] == 'プルダウン':
                options = ['-'] + list(set([option[1] for option in item['options'] if len(option) > 1]))
                input_fields[item['db_item']] = st.selectbox(label_text, options, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'プルダウン'
                options_dict[item['db_item']] = item['options']
                LOGGER.debug(f"プルダウン入力フィールドを作成しました: {item['db_item']} with options {options}")

            elif item['input_type'] == 'ラジオボタン':
                options = [option[1] for option in item['options'] if len(option) > 1]
                radio_key = f"radio_{item['db_item']}"
                clear_key = f"clear_radio_{item['db_item']}"

                if st.session_state.get(clear_key, False):
                    st.session_state[radio_key] = None
                    st.session_state[clear_key] = False
                    LOGGER.debug(f"ラジオボタンのセッションステートをリセットしました: {item['db_item']}")

                st.text(label_text)  # フリーワードのタイトルと同じサイズと文字タイプに統一
                if options:
                    radio_index = st.radio("", range(len(options)), format_func=lambda i: options[i], index=st.session_state.get(radio_key, 0), key=radio_key)
                    input_fields[item['db_item']] = options[radio_index] if radio_index is not None else None
                    input_fields_types[item['db_item']] = 'ラジオボタン'
                    options_dict[item['db_item']] = item['options']
                    LOGGER.debug(f"ラジオボタン入力フィールドを作成しました: {item['db_item']} with options {options}")
                else:
                    LOGGER.warning(f"ラジオボタン入力フィールド '{item['db_item']}' にオプションがありません。")

            elif item['input_type'] == 'チェックボックス':
                st.text(label_text)  # フリーワードのタイトルと同じサイズと文字タイプに統一
                checkbox_values = {}
                for option in item['options']:
                    if len(option) > 1:
                        checkbox_values[option[1]] = st.checkbox(option[1], key=f"checkbox_{item['db_item']}_{option[0]}")
                input_fields[item['db_item']] = checkbox_values
                input_fields_types[item['db_item']] = 'チェックボックス'
                options_dict[item['db_item']] = item['options']  # オプションを保存
                LOGGER.debug(f"チェックボックス入力フィールドを作成しました: {item['db_item']} with options {options_dict[item['db_item']]}")

            elif item['input_type'] == 'Date':
                start_date = st.date_input(f"{label_text} 開始日", key=f"start_date_{item['db_item']}", value=None)
                end_date = st.date_input(f"{label_text} 終了日", key=f"end_date_{item['db_item']}", value=None)
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'date'
                LOGGER.debug(f"Date入力フィールドを作成しました: {item['db_item']} with start_date={start_date}, end_date={end_date}")

            elif item['input_type'] == 'Datetime':
                start_date = st.date_input(f"{label_text} 開始日", key=f"start_datetime_{item['db_item']}", value=None)
                end_date = st.date_input(f"{label_text} 終了日", key=f"end_datetime_{item['db_item']}", value=None)
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'datetime'
                LOGGER.debug(f"Datetime入力フィールドを作成しました: {item['db_item']} with start_date={start_date}, end_date={end_date}")

    st.session_state['input_fields'] = input_fields
    st.session_state['input_fields_types'] = input_fields_types
    st.session_state['options_dict'] = options_dict

    return input_fields, input_fields_types, options_dict

# セッションステートを初期化する関数
def initialize_session_state():
    if 'selected_sql_file' not in st.session_state:
        st.session_state['selected_sql_file'] = None
        LOGGER.debug("セッションステート 'selected_sql_file' を初期化しました。")
    if 'df' not in st.session_state:
        st.session_state['df'] = None
        LOGGER.debug("セッションステート 'df' を初期化しました。")
    if 'limit' not in st.session_state:
        st.session_state['limit'] = 20  # デフォルト値を20に変更
        LOGGER.debug("セッションステート 'limit' を初期化しました。")
    if 'total_records' not in st.session_state:
        st.session_state['total_records'] = 0
        LOGGER.debug("セッションステート 'total_records' を初期化しました。")
    if 'selected_rows' not in st.session_state:
        st.session_state['selected_rows'] = 20  # デフォルト値を20に変更
        LOGGER.debug("セッションステート 'selected_rows' を初期化しました。")
    if 'input_fields' not in st.session_state:
        st.session_state['input_fields'] = {}
        LOGGER.debug("セッションステート 'input_fields' を初期化しました。")
    if 'input_fields_types' not in st.session_state:
        st.session_state['input_fields_types'] = {}
        LOGGER.debug("セッションステート 'input_fields_types' を初期化しました。")
    if 'options_dict' not in st.session_state:
        st.session_state['options_dict'] = {}
        LOGGER.debug("セッションステート 'options_dict' を初期化しました。")
    if 'current_page' not in st.session_state:
        st.session_state['current_page'] = 1
        LOGGER.debug("セッションステート 'current_page' を初期化しました。")
    if 'last_selected_table' not in st.session_state:
        st.session_state['last_selected_table'] = None
        LOGGER.debug("セッションステート 'last_selected_table' を初期化しました。")

# テキストを省略表示する関数
def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

def load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types, options_dict):
    try:
        df = pd.read_parquet(parquet_file_path)
        LOGGER.info(f"Parquetファイル '{parquet_file_path}' を正常に読み込みました。")
        
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
        LOGGER.info("NaN値を適切に置換しました。")

        # フィルタ条件の適用
        for field, value in input_fields.items():
            LOGGER.info(f"フィルタ条件 - {field}: {value}")  # フィルター条件をログに記録

            if input_fields_types[field] == 'FA' and value:
                if df[field].dtype == 'Int64':
                    try:
                        df = df[df[field] == int(value)]
                        LOGGER.debug(f"フィルタリング - {field} == {int(value)}")
                    except ValueError:
                        LOGGER.warning(f"無効な整数値 '{value}' が入力されました。フィルタリングをスキップします。")
                else:
                    df = df[df[field].astype(str).str.contains(value, na=False)]
                    LOGGER.debug(f"フィルタリング - {field} に '{value}' を含む")

            elif input_fields_types[field] == 'プルダウン' and value != '-':
                df = df[df[field] == value]
                LOGGER.debug(f"フィルタリング - {field} == '{value}'")

            elif input_fields_types[field] == 'ラジオボタン' and value:
                df = df[df[field] == value]
                LOGGER.debug(f"フィルタリング - {field} == '{value}'")

            elif input_fields_types[field] == 'チェックボックス':
                # 元の挙動に戻す：選択ラベルでそのまま比較
                selected_labels = [label for label, selected in value.items() if selected]
                if selected_labels:
                    df[field] = df[field].astype(str)
                    df = df[df[field].isin(selected_labels)]
                    LOGGER.debug(f"フィルタリング - {field} に選択されたラベル {selected_labels} が含まれる")

            elif input_fields_types[field] in ['date', 'datetime']:
                start_date = value.get('start_date')
                end_date = value.get('end_date')
                
                df[field] = pd.to_datetime(df[field], errors='coerce')
                LOGGER.debug(f"フィルタリング - {field} をdatetime型に変換しました。")
                
                if start_date and end_date:
                    start_datetime = pd.to_datetime(start_date).floor('D')
                    end_datetime = pd.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)
                    df = df[(df[field] >= start_datetime) & (df[field] <= end_datetime)]
                    LOGGER.debug(f"フィルタリング - {field} を {start_datetime} から {end_datetime} までに制限しました。")
                elif start_date:
                    start_datetime = pd.to_datetime(start_date).floor('D')
                    df = df[df[field] >= start_datetime]
                    LOGGER.debug(f"フィルタリング - {field} を {start_datetime} 以降に制限しました。")
                elif end_date:
                    end_datetime = pd.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)
                    df = df[df[field] <= end_datetime]
                    LOGGER.debug(f"フィルタリング - {field} を {end_datetime} 以前に制限しました。")
                
        if df.empty:
            LOGGER.warning("フィルタリング後のDataFrameが空です。")
            return pd.DataFrame()
        else:
            LOGGER.info("フィルタリング後のDataFrameが取得されました。")
            return df.sort_index(ascending=False)  # フィルタリング後に降順に並べ替え
    except Exception as e:
        LOGGER.error(f"データフィルタリング中にエラーが発生しました: {e}")
        LOGGER.debug(traceback.format_exc())
        return None

# Parquetファイルの選択時の処理
def on_sql_file_change(sql_files_dict):
    try:
        selected_display_name = st.session_state.get('selected_display_name')
        if not selected_display_name:
            LOGGER.warning("選択されたSQLファイル名がセッションステートに存在しません。")
            return

        st.session_state['selected_sql_file'] = sql_files_dict.get(selected_display_name)
        sql_file_name = get_sql_file_name(selected_display_name)
        # 設定ファイルからパスを取得
        try:
            from src.core.config.settings import AppConfig
            app_config = AppConfig.from_config_file('config/settings.ini')
            csv_base_path = app_config.paths.csv_base_path
        except ImportError:
            # フォールバック：旧構造
            import configparser
            config = configparser.ConfigParser()
            config.read('config/settings.ini', encoding='utf-8')
            csv_base_path = config['Paths']['csv_base_path']
        
        parquet_file_path = os.path.join(csv_base_path, f"{sql_file_name}.parquet")

        if os.path.exists(parquet_file_path):
            df = pd.read_parquet(parquet_file_path)
            df = df.sort_index(ascending=False)  # インデックスの降順で並べ替え
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
            LOGGER.info(f"Parquetファイル '{parquet_file_path}' をロードしました。")
        else:
            st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
            LOGGER.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
    except Exception as e:
        LOGGER.error(f"on_sql_file_change関数内でエラーが発生しました: {e}")
        st.error(f"Error in on_sql_file_change: {e}")

def calculate_offset(page_number, page_size):
    return (page_number - 1) * page_size

def on_limit_change():
    st.session_state['limit'] = st.session_state.get('rows_selectbox', 20)
    st.session_state['selected_rows'] = st.session_state['limit']
    df = st.session_state.get('df')
    if df is not None:
        page_number = st.session_state.get('current_page', 1)  # 現在のページ番号を取得（デフォルトは1）
        st.session_state['df_view'] = load_and_prepare_data(df, page_number, st.session_state['selected_rows'])
        LOGGER.info(f"表示行数を {st.session_state['selected_rows']} に変更しました。")

# データフレームを制限して準備する関数
def load_and_prepare_data(df, page_number, page_size):
    if df is None:
        LOGGER.info("DataFrameが存在しません。")
        return pd.DataFrame()  # 空のDataFrameを返す
    offset = calculate_offset(page_number, page_size)
    limited_df = df.iloc[offset:offset + page_size]  # インデックスの降順は既にソート済みと仮定
    LOGGER.info(f"ページ {page_number} のデータをロードしました。")
    return limited_df

# 検索ボタンがクリックされた場合の処理
def on_search_click():
    input_fields = st.session_state.get('input_fields', {})
    input_fields_types = st.session_state.get('input_fields_types', {})
    selected_display_name = st.session_state.get('selected_display_name')
    if not selected_display_name:
        st.error("SQLファイルが選択されていません。")
        LOGGER.warning("SQLファイルが選択されていません。")
        return

    sql_file_name = get_sql_file_name(selected_display_name)
    if not sql_file_name:
        st.error(f"選択されたオプション '{selected_display_name}' に対応するSQLファイルが見つかりません。")
        LOGGER.error(f"選択されたオプション '{selected_display_name}' に対応するSQLファイルが見つかりません。")
        return

    # 設定ファイルからパスを取得
    try:
        from src.core.config.settings import AppConfig
        app_config = AppConfig.from_config_file('config/settings.ini')
        csv_base_path = app_config.paths.csv_base_path
    except ImportError:
        # フォールバック：旧構造
        import configparser
        config = configparser.ConfigParser()
        config.read('config/settings.ini', encoding='utf-8')
        csv_base_path = config['Paths']['csv_base_path']
    
    parquet_file_path = os.path.join(csv_base_path, f"{sql_file_name}.parquet")

    if os.path.exists(parquet_file_path):
        df = load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types, st.session_state.get('options_dict', {}))
        if df is None:
            st.error("データの読み込みまたはフィルタリングに失敗しました。")
            LOGGER.error("データの読み込みまたはフィルタリングに失敗しました。")
        elif df.empty:
            st.error("該当の検索結果はありませんでした。")
            LOGGER.info("該当の検索結果はありませんでした。")
        else:
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
            st.session_state['current_page'] = 1  # ページをリセット
            st.session_state['df_view'] = load_and_prepare_data(df, 1, st.session_state['selected_rows'])
            LOGGER.info(f"検索結果をロードしました。レコード数: {len(df)}")
    else:
        st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
        LOGGER.error(f"Parquetファイルが見つかりません: {parquet_file_path}")

def get_parquet_file_last_modified(parquet_file_path):
    if os.path.exists(parquet_file_path):
        last_modified_timestamp = os.path.getmtime(parquet_file_path)
        last_modified_datetime = datetime.fromtimestamp(last_modified_timestamp)
        last_modified_str = last_modified_datetime.strftime("%Y-%m-%d %H:%M:%S")
        LOGGER.info(f"Parquetファイル '{parquet_file_path}' の最終更新日時: {last_modified_str}")
        return last_modified_str
    else:
        LOGGER.warning(f"Parquetファイル '{parquet_file_path}' が存在しません。")
        return None

# Streamlitアプリケーションのメイン部分
def main():
    st.title("SQLデータビューア")

    initialize_session_state()

    # SQLファイルの選択
    sql_files_dict = load_sql_list_from_spreadsheet()
    if not sql_files_dict:
        st.warning("実行対象のSQLファイルが見つかりません。")
        return

    selected_display_names = list(sql_files_dict.keys())
    selected_display_name = st.selectbox("SQLファイルを選択してください", [""] + selected_display_names, key='selected_display_name')

    if selected_display_name:
        on_sql_file_change(sql_files_dict)

    # 選択されたSQLファイルに基づいて条件入力フォームを表示
    if st.session_state.get('selected_sql_file') and st.session_state.get('df') is not None:
        sheet_name = st.session_state.get('last_selected_table')
        if sheet_name:
            sheet = load_sheet_from_spreadsheet(sheet_name)
            if sheet:
                filtered_data = get_filtered_data_from_sheet(sheet)
                input_fields, input_fields_types, options_dict = create_dynamic_input_fields(filtered_data)
                st.session_state['input_fields'] = input_fields
                st.session_state['input_fields_types'] = input_fields_types
                st.session_state['options_dict'] = options_dict

                if st.button("検索", on_click=on_search_click):
                    LOGGER.info("検索ボタンがクリックされました。")

        # データ表示セクション
        if st.session_state.get('df_view') is not None:
            df_view = st.session_state['df_view']
            total_records = st.session_state.get('total_records', 0)

            st.write(f"総レコード数: {total_records}")
            st.dataframe(df_view)

            # ページネーション
            if total_records > st.session_state['selected_rows']:
                num_pages = (total_records + st.session_state['selected_rows'] - 1) // st.session_state['selected_rows']
                current_page = st.session_state.get('current_page', 1)
                col1, col2, col3 = st.columns(3)

                with col2:
                    page = st.number_input("ページ番号", min_value=1, max_value=num_pages, value=current_page, step=1, key='current_page_input')
                    if st.button("移動"):
                        st.session_state['current_page'] = page
                        st.session_state['df_view'] = load_and_prepare_data(st.session_state['df'], page, st.session_state['selected_rows'])
                        LOGGER.info(f"ページ {page} に移動しました。")

    # サイドバーに設定
    with st.sidebar:
        st.header("設定")
        rows_options = [10, 20, 50, 100]
        selected_rows = st.selectbox("表示する行数", rows_options, index=rows_options.index(st.session_state.get('selected_rows', 20)), key='rows_selectbox', on_change=on_limit_change)

if __name__ == "__main__":
    main()
