import streamlit as st
import os
import pandas as pd
from io import StringIO
from ..data.subcode_loader import apply_data_types_to_df
import concurrent.futures
import dask.dataframe as dd
import numpy as np
from functools import partial
from datetime import datetime
import pyarrow.parquet as pq
from ..streamlit.subcode_streamlit_loader import (
    on_limit_change, load_and_filter_parquet,
    load_sheet_from_spreadsheet,
    get_filtered_data_from_sheet
)

try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('utils')
except ImportError:
    # フォールバック：旧構造
    from my_logging import setup_department_logger
    LOGGER = setup_department_logger('utils')

# テキストを指定の長さに切り詰める関数
def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

# データフレームにスタイルを適用する関数
def apply_styles(df, selected_rows=100):
    # ヘッダ行とデータ行の文字数を制限し、省略表示にする
    df.columns = [truncate_text(col, 20) for col in df.columns]  # ヘッダ行は20文字まで
    
    # スタイル設定
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'
    
    styled_df = df.head(selected_rows).style.apply(highlight_header, axis=0).map(white_background, subset=pd.IndexSlice[:, :])
    
    return styled_df

# CSVデータを読み込み、スタイル付きのデータフレームを返す関数
def load_and_prepare_data(csv_data, selected_rows):
    df = pd.read_csv(StringIO(csv_data), encoding='cp932')
    df = df.head(selected_rows)  # 選択された行数だけ抽出
    styled_df = df.style.set_properties(**{'text-align': 'left'}).set_table_styles([dict(selector='th', props=[('text-align', 'center')])])
    return styled_df

# Null値を空文字列に置換する関数
def replace_null_values(val):
    if pd.isna(val) or val is None or val == 'nan' or val == 'None':
        return ''
    return val

# CSVダウンロード用にデータを処理する関数
def prepare_csv_data(df, input_fields_types, logger):
    # NaN、None、'nan'、'None'を空文字列に置換
    csv_df = df.map(replace_null_values)

    # Int64型と指定されたカラムで、空文字列を0に置換
    int64_columns = [col for col, dtype in input_fields_types.items() if dtype == 'int' and col in csv_df.columns]
    for col in int64_columns:
        csv_df[col] = csv_df[col].replace('', 0)

    # データ型を適用
    csv_df = apply_data_types_to_df(csv_df, input_fields_types, logger)

    # float64型のカラムで、残っている空文字列を0に置換
    float64_columns = [col for col, dtype in input_fields_types.items() if dtype == 'float' and col in csv_df.columns]
    for col in float64_columns:
        csv_df[col] = csv_df[col].replace('', 0)

    # すべての値を適切な文字列表現に変換
    csv_df = csv_df.map(lambda x: str(int(x)) if isinstance(x, (float, int)) and float(x).is_integer() else str(x))

    # オブジェクト型の列で '#######' を空文字列に置換
    object_columns = csv_df.select_dtypes(include=['object']).columns
    for col in object_columns:
        csv_df[col] = csv_df[col].replace('#######', '')

    return csv_df

# Parquetファイルの最終更新日時を取得する関数
def get_parquet_file_last_modified(file_path):
    if os.path.exists(file_path):
        timestamp = os.path.getmtime(file_path)
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    return "ファイルが見つかりません"

    # すべての値を適切な文字列表現に変換
    csv_df = csv_df.map(lambda x: str(int(x)) if isinstance(x, (float, int)) and float(x).is_integer() else str(x))

    # オブジェクト型の列で '#######' を空文字列に置換
    object_columns = csv_df.select_dtypes(include=['object']).columns
    for col in object_columns:
        csv_df[col] = csv_df[col].replace('#######', '')

    return csv_df

# SQLファイル名に基づいてデータを読み込む関数
def load_data(sql_file_name):
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)
    return data

# UIのセットアップを行う関数
def setup_ui(last_modified):
    col1, col2 = st.columns([10, 3])
    with col2:
        st.markdown("最終データ取得日時：" + last_modified)

# フィルター適用時のデータ取得を処理する関数
def handle_filter_submission(parquet_file_path):
    input_values = st.session_state['input_fields']
    input_fields_types = st.session_state['input_fields_types']
    options_dict = st.session_state['options_dict']
    
    df = load_and_filter_parquet(parquet_file_path, input_values, input_fields_types, options_dict)
    if df is not None and not df.empty:
        st.session_state['df'] = df
        st.session_state['total_records'] = len(df)
        st.session_state['current_page'] = 1  # ページのみリセット（表示件数は保持）
        return df
    else:
        st.error("絞込条件に合致するデータがありません。")
        return None
    
# CSVダウンロードボタンを表示する関数
def display_csv_download_button(df, input_fields_types):
    clean_df = df.dropna(how='all').reset_index(drop=True)
    now = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    csv_df = prepare_csv_data(clean_df, input_fields_types, LOGGER)
    csv_data = csv_df.to_csv(index=False).encode('cp932', errors='ignore')
    st.download_button(label="CSV DL", data=csv_data, 
                       file_name=f'{now}_export.csv', mime='text/csv')

# データ表示全体を管理する関数
def display_data(df, page_size, input_fields_types):
    LOGGER.info(f"display_data called with page_size: {page_size}")
    LOGGER.info(f"Current session state: {st.session_state}")
    total_rows = len(df)
    total_pages = (total_rows + page_size - 1) // page_size
    
    cols_pagination_top = st.columns([2, 7, 1])
    with cols_pagination_top[0]:
        start_index = (st.session_state['current_page'] - 1) * page_size + 1
        end_index = min(st.session_state['current_page'] * page_size, total_rows)
        st.write(f"{start_index:,} - {end_index:,} / {total_rows:,} 件")
    
    with cols_pagination_top[2]:
        display_csv_download_button(df, input_fields_types)
        display_row_selector()
    
    df_view = get_paginated_df(df, page_size)
    if not df_view.empty:
        display_styled_df(df_view)
        display_pagination_buttons(total_pages)
        
        # プリフェッチを追加
        next_page_data = prefetch_next_page(df, st.session_state['current_page'], page_size)
        st.session_state['next_page_data'] = next_page_data
        LOGGER.info(f"display_data: Full DataFrame shape: {df.shape}, Page size: {page_size}")
    else:
        st.warning("表示するデータがありません。")

# 表示行数選択UIを表示する関数
def display_row_selector():
    rows_options = [20, 50, 100, 200, 500, 1000, 2000, 5000]
    limit = st.selectbox("", rows_options, 
                         index=rows_options.index(st.session_state.get('limit', 100)), 
                         key="rows_selectbox", on_change=on_limit_change)
    st.session_state['limit'] = limit

# ページネーション用にデータフレームの一部を取得する関数
def get_paginated_df(df, page_size):
    current_page = st.session_state['current_page']
    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    LOGGER.info(f"get_paginated_df: page_size={page_size}, start_index={start_index}, end_index={end_index}, df.shape={df.shape}")
    result = df.iloc[start_index:end_index]
    LOGGER.info(f"get_paginated_df result shape: {result.shape}")
    LOGGER.info(f"get_paginated_df: start_index={start_index}, end_index={end_index}")
    LOGGER.info(f"get_paginated_df: Returning DataFrame shape: {df.iloc[start_index:end_index].shape}")
    return result

# スタイル適用済みのデータフレームを表示する関数
def display_styled_df(df_view):
    LOGGER.info(f"display_styled_df: DataFrame shape = {df_view.shape}")
    selected_rows = st.session_state.get('limit', 100)
    current_page = st.session_state['current_page']
    start_index = (current_page - 1) * selected_rows + 1
    
    df_view_reset = df_view.reset_index(drop=True)
    df_view_reset.index = range(start_index, start_index + len(df_view_reset))
    df_view_reset.index = df_view_reset.index.map(str)
    
    styled_df = apply_styles(df_view_reset, selected_rows)
    st.dataframe(styled_df, use_container_width=True, height=700)

# ページネーションボタンを表示する関数
def display_pagination_buttons(total_pages):
    cols_pagination_buttons = st.columns([7, 1, 1])
    with cols_pagination_buttons[1]:
        if st.session_state['current_page'] > 1:
            if st.button("◀ ◀ 前へ", key="prev_button"):
                st.session_state['current_page'] -= 1
    with cols_pagination_buttons[2]:
        if st.session_state['current_page'] < total_pages:
            if st.button("次へ ▶ ▶", key="next_button"):
                st.session_state['current_page'] += 1

# SQLファイル名に基づいてデータを読み込み、初期化する関数
def load_and_initialize_data(sql_file_name, num_rows=None):
    try:
        # 設定ファイルからパスを取得
        try:
            from src.core.config.settings import AppConfig
            app_config = AppConfig.from_config_file('config/settings.ini')
            csv_base_path = os.path.normpath(app_config.paths.csv_base_path)
        except ImportError:
            # フォールバック：旧構造
            import configparser
            config = configparser.ConfigParser()
            config.read('config/settings.ini', encoding='utf-8')
            csv_base_path = os.path.normpath(config['Paths']['csv_base_path'])
        
        # 常に設定ファイルのパスを使用（Windows UNCパス対応）
        parquet_file_path = os.path.join(csv_base_path, f"{sql_file_name}.parquet")
        
        LOGGER.info(f"Parquetファイルパス (utils.py): {parquet_file_path}")
        if os.path.exists(parquet_file_path):
            df, total_rows = load_parquet_file(parquet_file_path, num_rows)
            if df is not None:
                LOGGER.info(f"Loaded initial DataFrame shape: {df.shape}, Total rows: {total_rows}")
                st.session_state['df'] = df
                st.session_state['total_records'] = total_rows
                
                # データのプリフェッチ
                if num_rows is not None and num_rows < total_rows:
                    next_page_start = num_rows
                    next_page_end = min(num_rows * 2, total_rows)
                    st.session_state['next_page_data'] = load_parquet_file(parquet_file_path, next_page_end - next_page_start)[0]
                
                return df
            else:
                LOGGER.error("Failed to load DataFrame from Parquet file")
                return None
        else:
            LOGGER.error(f"Parquet file not found: {parquet_file_path}")
            return None
    except Exception as e:
        LOGGER.error(f"Error in load_and_initialize_data: {str(e)}")
        return None

# ページネーションUIを作成する関数
def create_pagination_ui(df, page_size):
    total_rows = len(df)
    total_pages = (total_rows + page_size - 1) // page_size
    current_page = st.session_state['current_page']

    start_index = (current_page - 1) * page_size + 1
    end_index = min(current_page * page_size, total_rows)
    
    cols_pagination_top = st.columns([2, 7, 1])
    with cols_pagination_top[0]:
        st.write(f"{start_index:,} - {end_index:,} / {total_rows:,} 件")
    
    return cols_pagination_top, total_pages

# キャッシュを使用してParquetファイルを読み込む
@st.cache_data(ttl=1800, show_spinner=False)  # 30分 = 1800秒
def load_parquet_file(file_path, num_rows=None):
    try:
        table = pq.read_table(file_path)
        total_rows = table.num_rows
        if num_rows is not None and num_rows < total_rows:
            df = table.slice(0, num_rows).to_pandas()
        else:
            df = table.to_pandas()
        df = optimize_dtypes(df)
        LOGGER.info(f"load_parquet_file: Loaded DataFrame shape: {df.shape}, Total rows in file: {total_rows}")
        return df, total_rows
    except Exception as e:
        LOGGER.error(f"Error in load_parquet_file: {str(e)}")
        return None, 0

@st.cache_data
def filter_dataframe(df, input_values, input_fields_types, options_dict):
    # フィルタリングのロジック
    filtered_df = df.copy()
    for field, value in input_values.items():
        if value:
            if input_fields_types[field] == 'text':
                filtered_df = filtered_df[filtered_df[field].str.contains(value, na=False, case=False)]
            elif input_fields_types[field] in ['int', 'float']:
                try:
                    filtered_df = filtered_df[filtered_df[field] == float(value)]
                except ValueError:
                    st.warning(f"{field}の値が正しくありません。数値を入力してください。")
            elif input_fields_types[field] == 'date':
                filtered_df = filtered_df[filtered_df[field] == value]
            elif input_fields_types[field] == 'select' and field in options_dict:
                if value != "すべて":
                    filtered_df = filtered_df[filtered_df[field] == value]
    return filtered_df

# データ型を最適化する新しい関数
def optimize_dtypes(df):
    for col in df.columns:
        # 列の一意な値を取得
        unique_values = df[col].dropna().unique()

        if df[col].dtype == 'object':
            # 日付形式の判定
            if len(unique_values) > 0 and all(isinstance(val, str) and len(val) == 10 and val[4] == '-' and val[7] == '-' for val in unique_values):
                # YYY-MM-DD 形式の日付
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')
            elif len(unique_values) > 0 and all(isinstance(val, str) and len(val) == 10 and val[4] == '/' and val[7] == '/' for val in unique_values):
                # YYYY/MM/DD 形式の日付
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y/%m/%d')
            elif len(unique_values) > 0 and all(isinstance(val, str) and len(val) == 19 and val[4] == '-' and val[7] == '-' and val[10] == ' ' for val in unique_values):
                # YYYY-MM-DD HH:MM:SS 形式の日時
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
            else:
                # 数値への変換を試みる
                try:
                    df[col] = pd.to_numeric(df[col], errors='raise')
                except:
                    # 数値に変換できない場合は文字列のまま
                    pass
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')

    LOGGER.info(f"Optimized DataFrame dtypes: {df.dtypes}")
    return df

# 並列フィルタリングのための新しい関数
def parallel_filter(df_chunk, conditions):
    for field, value in conditions.items():
        if value:
            if df_chunk[field].dtype == 'object':
                df_chunk = df_chunk[df_chunk[field].str.contains(value, na=False, case=False)]
            elif df_chunk[field].dtype in ['int64', 'float64']:
                df_chunk = df_chunk[df_chunk[field] == float(value)]
            elif df_chunk[field].dtype == 'datetime64[ns]':
                df_chunk = df_chunk[df_chunk[field] == pd.to_datetime(value)]
    return df_chunk

# 並列データフレームフィルタリングの新しい関数
@st.cache_data
def parallel_dataframe_filter(df, conditions, num_workers=4):
    chunks = np.array_split(df, num_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        filtered_chunks = list(executor.map(partial(parallel_filter, conditions=conditions), chunks))
    return pd.concat(filtered_chunks)

# 次のページのデータをプリフェッチする新しい関数
def prefetch_next_page(df, current_page, page_size):
    next_page = current_page + 1
    start_index = (next_page - 1) * page_size
    end_index = start_index + page_size
    return df.iloc[start_index:end_index]