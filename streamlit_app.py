import streamlit as st

# Streamlitのアプリケーション設定
st.set_page_config(page_title="塾ステ CSVダウンロードツール ストミンくん β版", page_icon=":bar_chart:", layout="wide")

import os
import pandas as pd
from my_logging import setup_department_logger
from subcode_streamlit_loader import (
    load_sql_list_from_spreadsheet, create_dynamic_input_fields,
    initialize_session_state, on_limit_change, load_and_filter_parquet,
    load_and_prepare_data, get_sql_file_name, load_sheet_from_spreadsheet,
    get_filtered_data_from_sheet, on_sql_file_change, apply_styles, calculate_offset,get_parquet_file_last_modified
)
from datetime import datetime

# ロガーの設定
LOGGER = setup_department_logger('main')

# セッションステートを初期化
initialize_session_state()

# 外部CSSファイルの読み込み
with open('styles.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# サイドバーのタイトルを小さくするためのCSSスタイル
sidebar_header = """
<style>
.sidebar .markdown-text-container h3 {
    font-size: 12px;
    margin: 0;
    padding: 0;
    line-height: 1.2;
    text-align: left;
}
</style>
<div style="display: flex; align-items: flex-start;">
    <h3>塾ステ CSVダウンロードツール<br>ストミンくん β版</h3>
</div>
"""
st.sidebar.markdown(sidebar_header, unsafe_allow_html=True)

# サイドバーに再読み込みボタンを追加
if st.sidebar.button("リスト再読み込み"):
    load_sql_list_from_spreadsheet.clear()  # キャッシュをクリア
    st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()
    st.experimental_rerun()

# サイドバーにSQLファイルリストを表示
if 'sql_files_dict' not in st.session_state:
    st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()

sql_files_dict = st.session_state['sql_files_dict']
sql_file_display_names = list(sql_files_dict.keys())

# セッションステートに選択されたファイルを設定
if "selected_display_name" not in st.session_state:
    st.session_state.selected_display_name = sql_file_display_names[0]  # 一番上のファイルをデフォルトで選択
    on_sql_file_change(sql_files_dict)  # 初期選択時のフィルタリングを実行

# Parquetファイル選択のラジオボタン
selected_display_name = st.sidebar.radio("テーブル選択", sql_file_display_names, key="selected_display_name", on_change=lambda: on_sql_file_change(sql_files_dict))

# 他のテーブルを選択した場合にページネーションをリセット
if "selected_display_name" not in st.session_state or st.session_state.selected_display_name != selected_display_name:
    st.session_state.selected_display_name = selected_display_name
    st.session_state.current_page = 1
    on_sql_file_change(sql_files_dict)

# 絞込検索とテーブル表示を初期化
sql_file_name = get_sql_file_name(selected_display_name)
sheet = load_sheet_from_spreadsheet(sql_file_name)
data = get_filtered_data_from_sheet(sheet)

parquet_file_path = f"data_parquet/{sql_file_name}.parquet"
LOGGER.info(f"Parquet file path: {parquet_file_path}")
last_modified = get_parquet_file_last_modified(parquet_file_path)

if data:
    col1, col2 = st.columns([10, 3])
    with col2:
        st.markdown("最終データ取得日時：" + last_modified)
    
    with st.expander("絞込検索"):
        with st.form(key='filter_form'):
            input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
            for field in input_fields:
                if f"input_{field}" not in st.session_state:
                    st.session_state[f"input_{field}"] = input_fields[field]

            st.session_state['input_fields'] = input_fields
            st.session_state['input_fields_types'] = input_fields_types
            st.session_state['options_dict'] = options_dict

            cols = st.columns([9, 1])  # カラムを追加してボタンを右寄せ
            with cols[0]:
                st.empty()  # 空のウィジェットでスペースを確保
            with cols[1]:
                submit_button = st.form_submit_button(label='絞込')

    if submit_button:
        input_values = st.session_state['input_fields']
        input_fields_types = st.session_state['input_fields_types']
        options_dict = st.session_state['options_dict']
        parquet_file_path = f"data_parquet/{sql_file_name}.parquet"

        if os.path.exists(parquet_file_path):
            df = load_and_filter_parquet(parquet_file_path, input_values, input_fields_types, options_dict)
            if df is not None:
                if df.empty:
                    st.error("絞込条件に合致するデータがありません。")
                else:
                    st.session_state['df'] = df
                    st.session_state['total_records'] = len(df)
                    st.session_state.current_page = 1  # ページネーションをリセット
            else:
                st.error(f"データの読み込みまたはフィルタリングに失敗しました: {parquet_file_path}")
        else:
            st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")

    rows_options = [20, 50, 100, 200, 500, 1000, 2000, 5000]

    # データフレームの取得
    df = st.session_state.get('df', None)
    if df is not None and not df.empty:

        # セッションステートにページネーション情報を初期化
        if 'limit' not in st.session_state:
            st.session_state['limit'] = 100  # デフォルト値を設定
        if 'current_page' not in st.session_state:
            st.session_state['current_page'] = 1

        page_size = st.session_state['limit']
        total_pages = (len(df) + page_size - 1) // page_size

        # 件数表示と行数の切り替え
        cols_pagination_top = st.columns([2, 7, 1])
        with cols_pagination_top[0]:
            start_index = (st.session_state.current_page - 1) * page_size + 1
            end_index = min(st.session_state.current_page * page_size, len(df))
            end_index = min(end_index, len(df))  # 絞り込み結果が少ない場合の処理
            st.write(f"{start_index:,} - {end_index:,} / {len(df):,} 件")
        with cols_pagination_top[2]:
            clean_df = df.dropna(how='all').reset_index(drop=True)
            now = datetime.now().strftime("%Y-%m-%d-%H%M%S")

            # NaN、None、'nan'、'None'を空文字列に置換する関数
            def replace_null_values(val):
                if pd.isna(val) or val is None or val == 'nan' or val == 'None':
                    return ''
                return val

            # CSVダウンロード用にデータを処理
            csv_df = clean_df.sort_index(ascending=True)
            csv_df = csv_df.applymap(replace_null_values)

            # 数値型の列で空文字列になっているセルを0に置換
            numeric_columns = csv_df.select_dtypes(include=['int64', 'float64']).columns
            for col in numeric_columns:
                csv_df[col] = csv_df[col].replace('', 0)

            # オブジェクト型の列で '#######' を空文字列に置換
            object_columns = csv_df.select_dtypes(include=['object']).columns
            for col in object_columns:
                csv_df[col] = csv_df[col].replace('#######', '')

            csv_data = csv_df.to_csv(index=False).encode('cp932', errors='ignore')
            st.download_button(
                label="CSV DL",
                data=csv_data,
                file_name=f'{now}_export.csv',
                mime='text/csv'
            )
            limit = st.selectbox("", rows_options, index=rows_options.index(st.session_state['limit']), key="rows_selectbox", on_change=on_limit_change)
            st.session_state['limit'] = limit  # 選択された行数をセッションステートに保存
        with cols_pagination_top[1]:
            st.empty()

        # データフレームの行数を制限して取得
        start_index = (st.session_state.current_page - 1) * page_size
        end_index = start_index + page_size
        df_view = df.iloc[start_index:end_index]
        if not df_view.empty:
            # スタイルを適用
            styled_df = apply_styles(df_view)
            st.dataframe(styled_df, use_container_width=True, height=700)

            # ページネーションボタンをテーブルの真下に配置
            cols_pagination_buttons = st.columns([7, 1, 1])
            with cols_pagination_buttons[1]:
                if st.session_state.current_page > 1:
                    if st.button("◀ ◀ 前へ", key="prev_button"):
                        st.session_state.current_page -= 1
            with cols_pagination_buttons[2]:
                if st.session_state.current_page < total_pages:
                    if st.button("次へ ▶ ▶", key="next_button"):
                        st.session_state.current_page += 1
        else:
            st.warning("DataFrame view is empty.")
    else:
        st.warning("DataFrame is None or empty. Cannot display the table.")
else:
    st.error("指定されている項目がありません")
