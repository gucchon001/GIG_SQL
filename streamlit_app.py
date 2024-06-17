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
    get_filtered_data_from_sheet, on_sql_file_change, apply_styles, calculate_offset
)

# ロガーの設定
LOGGER = setup_department_logger('main')

# セッションステートを初期化
initialize_session_state()

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

# ダークモード対応のCSS
dark_mode_css = """
<style>
body {
    background-color: #333;
    color: #fff;
}
table {
    background-color: #444;
    color: #fff;
}
thead th {
    background-color: #555;
    color: #fff;
}
tbody tr:nth-child(even) {
    background-color: #555;
}
tbody tr:nth-child(odd) {
    background-color: #666;
}
</style>
"""
st.markdown(dark_mode_css, unsafe_allow_html=True)

# サイドバーにSQLファイルリストを表示
sql_files_dict = load_sql_list_from_spreadsheet()
sql_file_display_names = list(sql_files_dict.keys())

# セッションステートに選択されたファイルを設定
if "selected_display_name" not in st.session_state:
    st.session_state.selected_display_name = sql_file_display_names[0]  # 一番上のファイルをデフォルトで選択
    on_sql_file_change(sql_files_dict)  # 初期選択時のフィルタリングを実行

# Parquetファイル選択のラジオボタン
selected_display_name = st.sidebar.radio("", sql_file_display_names, key="selected_display_name", on_change=lambda: on_sql_file_change(sql_files_dict))
LOGGER.info(f"Selected display name: {selected_display_name}")

# 絞込検索とテーブル表示を初期化
sql_file_name = get_sql_file_name(selected_display_name)
LOGGER.info(f"SQL file name: {sql_file_name}")
sheet = load_sheet_from_spreadsheet(sql_file_name)
data = get_filtered_data_from_sheet(sheet)
LOGGER.info(f"Filtered data: {data}")

if data:
    with st.expander("絞込検索"):
        with st.form(key='filter_form'):
            input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
            for field in input_fields:
                if f"input_{field}" not in st.session_state:
                    st.session_state[f"input_{field}"] = input_fields[field]

            st.session_state['input_fields'] = input_fields
            st.session_state['input_fields_types'] = input_fields_types
            st.session_state['options_dict'] = options_dict

            col1, col2, col3 = st.columns([9, 1, 1])
            with col3:
                submit_button = st.form_submit_button(label='絞込')
                if submit_button:
                    input_values = st.session_state['input_fields']
                    input_fields_types = st.session_state['input_fields_types']
                    parquet_file_path = f"data_parquet/{sql_file_name}.parquet"

                    if os.path.exists(parquet_file_path):
                        LOGGER.info(f"Parquetファイルパス: {parquet_file_path}")
                        df = load_and_filter_parquet(parquet_file_path, input_values, input_fields_types)
                        if df is not None:
                            if df.empty:
                                st.warning("絞込条件に合致するデータがありません。")
                            else:
                                LOGGER.info(f"DataFrame after filtering: {df.head()}")
                                st.session_state['df'] = df
                                st.session_state['total_records'] = len(df)
                        else:
                            LOGGER.error(f"DataFrame is None after loading and filtering: {parquet_file_path}")
                            st.error(f"データの読み込みまたはフィルタリングに失敗しました: {parquet_file_path}")
                    else:
                        st.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
                        LOGGER.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
else:
    st.error("指定されている項目がありません")

# データフレームの取得とページネーションの設定は、絞込検索の後に配置します
df = st.session_state.get('df', None)  # ここで df を st.session_state から取得
if df is not None and not df.empty:
    rows_options = [100, 200, 500]

    LOGGER.info(f"DataFrame loaded, total rows: {len(df)}")

    # ページネーションの設定
    if 'limit' in st.session_state:
        page_size = st.session_state['limit']
    else:
        page_size = 100  # デフォルト値を設定
    LOGGER.info(f"Page size: {page_size}")

    current_page = st.number_input('Current Page', min_value=1, value=1, step=1, key="current_page")
    LOGGER.info(f"Current page: {current_page}")

    cols_rows = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    with cols_rows[8]:
        limit = st.selectbox("行数", rows_options, index=rows_options.index(st.session_state['limit']), key="rows_selectbox_top", on_change=on_limit_change)
        st.session_state['limit'] = limit  # 選択された行数をセッションステートに保存

    with cols_rows[9]:
        total_records = st.session_state['total_records']
        st.write(f"{page_size} / {total_records}")
        LOGGER.info(f"Total records: {total_records}, Page size: {page_size}")

    # データフレームの行数を制限して取得
    if df is not None:
        LOGGER.info(f"df is not None before calling load_and_prepare_data")
        df_view = load_and_prepare_data(df, current_page, page_size)  # オフセットとページサイズを考慮してデータを準備
        if df_view is not None and not df_view.empty:
            LOGGER.info(f"DataFrame view loaded, total rows: {len(df_view)}")

            # スタイルを適用
            styled_df = apply_styles(df_view)

            # テーブルの高さを行数に応じて動的に設定
            table_height = min(600, 24 * page_size)  # 1行あたり約24pxの高さを確保
            st.dataframe(styled_df, height=table_height, use_container_width=True)
            LOGGER.info(f"Table displayed with limit: {page_size} and offset: {calculate_offset(current_page, page_size)}")
        else:
            LOGGER.warning("DataFrame view is None or empty after calling load_and_prepare_data. Cannot display the table.")
    else:
        LOGGER.warning("DataFrame is None before calling load_and_prepare_data. Cannot display the table.")
else:
    LOGGER.warning("DataFrame is None or empty. Cannot display the table.")
