import os
import streamlit as st
import pandas as pd
import logging
from subcode_streamlit_loader_2 import (
    load_sql_list_from_spreadsheet, create_dynamic_input_fields,
    initialize_session_state, on_limit_change, on_search_click,
    load_and_prepare_data, get_sql_file_name, load_sheet_from_spreadsheet,
    get_filtered_data_from_sheet, on_sql_file_change, apply_styles
)

# Streamlitのアプリケーション設定
st.set_page_config(page_title="塾ステ CSVダウンロードツール ストミンくん β版", page_icon=":bar_chart:", layout="wide")

# ロガーの設定
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

# タイトルとアイコンを配置
col1, col2 = st.columns([1, 6])
with col1:
    st.image("https://icons.iconarchive.com/icons/papirus-team/papirus-apps/512/libreoffice-calc-icon.png", width=100)
with col2:
    st.title("塾ステ CSVダウンロードツール ストミンくん β版")

# セッションステートを初期化
initialize_session_state()

# サイドバーにSQLファイルリストを表示
sql_files_dict = load_sql_list_from_spreadsheet()
sql_file_display_names = list(sql_files_dict.keys())

# Parquetファイル選択のラジオボタン
selected_display_name = st.sidebar.radio("Parquetファイルを選択してください", sql_file_display_names, key="selected_display_name", on_change=lambda: on_sql_file_change(sql_files_dict))

# 行数の選択肢
rows_options = [20, 50, 100, 200, 500, 1000, 5000, 10000]

if st.session_state['df'] is not None:
    df = st.session_state['df']
    logger.info(f"Initial DataFrame rows: {len(df)}")

    # 初回のみ全件取得し、インデックスの降順で並べ替える
    if 'original_df' not in st.session_state:
        st.session_state['original_df'] = df
        df = df.sort_index(ascending=False)
        st.session_state['df'] = df

    cols_rows = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    with cols_rows[8]:
        limit = st.selectbox("行数", rows_options, index=rows_options.index(st.session_state['limit']), key="rows_selectbox_top", on_change=on_limit_change)
        st.session_state['limit'] = limit  # 選択された行数をセッションステートに保存

    with cols_rows[9]:
        total_records = st.session_state['total_records']
        if total_records <= st.session_state['limit']:
            st.session_state['selected_rows'] = total_records
        else:
            st.session_state['selected_rows'] = st.session_state['limit']

        st.write(f"{st.session_state['selected_rows']} / {st.session_state['total_records']}")
        logger.info(f"Total records: {total_records}, Selected rows: {st.session_state['selected_rows']}")

    # データフレームの行数を制限して取得
    df = load_and_prepare_data(df, st.session_state['selected_rows'])  # Ensure df is reloaded correctly
    styled_df = apply_styles(df)

    # テーブルの高さを行数に応じて動的に設定
    table_height = min(600, 24 * st.session_state['selected_rows'])  # 1行あたり約24pxの高さを確保
    st.dataframe(styled_df, height=table_height, use_container_width=True)
    logger.info(f"Table displayed with limit: {st.session_state['selected_rows']}")

if selected_display_name:
    sql_file_name = get_sql_file_name(selected_display_name)
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)

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
                        on_search_click()
    else:
        st.error("指定されている項目がありません")