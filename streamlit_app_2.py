import streamlit as st

# Streamlitのアプリケーション設定
st.set_page_config(page_title="塾ステ CSVダウンロードツール ストミンくん β版", page_icon=":bar_chart:", layout="wide")

import pandas as pd
import logging
import traceback
from subcode_streamlit_loader_2 import load_sql_list_from_spreadsheet, create_dynamic_input_fields, initialize_session_state, on_sql_file_change, on_limit_change, on_search_click, load_and_prepare_data
from subcode_tkinter_loader import get_sql_file_name, load_sheet_from_spreadsheet, get_filtered_data_from_sheet
from my_logging import setup_department_logger

# ロガーの設定
logger = setup_department_logger('main')

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

# SQLファイル選択のラジオボタン
selected_display_name = st.sidebar.radio("SQLファイルを選択してください", sql_file_display_names, key="selected_display_name", on_change=on_sql_file_change)

# 行数の選択肢
rows_options = [20, 50, 100, 200, 500, 1000, 5000, 10000]

# テーブルの表示
if selected_display_name:
    sql_file_name = get_sql_file_name(selected_display_name)
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)
    
    if data:
        with st.expander("絞込検索"):
            with st.form(key='filter_form'):
                input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
                # フィールド値をセッションステートに保存しますが、セッションステートを直接使用しません
                for field in input_fields:
                    if f"input_{field}" not in st.session_state:
                        st.session_state[f"input_{field}"] = input_fields[field]
                
                st.session_state['input_fields'] = input_fields
                st.session_state['input_fields_types'] = input_fields_types
                st.session_state['options_dict'] = options_dict

                # ボタンを右寄せに配置
                col1, col2, col3 = st.columns([9, 1, 1])
                with col3:
                    submit_button = st.form_submit_button(label='絞込')
                    if submit_button:
                        on_search_click()
    else:
        st.error("指定されている項目がありません")

if st.session_state['df'] is not None:
    df = st.session_state['df']
    
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

    styled_df = load_and_prepare_data(df, st.session_state['selected_rows'])
    st.dataframe(styled_df, height=600, use_container_width=True)
    logger.info(f"Table displayed with limit: {st.session_state['selected_rows']}")