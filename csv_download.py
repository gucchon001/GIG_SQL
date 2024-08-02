import streamlit as st
from my_logging import setup_department_logger
from subcode_streamlit_loader import (
    create_dynamic_input_fields, initialize_session_state, get_sql_file_name
)
from utils import (
    get_parquet_file_last_modified, load_data, setup_ui, handle_filter_submission, display_data, load_and_initialize_data
)

# ロガーの設定
LOGGER = setup_department_logger('csv_download')

def csv_download(selected_display_name):
    try:
        sql_file_name = get_sql_file_name(selected_display_name)
        LOGGER.info(f"Selected SQL file: {sql_file_name}")
        
        data = load_data(sql_file_name)
        
        initialize_session_state()
        
        with open('styles.css', encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
        
        parquet_file_path = f"data_parquet/{sql_file_name}.parquet"
        last_modified = get_parquet_file_last_modified(parquet_file_path)
        
        if not data:
            st.error("指定されている項目がありません")
            return
        
        setup_ui(last_modified)
        
        # ページ表示が変更されたかどうかを確認
        if 'last_selected_table' not in st.session_state or st.session_state['last_selected_table'] != selected_display_name:
            # ページネーションをデフォルトにリセット
            st.session_state['limit'] = 20
            st.session_state['current_page'] = 1
            st.session_state['last_selected_table'] = selected_display_name
        
        with st.expander("絞込検索"):
            submit_button = create_filter_form(data)
        
        if submit_button:
            df = handle_filter_submission(parquet_file_path)
            # フィルター適用時もページネーションをリセット
            st.session_state['limit'] = 20
            st.session_state['current_page'] = 1
        else:
            df = load_and_initialize_data(sql_file_name)
        
        if df is not None and not df.empty:
            page_size = st.session_state.get('limit', 20)
            LOGGER.info(f"csv_download: Calling display_data with page_size={page_size}")
            display_data(df, page_size, st.session_state['input_fields_types'])
        else:
            st.warning("DataFrame is None or empty. Cannot display the table.")
    
    except Exception as e:
        LOGGER.error(f"An error occurred: {str(e)}")
        st.error(f"エラーが発生しました: {str(e)}")

def create_filter_form(data):
    with st.form(key='filter_form'):
        input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
        for field in input_fields:
            if f"input_{field}" not in st.session_state:
                st.session_state[f"input_{field}"] = input_fields[field]

        st.session_state['input_fields'] = input_fields
        st.session_state['input_fields_types'] = input_fields_types
        st.session_state['options_dict'] = options_dict

        cols = st.columns([9, 1])
        with cols[1]:
            submit_button = st.form_submit_button(label='絞込')
    
    return submit_button

def create_filter_form(data):
    with st.form(key='filter_form'):
        input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
        for field in input_fields:
            if f"input_{field}" not in st.session_state:
                st.session_state[f"input_{field}"] = input_fields[field]

        st.session_state['input_fields'] = input_fields
        st.session_state['input_fields_types'] = input_fields_types
        st.session_state['options_dict'] = options_dict

        cols = st.columns([9, 1])
        with cols[1]:
            submit_button = st.form_submit_button(label='絞込')
    
    return submit_button