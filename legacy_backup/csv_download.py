import streamlit as st
from src.core.logging.logger import get_logger
from src.streamlit_system.ui.session_manager import create_dynamic_input_fields, initialize_session_state

# 新構造への移行中（一時的に残す関数）
from legacy_backup.subcode_streamlit_loader import get_sql_file_name

# 新構造のモジュールを使用
from src.utils.data_processing import get_parquet_file_last_modified, load_parquet_file
from src.streamlit_system.ui.display_utils import display_data, setup_ui

# ロガーの設定
logger = get_logger(__name__)

def csv_download(selected_display_name):
    try:
        sql_file_name = get_sql_file_name(selected_display_name)
        logger.info(f"Selected SQL file: {sql_file_name}")
        
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
            # フィルター適用時はページのみリセット（表示件数は保持）
            st.session_state['current_page'] = 1
        else:
            df = load_and_initialize_data(sql_file_name)
        
        if df is not None and not df.empty:
            page_size = st.session_state.get('limit', 20)
            logger.info(f"csv_download: Calling display_data with page_size={page_size}")
            display_data(df, page_size, st.session_state['input_fields_types'])
        else:
            st.warning("DataFrame is None or empty. Cannot display the table.")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
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