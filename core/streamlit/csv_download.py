import streamlit as st
import json
import os
from src.core.logging.logger import get_logger
from src.streamlit_system.ui.session_manager import initialize_session_state

# === 新構造モジュール ===
from src.utils.data_processing import get_parquet_file_last_modified, load_parquet_file
from src.streamlit_system.ui.display_utils import display_data

# === 旧構造モジュール（段階的移行中） ===
from ..utils.utils import setup_ui as old_setup_ui, load_data, handle_filter_submission, load_and_initialize_data
from .subcode_streamlit_loader import (
    get_sql_file_name,
    load_sheet_from_spreadsheet, 
    get_filtered_data_from_sheet, 
    load_and_filter_parquet, 
    create_dynamic_input_fields
)

# ロガーの設定
logger = get_logger(__name__)

def csv_download(selected_display_name):
    try:
        sql_file_name = get_sql_file_name(selected_display_name)
        logger.info(f"Selected SQL file: {sql_file_name}")
        
        # キャッシュ経由でスプレッドシート設定を取得
        data = _cached_load_data(sql_file_name)
        logger.debug(f"load_data result: {len(data) if data else 0}件のフィルタ設定を取得")
        
        initialize_session_state()
        
        with open('styles.css', encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
        
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
        
        # 設定ファイルのパスを使用（Windows UNCパス対応）
        parquet_file_path = os.path.join(csv_base_path, f"{sql_file_name}.parquet")
        logger.info(f"Parquetファイルパス: {parquet_file_path}")
        logger.info(f"Parquetファイル存在確認: {os.path.exists(parquet_file_path)}")
        last_modified = get_parquet_file_last_modified(parquet_file_path)
        
        # Noneの場合のデフォルト値を設定
        if last_modified is None:
            last_modified = "ファイルが見つかりません"
        
        if not data:
            st.error("指定されている項目がありません")
            return
        
        old_setup_ui(last_modified)
        
        # ページ表示が変更されたかどうかを確認
        if 'last_selected_table' not in st.session_state or st.session_state['last_selected_table'] != selected_display_name:
            # テーブル変更時のみページネーションをリセット（表示件数は保持）
            if 'limit' not in st.session_state:
                st.session_state['limit'] = 50  # 初回のみデフォルト設定
            st.session_state['current_page'] = 1
            st.session_state['last_selected_table'] = selected_display_name
        
        # 初期状態は閉じる。押下後は閉じるためのフラグで制御
        if 'filter_expanded' not in st.session_state:
            st.session_state['filter_expanded'] = False
        with st.expander("絞込検索", expanded=st.session_state.get('filter_expanded', True)):
            submit_button = create_filter_form(data)
        
        # 直近のフィルター入力がセッションに存在する場合は再実行時もフィルター継続
        has_filter_state = any(k.startswith('input_') for k in st.session_state.keys())

        if submit_button or has_filter_state:
            logger.info("フィルター送信ボタンが押されました")
            filters_signature = json.dumps(st.session_state.get('input_fields', {}), ensure_ascii=False, sort_keys=True, default=str)
            df = _cached_handle_filter_submission(parquet_file_path, filters_signature)
            logger.info(f"handle_filter_submission結果: {df.shape if df is not None and not df.empty else 'None/Empty'}")
            # ページのリセットは明示的な送信時のみ行う（再描画では保持）
            if submit_button:
                st.session_state['current_page'] = 1
            if submit_button:
                # ボタン押下直後はアコーディオンを閉じる
                st.session_state['filter_expanded'] = False
        else:
            logger.info("データ初期読み込み処理を開始")
            df = _cached_load_and_initialize_data(sql_file_name)
            logger.info(f"load_and_initialize_data結果: {df.shape if df is not None and not df.empty else 'None/Empty'}")
        
        if df is not None and not df.empty:
            # Parquet 全件読み込み → クライアント側ページングに戻す
            page_size = st.session_state.get('limit', 20)
            st.session_state['total_records'] = len(df)
            st.session_state['__data_is_paged__'] = False
            logger.info(f"csv_download: Calling display_data with page_size={page_size}")
            display_data(df, page_size, st.session_state['input_fields_types'])
        else:
            logger.warning(f"DataFrame表示不可: df={df}")
            st.warning("DataFrame is None or empty. Cannot display the table.")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        st.error(f"エラーが発生しました: {str(e)}")

def create_filter_form(data):
    with st.form(key='filter_form'):
        logger.debug(f"create_filter_form: データ件数 = {len(data) if data else 0}")
        logger.debug(f"create_filter_form: data sample = {data[:2] if data else []}")
        
        # データの構造をチェック
        if data:
            for i, item in enumerate(data[:3]):
                logger.debug(f"データ{i}: {item}")
        
        input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
        logger.debug(f"create_filter_form: input_fields = {len(input_fields) if input_fields else 0}件")
        logger.debug(f"create_filter_form: input_fields_types = {len(input_fields_types) if input_fields_types else 0}件")
        logger.debug(f"create_filter_form: input_fields keys = {list(input_fields.keys()) if input_fields else []}")
        
        if not input_fields:
            st.info("フィルタ項目がありません。スプレッドシートの設定を確認してください。")
            logger.warning("フィルタ項目が空です")
        else:
            st.info(f"フィルタ項目が{len(input_fields)}件見つかりました")
        
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


# ===== キャッシュラッパー =====

@st.cache_data(ttl=300, show_spinner=False, max_entries=64)
def _cached_load_data(sql_file_name: str):
    """スプレッドシート設定のキャッシュ取得"""
    # 旧関数は引数だけで決まるためキャッシュキーは関数引数に委譲
    return load_data(sql_file_name)


@st.cache_data(ttl=300, show_spinner=False, max_entries=16)
def _cached_load_and_initialize_data(sql_file_name: str):
    """初期データ読み込みのキャッシュ取得（Parquet）"""
    return load_and_initialize_data(sql_file_name)


@st.cache_data(ttl=120, show_spinner=False, max_entries=64)
def _cached_handle_filter_submission(parquet_file_path: str, filters_signature: str):
    """フィルター適用後データのキャッシュ取得（Parquet）。フィルタ条件でキャッシュキーを分離"""
    return handle_filter_submission(parquet_file_path)