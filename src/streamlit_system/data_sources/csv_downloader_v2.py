"""
CSV ダウンロード機能 (新構造版)

新構造のモジュールを使用したCSVダウンロード機能
"""
import streamlit as st
import pandas as pd
from typing import Optional
from src.core.logging.logger import get_logger
from src.streamlit_system.ui.session_manager import create_dynamic_input_fields, initialize_session_state
from src.streamlit_system.ui.display_utils import display_data, setup_ui
from src.utils.data_processing import get_parquet_file_last_modified, load_parquet_file

logger = get_logger(__name__)


def get_sql_file_name(selected_option: str) -> Optional[str]:
    """
    選択されたオプションに対応するSQLファイル名を取得
    
    Args:
        selected_option (str): 選択されたオプション
        
    Returns:
        Optional[str]: SQLファイル名（拡張子なし）
    """
    sql_files_dict = st.session_state.get('sql_files_dict', {})
    sql_file_name = sql_files_dict.get(selected_option)
    
    if sql_file_name:
        logger.info(f"選択されたオプション '{selected_option}' に対応するSQLファイル名: {sql_file_name}")
        return sql_file_name.replace('.sql', '')
    else:
        logger.warning(f"選択されたオプション '{selected_option}' に対応するSQLファイルが見つかりません。")
        return None


def load_data(sql_file_name: str) -> Optional[dict]:
    """
    SQLファイル名に基づいてデータを読み込み
    
    Args:
        sql_file_name (str): SQLファイル名
        
    Returns:
        Optional[dict]: データ辞書
    """
    try:
        parquet_file_path = f"data_Parquet/{sql_file_name}.parquet"
        
        # Parquetファイルの存在確認
        import os
        if not os.path.exists(parquet_file_path):
            logger.warning(f"Parquetファイルが見つかりません: {parquet_file_path}")
            return None
        
        # 最終更新日時を取得
        last_modified = get_parquet_file_last_modified(parquet_file_path)
        
        data = {
            'sql_file_name': sql_file_name,
            'parquet_file_path': parquet_file_path,
            'last_modified': last_modified
        }
        
        logger.info(f"データ読み込み完了: {sql_file_name}")
        return data
        
    except Exception as e:
        logger.error(f"データ読み込みエラー: {e}")
        return None


def handle_filter_submission(parquet_file_path: str) -> None:
    """
    フィルタ送信処理
    
    Args:
        parquet_file_path (str): Parquetファイルパス
    """
    try:
        input_fields = st.session_state.get('input_fields', {})
        input_fields_types = st.session_state.get('input_fields_types', {})
        options_dict = st.session_state.get('options_dict', {})
        
        # Parquetファイルを読み込み、フィルタリング
        from src.utils.data_processing import load_and_filter_parquet
        df = load_and_filter_parquet(parquet_file_path, input_fields, input_fields_types, options_dict)
        
        if df is not None and not df.empty:
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
            st.session_state['current_page'] = 1  # ページをリセット
            logger.info(f"フィルタ適用完了: {len(df)}件")
        else:
            st.warning("フィルタ条件に一致するデータがありません")
            st.session_state['df'] = pd.DataFrame()
            st.session_state['total_records'] = 0
            
    except Exception as e:
        logger.error(f"フィルタ処理エラー: {e}")
        st.error("フィルタ処理中にエラーが発生しました")


def load_and_initialize_data(sql_file_name: str, num_rows: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    データを読み込み、初期化
    
    Args:
        sql_file_name (str): SQLファイル名
        num_rows (Optional[int]): 読み込み行数制限
        
    Returns:
        Optional[pd.DataFrame]: 読み込み済みDataFrame
    """
    try:
        parquet_file_path = f"data_Parquet/{sql_file_name}.parquet"
        
        # Parquetファイル読み込み
        df = load_parquet_file(parquet_file_path, num_rows)
        
        if df is not None:
            # セッション状態に保存
            st.session_state['df'] = df
            st.session_state['total_records'] = len(df)
            st.session_state['current_page'] = 1
            
            logger.info(f"データ初期化完了: {len(df)}件")
            return df
        else:
            logger.warning("データの読み込みに失敗しました")
            return None
            
    except Exception as e:
        logger.error(f"データ初期化エラー: {e}")
        return None


def csv_download(selected_display_name: str) -> None:
    """
    CSVダウンロード機能のメイン処理（新構造版）
    
    Args:
        selected_display_name (str): 選択された表示名
    """
    try:
        # SQLファイル名を取得
        sql_file_name = get_sql_file_name(selected_display_name)
        if not sql_file_name:
            st.error("SQLファイル名の取得に失敗しました")
            return
        
        logger.info(f"Selected SQL file: {sql_file_name}")
        
        # データ読み込み
        data = load_data(sql_file_name)
        if not data:
            st.error("データの読み込みに失敗しました")
            return
        
        # セッション状態を初期化
        initialize_session_state()
        
        # UI設定
        setup_ui(data.get('last_modified'))
        
        # フィルタ送信フォーム
        with st.form("filter_form", clear_on_submit=False):
            st.subheader("🔍 絞り込み条件")
            
            # 動的入力フィールドの作成（簡易版）
            col1, col2 = st.columns(2)
            with col1:
                search_text = st.text_input("検索キーワード", key="search_keyword")
            with col2:
                submitted = st.form_submit_button("絞り込み", type="primary")
            
            if submitted:
                logger.info("フィルタが送信されました")
                handle_filter_submission(data['parquet_file_path'])
        
        # データ表示
        df = st.session_state.get('df')
        if df is not None and not df.empty:
            page_size = st.session_state.get('limit', 20)
            input_fields_types = st.session_state.get('input_fields_types', {})
            display_data(df, page_size, input_fields_types)
        else:
            # 初期データ読み込み
            initial_df = load_and_initialize_data(sql_file_name, 1000)  # 最初は1000件制限
            if initial_df is not None:
                page_size = st.session_state.get('limit', 20)
                input_fields_types = st.session_state.get('input_fields_types', {})
                display_data(initial_df, page_size, input_fields_types)
            else:
                st.error("データが見つかりません")
        
        logger.info("CSV download function call completed")
        
    except Exception as e:
        logger.error(f"CSVダウンロード処理エラー: {e}")
        st.error("CSVダウンロード処理中にエラーが発生しました")


# 後方互換性のためのエイリアス
csv_download_v2 = csv_download