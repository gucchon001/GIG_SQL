"""
Streamlit 表示ユーティリティモジュール

データ表示、スタイリング、ページネーション関連の機能
"""
import streamlit as st
import pandas as pd
from typing import Tuple, Optional, Any
import numpy as np
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def apply_styles(df: pd.DataFrame, selected_rows: int = 100) -> pd.DataFrame:
    """
    DataFrameにスタイルを適用
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        selected_rows (int): 表示行数
        
    Returns:
        pd.DataFrame: スタイル適用済みDataFrame
    """
    if df.empty:
        return df
    
    # ヘッダ行の文字数制限
    df_styled = df.copy()
    df_styled.columns = [truncate_text(str(col), 20) for col in df_styled.columns]
    
    # スタイル関数定義
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'
    
    # スタイル適用
    styled_df = df_styled.head(selected_rows).style.apply(
        highlight_header, axis=0
    ).map(white_background, subset=pd.IndexSlice[:, :])
    
    logger.debug(f"スタイル適用完了: {selected_rows}行")
    return styled_df


def truncate_text(text: str, max_length: int = 35) -> str:
    """
    テキストを指定長で切り詰め
    
    Args:
        text (str): 対象テキスト
        max_length (int): 最大長
        
    Returns:
        str: 切り詰め後テキスト
    """
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return str(text)


def display_data(df: pd.DataFrame, page_size: int, input_fields_types: dict) -> None:
    """
    データを表示（ページネーション対応）
    
    Args:
        df (pd.DataFrame): 表示対象DataFrame
        page_size (int): ページサイズ
        input_fields_types (dict): フィールドタイプ辞書
    """
    if df.empty:
        st.info("データがありません")
        return
    
    # 件数表示を追加
    total_rows = len(df)
    current_page = st.session_state.get('current_page', 1)
    start_index = (current_page - 1) * page_size + 1
    end_index = min(current_page * page_size, total_rows)
    
    # 件数表示と更新ボタン
    col1, col2, col3 = st.columns([2, 1, 2])
    with col1:
        st.markdown(f"**{start_index:,} - {end_index:,} / {total_rows:,} 件**")
    with col2:
        if st.button("🔄 更新", key="top_refresh", help="最新のデータを取得して表示を更新"):
            # セッション状態をクリアして再読み込みを促す
            if 'df' in st.session_state:
                del st.session_state['df']
            if 'df_view' in st.session_state:
                del st.session_state['df_view']
            # キャッシュクリア
            st.cache_data.clear()
            logger.info("トップ更新ボタンが押されました - セッション状態とキャッシュをクリア")
            st.rerun()
    
    # 行数選択UI
    display_row_selector()
    
    # テーブル更新ボタンとCSVダウンロードボタン
    display_table_action_buttons(df, input_fields_types)
    
    # データ準備
    df_view = get_paginated_df(df, page_size)
    
    # スタイル適用して表示
    display_styled_df(df_view)
    
    # ページネーション
    total_pages = (len(df) + page_size - 1) // page_size
    if total_pages > 1:
        display_pagination_buttons(total_pages)
    
    logger.info(f"データ表示完了: Full DataFrame shape: {df.shape}, Page size: {page_size}")


def display_row_selector() -> None:
    """行数選択UIを表示"""
    col1, col2 = st.columns([3, 1])
    
    with col2:
        rows_options = [10, 20, 50, 100, 200]
        current_limit = st.session_state.get('limit', 50)
        
        # デフォルト値のインデックスを取得
        try:
            default_index = rows_options.index(current_limit)
        except ValueError:
            default_index = 2  # 50がデフォルト
        
        limit = st.selectbox(
            "表示件数", rows_options,
            index=default_index,
            key="limit_selector"
        )
        
        if limit != st.session_state.get('limit', 50):
            st.session_state['limit'] = limit
            st.rerun()


def get_paginated_df(df: pd.DataFrame, page_size: int) -> pd.DataFrame:
    """
    ページネーション用にDataFrameを切り詰め
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        page_size (int): ページサイズ
        
    Returns:
        pd.DataFrame: ページネーション済みDataFrame
    """
    current_page = st.session_state.get('current_page', 1)
    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    
    logger.info(f"get_paginated_df: page_size={page_size}, start_index={start_index}, end_index={end_index}, df.shape={df.shape}")
    
    result_df = df.iloc[start_index:end_index]
    
    logger.info(f"get_paginated_df result shape: {result_df.shape}")
    logger.info(f"get_paginated_df: start_index={start_index}, end_index={end_index}")
    logger.info(f"get_paginated_df: Returning DataFrame shape: {result_df.shape}")
    
    return result_df


def display_styled_df(df: pd.DataFrame) -> None:
    """
    スタイル適用済みDataFrameを表示
    
    Args:
        df (pd.DataFrame): 表示対象DataFrame
    """
    if df.empty:
        st.info("表示するデータがありません")
        return
    
    logger.info(f"display_styled_df: DataFrame shape = {df.shape}")
    
    # スタイル適用
    styled_df = apply_styles(df, len(df))
    
    # Streamlitで表示
    st.dataframe(styled_df, use_container_width=True)


def display_pagination_buttons(total_pages: int) -> None:
    """
    ページネーションボタンを表示
    
    Args:
        total_pages (int): 総ページ数
    """
    current_page = st.session_state.get('current_page', 1)
    
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col1:
        if st.button("最初", disabled=(current_page == 1)):
            st.session_state['current_page'] = 1
            st.rerun()
    
    with col2:
        if st.button("前へ", disabled=(current_page == 1)):
            st.session_state['current_page'] = current_page - 1
            st.rerun()
    
    with col3:
        st.write(f"ページ {current_page} / {total_pages}")
    
    with col4:
        if st.button("次へ", disabled=(current_page == total_pages)):
            st.session_state['current_page'] = current_page + 1
            st.rerun()
    
    with col5:
        if st.button("最後", disabled=(current_page == total_pages)):
            st.session_state['current_page'] = total_pages
            st.rerun()


def display_table_action_buttons(df: pd.DataFrame, input_fields_types: dict) -> None:
    """
    テーブル操作ボタン（更新・CSVダウンロード）を表示
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        input_fields_types (dict): フィールドタイプ辞書
    """
    if df.empty:
        return
    
    col1, col2, col3 = st.columns([1, 1, 3])
    
    with col1:
        if st.button("🔄 テーブル更新", help="最新のデータを取得して表示を更新します"):
            # セッション状態をクリアして再読み込みを促す
            if 'df' in st.session_state:
                del st.session_state['df']
            if 'df_view' in st.session_state:
                del st.session_state['df_view']
            if 'input_fields_types' in st.session_state:
                del st.session_state['input_fields_types']
            # キャッシュクリア
            st.cache_data.clear()
            logger.info("テーブル更新ボタンが押されました - セッション状態とキャッシュをクリア")
            st.rerun()
    
    with col2:
        # CSVダウンロードボタン
        display_csv_download_button(df, input_fields_types)


def display_csv_download_button(df: pd.DataFrame, input_fields_types: dict) -> None:
    """
    CSVダウンロードボタンを表示
    
    Args:
        df (pd.DataFrame): ダウンロード対象DataFrame
        input_fields_types (dict): フィールドタイプ辞書
    """
    if df.empty:
        return
    
    try:
        # データ準備
        from src.utils.data_processing import prepare_csv_data
        csv_df = prepare_csv_data(df, input_fields_types)
        
        # CSV文字列に変換（Shift-JIS対応）
        csv_string = csv_df.to_csv(index=False)
        
        # ダウンロードボタン（Shift-JISエンコーディング）
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv_string.encode('cp932', errors='replace'),
            file_name=f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key="csv_download"
        )
        
        logger.info("CSVダウンロードボタン表示完了")
        
    except Exception as e:
        logger.error(f"CSVダウンロードボタン表示エラー: {e}")
        st.error("CSVダウンロードの準備でエラーが発生しました")


def create_pagination_ui(df: pd.DataFrame, page_size: int) -> pd.DataFrame:
    """
    ページネーション付きUIを作成
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        page_size (int): ページサイズ
        
    Returns:
        pd.DataFrame: 表示用DataFrame
    """
    if df.empty:
        st.info("データがありません")
        return pd.DataFrame()
    
    total_records = len(df)
    total_pages = (total_records + page_size - 1) // page_size
    current_page = st.session_state.get('current_page', 1)
    
    # ページ情報表示
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write(f"総レコード数: {total_records:,}件")
    with col2:
        st.write(f"ページ: {current_page}/{total_pages}")
    
    # ページネーション
    if total_pages > 1:
        display_pagination_buttons(total_pages)
    
    # データ取得
    paginated_df = get_paginated_df(df, page_size)
    
    logger.info(f"ページネーションUI作成完了: ページ{current_page}/{total_pages}")
    return paginated_df


def setup_ui(last_modified: Optional[str]) -> None:
    """
    基本UIセットアップ
    
    Args:
        last_modified (Optional[str]): 最終更新日時
    """
    if last_modified:
        st.sidebar.write(f"最終データ取得日時: {last_modified}")
    
    # スタイル適用
    from src.streamlit_system.ui.styles import apply_custom_styles
    apply_custom_styles()
    
    logger.debug("基本UI設定完了")


def display_metrics(df: pd.DataFrame) -> None:
    """
    データメトリクスを表示
    
    Args:
        df (pd.DataFrame): 対象DataFrame
    """
    if df.empty:
        return
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("総レコード数", f"{len(df):,}")
    
    with col2:
        st.metric("列数", len(df.columns))
    
    with col3:
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        st.metric("メモリ使用量", f"{memory_usage:.1f} MB")
    
    logger.debug("メトリクス表示完了")