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
    DataFrameにスタイルを適用（高速化最適化）
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        selected_rows (int): 表示行数
        
    Returns:
        pd.DataFrame: スタイル適用済みDataFrame
    """
    if df.empty:
        return df
    
    # 高速化: データ量に応じたスタイル簡素化
    df_styled = df.copy()
    
    # ヘッダ行の文字数制限（適応的）
    header_limit = 15 if len(df.columns) > 10 else 20
    df_styled.columns = [truncate_text(str(col), header_limit) for col in df_styled.columns]
    
    # セル内容の最適化（長いテキストを省略）
    for col in df_styled.columns:
        if df_styled[col].dtype == 'object':  # 文字列列のみ
            df_styled[col] = df_styled[col].astype(str).apply(
                lambda x: truncate_text(x, 30) if pd.notna(x) else x
            )
    
    # 高速化: 行数が多い場合はスタイル簡素化
    if selected_rows > 200:
        # 軽量スタイル
        logger.debug(f"軽量スタイル適用: {selected_rows}行")
        return df_styled.head(selected_rows)
    else:
        # 通常スタイル
        def highlight_header(s):
            return ['background-color: lightgrey' for _ in s]

        def white_background(val):
            return 'background-color: white'
        
        styled_df = df_styled.head(selected_rows).style.apply(
            highlight_header, axis=0
        ).map(white_background, subset=pd.IndexSlice[:, :])
        
        logger.debug(f"通常スタイル適用完了: {selected_rows}行")
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
    データを表示（ページネーション対応・高速化最適化）
    
    Args:
        df (pd.DataFrame): 表示対象DataFrame
        page_size (int): ページサイズ
        input_fields_types (dict): フィールドタイプ辞書
    """
    import time
    start_time = time.time()
    
    if df.empty:
        st.info("データがありません")
        return
    
    # 件数表示を追加
    total_rows = len(df)
    current_page = st.session_state.get('current_page', 1)
    start_index = (current_page - 1) * page_size + 1
    end_index = min(current_page * page_size, total_rows)
    
    # 件数表示とパフォーマンス情報
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**{start_index:,} - {end_index:,} / {total_rows:,} 件**")
    with col2:
        # データサイズ表示
        data_size = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        st.caption(f"📊 {data_size:.1f}MB")
    
    # 行数選択UI
    display_row_selector()
    
    # テーブル更新ボタンとCSVダウンロードボタン
    display_table_action_buttons(df, input_fields_types)
    
    # ページネーション（CSVボタンとテーブルの間に配置）
    total_pages = (len(df) + page_size - 1) // page_size
    if total_pages > 1:
        display_pagination_buttons(total_pages)
    
    # データ準備
    df_view = get_paginated_df(df, page_size)
    
    # スタイル適用して表示
    display_styled_df(df_view)
    
    # パフォーマンス測定
    end_time = time.time()
    render_time = end_time - start_time
    
    # パフォーマンス情報を表示
    if render_time > 1.0:  # 1秒以上の場合は表示
        st.caption(f"⏱️ 表示時間: {render_time:.2f}秒")
    
    logger.info(f"データ表示完了: Full DataFrame shape: {df.shape}, Page size: {page_size}, Render time: {render_time:.3f}s")


def display_row_selector() -> None:
    """行数選択UIを表示（インテリジェント最適化）"""
    col1, col2 = st.columns([3, 1])
    
    with col2:
        # データ量に応じた行数オプション
        total_records = st.session_state.get('total_records', 0)
        if total_records > 10000:
            rows_options = [50, 100, 200, 500]
            recommended = 100
        elif total_records > 1000:
            rows_options = [20, 50, 100, 200]
            recommended = 50
        else:
            rows_options = [10, 20, 50, 100]
            recommended = 20
            
        current_limit = st.session_state.get('limit', recommended)
        
        # デフォルト値のインデックスを取得
        try:
            default_index = rows_options.index(current_limit)
        except ValueError:
            default_index = 1  # 推奨値
        
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
    # 確実に降順ソートを適用（legacy版と同じ動作）
    if not df.empty:
        df = df.sort_index(ascending=False)
        logger.debug("ページネーション前に降順ソートを適用")
    
    current_page = st.session_state.get('current_page', 1)
    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    
    logger.info(f"get_paginated_df: page_size={page_size}, start_index={start_index}, end_index={end_index}, df.shape={df.shape}")
    
    result_df = df.iloc[start_index:end_index]
    
    logger.info(f"get_paginated_df result shape: {result_df.shape}")
    logger.info(f"get_paginated_df: 降順ソート後のページネーション完了")
    
    return result_df


def display_styled_df(df: pd.DataFrame) -> None:
    """
    スタイル適用済みDataFrameを表示（高速化対応）
    
    Args:
        df (pd.DataFrame): 表示対象DataFrame
    """
    if df.empty:
        st.info("表示するデータがありません")
        return
    
    logger.info(f"display_styled_df: DataFrame shape = {df.shape}")
    
    # 高速化: 大量データの場合はスタイルを簡素化
    if len(df) > 1000:
        # 大量データ用：軽量表示（固定高さでレイヤー重なり防止）
        st.dataframe(
            df,
            use_container_width=True,
            height=500,  # 適切な固定高さ
            hide_index=True
        )
        logger.debug("大量データ用軽量表示を適用")
    else:
        # 通常データ用：スタイル適用（固定高さでレイヤー重なり防止）
        styled_df = apply_styles(df, len(df))
        st.dataframe(styled_df, use_container_width=True, height=350)
        logger.debug("通常スタイル表示を適用")


def display_pagination_buttons(total_pages: int) -> None:
    """
    ページネーションボタンを表示（コンパクト版）
    
    Args:
        total_pages (int): 総ページ数
    """
    current_page = st.session_state.get('current_page', 1)
    
    # ページネーションボタンを中央揃え（余白なし）
    col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 1, 1, 2, 1, 1, 1])
    
    with col2:
        if st.button("⏪ 最初", disabled=(current_page == 1), key="first_page"):
            st.session_state['current_page'] = 1
            st.rerun()
    
    with col3:
        if st.button("◀ 前へ", disabled=(current_page == 1), key="prev_page"):
            st.session_state['current_page'] = current_page - 1
            st.rerun()
    
    with col4:
        # コンパクトな中央表示（余白最小）
        st.markdown(f"<div style='text-align: center; margin: 0; padding: 0;'>"
                   f"<strong>{current_page} / {total_pages}</strong></div>", 
                   unsafe_allow_html=True)
    
    with col5:
        if st.button("次へ ▶", disabled=(current_page == total_pages), key="next_page"):
            st.session_state['current_page'] = current_page + 1
            st.rerun()
    
    with col6:
        if st.button("最後 ⏩", disabled=(current_page == total_pages), key="last_page"):
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
    
    # 右寄せで2つのボタンを配置
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col2:
        if st.button("🔄 更新", help="最新のデータを取得して表示を更新します"):
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
    
    with col3:
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