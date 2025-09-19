"""
Streamlit 表示ユーティリティモジュール

データ表示、スタイリング、ページネーション関連の機能
"""
import streamlit as st
import pandas as pd
from typing import Tuple, Optional, Any, Dict
import numpy as np
import json
from src.core.logging.logger import get_logger
from src.streamlit_system.data_sources.sql_loader import SQLLoader

logger = get_logger(__name__)
def _clear_prepared_csv_artifacts() -> None:
    keys_to_clear = [
        '__page_csv_ready__', '__page_csv_rows__', '__page_csv_filename__',
        '__full_csv_ready__', '__full_csv_rows__', '__full_csv_filename__'
    ]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]


try:
    from st_copy_to_clipboard import st_copy_to_clipboard
    logger.info("st_copy_to_clipboard正常にインポートされました")
except ImportError as e:
    st_copy_to_clipboard = None
    logger.warning(f"st_copy_to_clipboardインポートエラー: {e}")

try:
    import pyperclip
    logger.info("pyperclip正常にインポートされました")
except ImportError as e:
    pyperclip = None
    logger.warning(f"pyperclipインポートエラー: {e}")


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
    
    # 件数表示を追加（DB側ページング時は全件数をセッションから参照）
    data_is_paged = bool(st.session_state.get('__data_is_paged__', False))
    total_rows = st.session_state.get('total_records', len(df)) if data_is_paged else len(df)
    current_page = max(1, int(st.session_state.get('current_page', 1)))
    start_index = (current_page - 1) * page_size + 1 if total_rows > 0 else 0
    end_index = min(current_page * page_size, total_rows)
    
    # ページ/フィルタ/テーブル/件数が変わったら準備済みCSVをクリア
    try:
        selected_sql_file = st.session_state.get('selected_sql_file')
        filters_signature = json.dumps(st.session_state.get('input_fields', {}), ensure_ascii=False, sort_keys=True, default=str)
        context_signature = json.dumps({
            'sql': selected_sql_file,
            'filters': filters_signature,
            'page': current_page,
            'limit': page_size,
        }, ensure_ascii=False, sort_keys=True)
        prev_signature = st.session_state.get('__csv_ctx__')
        if prev_signature != context_signature:
            _clear_prepared_csv_artifacts()
            st.session_state['__csv_ctx__'] = context_signature
    except Exception:
        pass
    
    # ツールバー：左（リロード）｜中央（余白）｜右（コピー・ダウンロード）
    col_left, col_spacer, col_right = st.columns([1, 5.5, 2])
    
    # 行数セレクタはテーブル直上へ移動（右寄せ表示のため）
    
    # リロード（左寄せ）
    with col_left:
        refresh_clicked = st.button("🔄 リロード", help="ページをリロードします", key="refresh_data_header")
        if refresh_clicked:
            preserved_input_fields = st.session_state.get('input_fields', {}).copy()
            preserved_input_fields_types = st.session_state.get('input_fields_types', {}).copy()
            preserved_options_dict = st.session_state.get('options_dict', {}).copy()
            preserved_limit = st.session_state.get('limit', 20)
            logger.info(f"リセットボタンクリック - 条件保護中 (limit={preserved_limit})")
            if 'df' in st.session_state:
                del st.session_state['df']
            if 'df_view' in st.session_state:
                del st.session_state['df_view']
            _clear_prepared_csv_artifacts()
            st.session_state['input_fields'] = preserved_input_fields
            st.session_state['input_fields_types'] = preserved_input_fields_types
            st.session_state['options_dict'] = preserved_options_dict
            st.session_state['limit'] = preserved_limit
            st.session_state['current_page'] = 1
            st.cache_data.clear()
            logger.info("リセットボタン押下完了 - データのみクリア、絞り込み条件は保持")
            st.rerun()

    # 右側にコピー・ダウンロードを横並びで配置（右寄せ）
    with col_right:
        col_copy, col_dl = st.columns([1, 1])
        with col_copy:
            try:
                _ = df.head(0)
            except Exception:
                pass
            copy_clicked = st.button("📋 コピー", help="表示中のテーブルをコピー（TSV形式）", key="copy_to_clipboard_toolbar")
            if copy_clicked:
                try:
                    # 絞込アコーディオンは開かないよう維持
                    st.session_state['filter_expanded'] = False
                    if pyperclip is None:
                        raise RuntimeError("pyperclip が見つかりません。requirements.txt を確認してください。")
                    csv_data = get_paginated_df(df, st.session_state.get('limit', page_size)).to_csv(index=False, sep='\t')
                    pyperclip.copy(csv_data)
                    st.toast(f"📋 {len(get_paginated_df(df, st.session_state.get('limit', page_size)))}行をコピーしました", icon="✅")
                except Exception as e:
                    logger.error(f"コピー失敗: {e}")
                    st.error(f"コピーに失敗しました: {e}")
        with col_dl:
            download_df = df
            display_csv_download_button_isolated(download_df, input_fields_types)
    
    # メタ情報（サイズ/時間）は非表示に変更（ユーザー要望）
    
    # データ準備（DB側ページングならそのまま表示、従来はクライアント側でページング）
    if data_is_paged:
        df_view = df
    else:
        df_view = get_paginated_df(df, page_size)
    
    # ページネーションを下にコンパクト表示
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    if total_pages > 1:
        display_pagination_buttons(total_pages)
    
    # 件数表示と表示件数プルダウンをテーブル直前・右寄せで横並びに表示
    rows_options = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
    current_limit = st.session_state.get('limit', 20)
    try:
        default_index = rows_options.index(current_limit)
    except ValueError:
        default_index = rows_options.index(20)
        st.session_state['limit'] = 20
        logger.warning(f"表示件数{current_limit}がオプションにないため、20に変更")

    # 右寄せのため、左に余白カラムを設ける
    left_spacer, right_block = st.columns([5, 2])
    with right_block:
        count_col, rows_col = st.columns([2, 1])
        with count_col:
            st.markdown(
                f"<div style='text-align: right; font-weight: 600;'>{start_index:,} - {end_index:,} / {total_rows:,} 件</div>",
                unsafe_allow_html=True
            )
        with rows_col:
            selected_limit = st.selectbox(
                "表示件数",
                rows_options,
                index=default_index,
                key="limit_selector_stable",
                label_visibility="collapsed",
                help="1ページあたりの表示件数"
            )
    if selected_limit != current_limit:
        st.session_state['limit'] = selected_limit
        st.session_state['current_page'] = 1
        _clear_prepared_csv_artifacts()
        logger.info(f"表示件数を{current_limit}から{selected_limit}に変更、ページを1にリセット")
        # すぐに反映させるため明示的に再実行
        st.rerun()

    # スタイル適用して表示
    display_styled_df(df_view)
    
    # パフォーマンス測定
    end_time = time.time()
    render_time = end_time - start_time
    
    # 表示時間は非表示に変更（ユーザー要望）
    
    logger.info(f"データ表示完了: Full DataFrame shape: {df.shape}, Page size: {page_size}, Render time: {render_time:.3f}s")


def display_row_selector() -> None:
    """行数選択UIを表示（固定オプション）"""
    col1, col2 = st.columns([3, 1])
    
    with col2:
        # 固定の行数オプション（すべて選択可能）
        rows_options = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
        current_limit = st.session_state.get('limit', 20)
        
        # デフォルト値のインデックスを取得
        try:
            default_index = rows_options.index(current_limit)
        except ValueError:
            # 現在の値がオプションにない場合は20を選択
            default_index = rows_options.index(20)
            st.session_state['limit'] = 20
            logger.warning(f"表示件数{current_limit}がオプションにないため、20に変更")
        
        # 直接的なアプローチ：セレクトボックスの値変更を監視
        selected_limit = st.selectbox(
            "表示件数", 
            rows_options,
            index=default_index,
            key="limit_selector_stable"
        )
        
        # 値が変更された場合の処理（st.rerunを削除して検索条件リセットを防ぐ）
        if selected_limit != current_limit:
            st.session_state['limit'] = selected_limit
            st.session_state['current_page'] = 1  # ページを1にリセット
            logger.info(f"表示件数を{current_limit}から{selected_limit}に変更、ページを1にリセット")


def get_paginated_df(df: pd.DataFrame, page_size: int) -> pd.DataFrame:
    """
    ページネーション用にDataFrameを切り詰め
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        page_size (int): ページサイズ
        
    Returns:
        pd.DataFrame: ページネーション済みDataFrame
    """
    # 大規模データでの全件ソートは高コストのため回避
    if not df.empty and len(df) <= 5000:
        df = df.sort_index(ascending=False)
        logger.debug("ページネーション前に降順ソートを適用（<=5000件）")
    
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


def display_pagination_with_count(total_pages: int, start_index: int, end_index: int, total_rows: int) -> None:
    """
    件数表示とページネーションボタンを表示
    
    Args:
        total_pages (int): 総ページ数
        start_index (int): 開始インデックス
        end_index (int): 終了インデックス
        total_rows (int): 総行数
    """
    current_page = st.session_state.get('current_page', 1)
    
    # 件数表示とページネーションを分離（更新ボタンは別の場所へ移動）
    col1, col2, col3, col4, col5, col6 = st.columns([2, 1, 1, 2, 1, 1])
    
    with col1:
        # 件数表示（一番左）
        st.markdown(f"**{start_index:,} - {end_index:,} / {total_rows:,} 件**")
    
    with col2:
        if st.button("⏪ 最初", disabled=(current_page == 1), key="first_page_btn"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            preserved_input_fields = st.session_state.get('input_fields', {})
            
            st.session_state['current_page'] = 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション: 最初のページに移動 (limit={preserved_limit})")
            st.rerun()
    
    with col3:
        if st.button("◀ 前へ", disabled=(current_page == 1), key="prev_page_btn"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            preserved_input_fields = st.session_state.get('input_fields', {})
            
            st.session_state['current_page'] = current_page - 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション: ページ{current_page-1}に移動 (limit={preserved_limit})")
            st.rerun()
    
    with col4:
        # ページ表示（中央）
        st.markdown(f"<div style='text-align: center; margin: 0; padding: 0;'>"
                   f"<strong>{current_page} / {total_pages}</strong></div>", 
                   unsafe_allow_html=True)
    
    with col5:
        if st.button("次へ ▶", disabled=(current_page == total_pages), key="next_page_btn"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            preserved_input_fields = st.session_state.get('input_fields', {})
            
            st.session_state['current_page'] = current_page + 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション: ページ{current_page+1}に移動 (limit={preserved_limit})")
            st.rerun()
    
    with col6:
        if st.button("最後 ⏩", disabled=(current_page == total_pages), key="last_page_btn"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            preserved_input_fields = st.session_state.get('input_fields', {})
            
            st.session_state['current_page'] = total_pages
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション: 最後のページ{total_pages}に移動 (limit={preserved_limit})")
            st.rerun()


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
        if st.button("⏪ 最初", disabled=(current_page == 1), key="first_page_compact"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            st.session_state['current_page'] = 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション(コンパクト): 最初のページに移動 (limit={preserved_limit})")
            st.rerun()
    
    with col3:
        if st.button("◀ 前へ", disabled=(current_page == 1), key="prev_page_compact"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            st.session_state['current_page'] = current_page - 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション(コンパクト): ページ{current_page-1}に移動 (limit={preserved_limit})")
            st.rerun()
    
    with col4:
        # コンパクトな中央表示（余白最小）
        st.markdown(f"<div style='text-align: center; margin: 0; padding: 0;'>"
                   f"<strong>{current_page} / {total_pages}</strong></div>", 
                   unsafe_allow_html=True)
    
    with col5:
        if st.button("次へ ▶", disabled=(current_page == total_pages), key="next_page_compact"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            st.session_state['current_page'] = current_page + 1
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション(コンパクト): ページ{current_page+1}に移動 (limit={preserved_limit})")
            st.rerun()
    
    with col6:
        if st.button("最後 ⏩", disabled=(current_page == total_pages), key="last_page_compact"):
            # セッション状態を保護
            preserved_limit = st.session_state.get('limit', 20)
            st.session_state['current_page'] = total_pages
            st.session_state['limit'] = preserved_limit  # 念のため保持
            _clear_prepared_csv_artifacts()
            logger.info(f"ページネーション(コンパクト): 最後のページ{total_pages}に移動 (limit={preserved_limit})")
            st.rerun()


def display_table_action_buttons(df_view: pd.DataFrame, input_fields_types: dict, full_df: pd.DataFrame = None) -> None:
    """
    テーブル操作ボタン（クリップボード・CSVダウンロード）を表示
    
    Args:
        df_view (pd.DataFrame): 表示用DataFrame（ページネーション済み）
        input_fields_types (dict): フィールドタイプ辞書
        full_df (pd.DataFrame): 全データ（CSVダウンロード用）
    """
    if df_view.empty:
        return
    
    # CSVダウンロード用のDataFrameを決定（全データ優先）
    download_df = full_df if full_df is not None and not full_df.empty else df_view
    
    # ボタンを分離して干渉を防ぐ
    # 第1行：コピーとダウンロードボタン
    col1, col2, col3 = st.columns([10, 1, 1.5])
    
    with col2:
        # クリップボードコピーボタン（表示中のデータのみ）
        
        try:
            logger.info(f"コピーボタン準備")
            logger.info(f"コピー対象DataFrame shape: {df_view.shape}")
            
            # 現在表示中のDataFrameをTSV形式に変換（Excelに貼り付けやすいタブ区切り）
            csv_data = df_view.to_csv(index=False, sep='\t')
            
            # ライブラリの利用可能性をデバッグ
            logger.info(f"st_copy_to_clipboard利用可能: {st_copy_to_clipboard is not None}")
            logger.info(f"pyperclip利用可能: {pyperclip is not None}")
            
            # Python側のトーストを使う安定版（pyperclip）に戻す
            copy_clicked = st.button(
                "📋 コピー",
                help="表示中のテーブルデータをクリップボードにコピーします",
                key="copy_to_clipboard_isolated"
            )

            if copy_clicked:
                try:
                    # ページ再実行は起こるが絞り込み値はUIから再構築されるため触れない
                    if pyperclip is None:
                        raise RuntimeError("pyperclip が見つかりません。requirements.txt を確認してください。")
                    pyperclip.copy(csv_data)
                    st.toast(f"📋 {len(df_view)}行のテーブルデータをコピーしました！", icon="✅")
                    logger.info("pyperclipでコピー完了")
                except Exception as py_err:
                    logger.error(f"pyperclipコピー失敗: {py_err}")
                    st.error(f"クリップボードコピーに失敗しました: {py_err}")
            
        except Exception as e:
            st.error(f"クリップボードへのコピーに失敗しました: {str(e)}")
            logger.error(f"クリップボードコピーエラー: {str(e)}")
    
    with col3:
        # CSVダウンロードボタン（表示中のデータ or 呼び出し側の full_df）
        target_df = full_df if (full_df is not None and not full_df.empty) else download_df
        display_csv_download_button_isolated(target_df, input_fields_types)

        # 全件ダウンロード（同条件・LIMIT/OFFSETなし）
        sql_file = st.session_state.get('selected_sql_file')
        conditions: Dict[str, Any] = st.session_state.get('input_fields', {})

        if sql_file:
            prepare_clicked = st.button(
                "📥 全件ダウンロードの準備",
                help="同じ条件で全レコードを取得してCSVを準備します（初回のみ時間がかかる場合があります）",
                key="prepare_full_csv"
            )

            if prepare_clicked:
                try:
                    full_df_cached = fetch_full_df_cached(sql_file, conditions)
                    st.session_state['__full_csv_ready__'] = full_df_cached.to_csv(index=False)
                    st.session_state['__full_csv_rows__'] = len(full_df_cached)
                    st.session_state['__full_csv_filename__'] = f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}_all.csv"
                    st.toast(f"全件データを準備しました（{len(full_df_cached)}行）", icon="✅")
                except Exception as e:
                    logger.error(f"全件CSV準備エラー: {e}")
                    st.error("全件CSVの準備に失敗しました")

        if '__full_csv_ready__' in st.session_state:
            st.download_button(
                label="📥 全件ダウンロード",
                data=st.session_state['__full_csv_ready__'].encode('cp932', errors='replace'),
                file_name=st.session_state.get('__full_csv_filename__', f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}_all.csv"),
                mime="text/csv",
                key="csv_download_all"
            )

            # 件数情報を補足表示
            rows = st.session_state.get('__full_csv_rows__')
            if rows is not None:
                st.caption(f"全件CSV: {rows:,} 行")


@st.cache_data(ttl=600, max_entries=64)
def fetch_full_df_cached(sql_file: str, conditions: Dict[str, Any]) -> pd.DataFrame:
    """同条件の全件データをDBから取得（キャッシュ付）"""
    loader = SQLLoader()
    df_full = loader.execute_sql_file(sql_file, conditions=conditions, limit=None)
    if df_full is None:
        return pd.DataFrame()
    return df_full



def display_csv_download_button_isolated(df: pd.DataFrame, input_fields_types: dict) -> None:
    """
    CSVダウンロードボタンを表示（分離版・セッション状態保護）
    
    Args:
        df (pd.DataFrame): ダウンロード対象DataFrame
        input_fields_types (dict): フィールドタイプ辞書
    """
    if df.empty:
        return
    
    # セッション状態を保護
    preserved_limit = st.session_state.get('limit', 20)
    preserved_input_fields = st.session_state.get('input_fields', {}).copy()
    preserved_input_fields_types = st.session_state.get('input_fields_types', {}).copy()
    preserved_options_dict = st.session_state.get('options_dict', {}).copy()
    
    # 二段階方式：準備→ダウンロード（レンダリング時の重処理を回避）
    try:
        ready_key = '__page_csv_ready__'
        rows_key = '__page_csv_rows__'
        fname_key = '__page_csv_filename__'

        if st.button("📄 CSV準備", key="prepare_page_csv"):
            from src.utils.data_processing import prepare_csv_data
            csv_df = prepare_csv_data(df, input_fields_types)
            logger.debug(f"CSV変換前データ型: {csv_df.dtypes.to_dict()}")
            csv_df = csv_df.astype(str)
            st.session_state[ready_key] = csv_df.to_csv(index=False)
            st.session_state[rows_key] = len(csv_df)
            st.session_state[fname_key] = f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            st.toast(f"このページのCSVを準備しました（{len(csv_df)}行）", icon="✅")

        if ready_key in st.session_state:
            download_clicked = st.download_button(
                label="📥 ダウンロード",
                data=st.session_state[ready_key].encode('cp932', errors='replace'),
                file_name=st.session_state.get(fname_key, f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"),
                mime="text/csv",
                key="csv_download_isolated"
            )

            if download_clicked:
                logger.info(f"CSVダウンロード実行 - 条件保護中 (limit={preserved_limit})")
                st.session_state['limit'] = preserved_limit
                st.session_state['input_fields'] = preserved_input_fields
                st.session_state['input_fields_types'] = preserved_input_fields_types
                st.session_state['options_dict'] = preserved_options_dict
                logger.info("CSVダウンロード完了 - セッション状態復元済み")

            rows = st.session_state.get(rows_key)
            if rows is not None:
                st.caption(f"このページCSV: {rows:,} 行")

        logger.debug("CSVダウンロードUI表示完了")

    except Exception as e:
        logger.error(f"CSVダウンロードボタン表示エラー: {e}")
        logger.error(f"エラー詳細: {type(e).__name__}: {str(e)}")
        st.error(f"CSVダウンロードの準備でエラーが発生しました: {type(e).__name__}")
        
        # フォールバック: 基本的なCSVダウンロード
        try:
            logger.info("フォールバック: 基本的なCSVダウンロードを試行")
            simple_csv = df.copy()
            # すべての値を文字列に変換
            simple_csv = simple_csv.astype(str, errors='ignore')
            simple_csv_string = simple_csv.to_csv(index=False)
            
            st.download_button(
                label="📥 ダウンロード（簡易版）",
                data=simple_csv_string.encode('utf-8', errors='replace'),
                file_name=f"data_simple_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="csv_download_fallback"
            )
        except Exception as fallback_error:
            logger.error(f"フォールバックCSVダウンロードもエラー: {fallback_error}")
            st.error("CSVダウンロード機能が利用できません")
        
        # エラー時もセッション状態を復元
        st.session_state['limit'] = preserved_limit
        st.session_state['input_fields'] = preserved_input_fields
        st.session_state['input_fields_types'] = preserved_input_fields_types
        st.session_state['options_dict'] = preserved_options_dict


def display_csv_download_button(df: pd.DataFrame, input_fields_types: dict) -> None:
    """
    CSVダウンロードボタンを表示（レガシー版・互換性維持）
    
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
            label="📥 ダウンロード",
            data=csv_string.encode('cp932', errors='replace'),
            file_name=f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"csv_download_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
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