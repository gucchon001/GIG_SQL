"""
Streamlit セッション管理モジュール

セッション状態の初期化と管理を担当
"""
import streamlit as st
from typing import Dict, Any, List, Tuple
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def initialize_session_state() -> None:
    """セッション状態を初期化"""
    defaults = {
        'df': None,
        'df_view': None,
        'total_records': 0,
        'current_page': 1,
        'selected_rows': 20,
        'limit': 20,  # 表示件数のデフォルト値
        'sql_files_dict': {},
        'selected_sql_file': None,
        'last_selected_table': None,
        'input_fields': {},
        'input_fields_types': {},
        'options_dict': {},
        'batch_status': '未実行',
        'batch_output': '',
        'selected_data': None
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    logger.debug("セッション状態初期化完了")


def create_dynamic_input_fields(data: List[Dict]) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, List]]:
    """
    動的入力フィールドを作成
    
    Args:
        data (List[Dict]): スプレッドシートデータ
        
    Returns:
        Tuple[Dict, Dict, Dict]: (入力フィールド, フィールドタイプ, オプション辞書)
    """
    input_fields = {}
    input_fields_types = {}
    options_dict = {}
    
    if not data:
        logger.warning("データが空です")
        return input_fields, input_fields_types, options_dict
    
    # データの最初の行からフィールドを特定
    first_row = data[0] if data else {}
    
    for field_name, field_info in first_row.items():
        if not field_name or field_name.startswith('_'):
            continue
            
        # フィールドタイプを判定
        field_type = 'text'  # デフォルト
        if '日付' in field_name or '日時' in field_name:
            field_type = 'date'
        elif 'フラグ' in field_name or field_name.endswith('可'):
            field_type = 'select'
        
        input_fields_types[field_name] = field_type
        
        # セレクトボックス用のオプションを生成
        if field_type == 'select':
            unique_values = list(set([row.get(field_name, '') for row in data if row.get(field_name)]))
            options_dict[field_name] = unique_values
        
        # 初期値設定
        if field_type == 'date':
            input_fields[field_name] = {'start_date': None, 'end_date': None}
        elif field_type == 'select':
            input_fields[field_name] = {opt: False for opt in options_dict.get(field_name, [])}
        else:
            input_fields[field_name] = ''
    
    logger.info(f"動的入力フィールド作成完了: {len(input_fields)}個")
    return input_fields, input_fields_types, options_dict


def truncate_text(text: str, max_length: int = 35) -> str:
    """
    テキストを指定長で切り詰め
    
    Args:
        text (str): 対象テキスト
        max_length (int): 最大長
        
    Returns:
        str: 切り詰め後テキスト
    """
    if not text or len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def calculate_offset(page_number: int, page_size: int) -> int:
    """
    ページネーション用オフセット計算
    
    Args:
        page_number (int): ページ番号
        page_size (int): ページサイズ
        
    Returns:
        int: オフセット値
    """
    return (page_number - 1) * page_size


def on_limit_change() -> None:
    """表示行数変更時の処理"""
    if 'rows_selectbox' in st.session_state:
        st.session_state['selected_rows'] = st.session_state['rows_selectbox']
        st.session_state['current_page'] = 1  # ページを最初にリセット
        logger.debug(f"表示行数変更: {st.session_state['selected_rows']}")


def on_search_click() -> None:
    """検索ボタンクリック時の処理"""
    st.session_state['current_page'] = 1  # ページを最初にリセット
    logger.info("検索実行: ページを1にリセット")


def on_sql_file_change(sql_files_dict: Dict[str, str]) -> None:
    """
    SQLファイル変更時の処理
    
    Args:
        sql_files_dict (Dict[str, str]): SQLファイル辞書
    """
    selected_display_name = st.session_state.get('selected_display_name', '')
    
    if selected_display_name and selected_display_name in sql_files_dict:
        sql_file_name = sql_files_dict[selected_display_name]
        st.session_state['selected_sql_file'] = sql_file_name
        st.session_state['last_selected_table'] = selected_display_name
        
        logger.info(f"SQLファイル変更: {selected_display_name} -> {sql_file_name}")
        
        # 関連データをリセット
        st.session_state['df'] = None
        st.session_state['df_view'] = None
        st.session_state['current_page'] = 1