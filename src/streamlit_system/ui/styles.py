"""
Streamlit スタイリングモジュール

CSS読み込みとスタイル適用
"""
import streamlit as st
import os
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def load_css(file_name: str) -> None:
    """
    CSSファイルを読み込んでStreamlitに適用
    
    Args:
        file_name (str): CSSファイル名
    """
    try:
        if os.path.exists(file_name):
            with open(file_name, 'r', encoding='utf-8') as f:
                css_content = f.read()
                st.markdown(f'<style>{css_content}</style>', unsafe_allow_html=True)
            logger.info(f"CSSファイル '{file_name}' を正常に読み込みました。")
        else:
            logger.warning(f"CSSファイル '{file_name}' が見つかりませんでした。")
    except FileNotFoundError:
        logger.error(f"CSSファイル '{file_name}' が見つかりませんでした。")
        st.error(f"CSSファイル '{file_name}' が見つかりませんでした。")
    except Exception as e:
        logger.error(f"CSSファイル読み込みエラー: {e}")


def apply_sidebar_styles() -> None:
    """サイドバー用のスタイルを適用"""
    sidebar_style = """
    <style>
    .sidebar .markdown-text-container h3 {
        font-size: 12px;
        margin: 0;
        padding: 0;
        line-height: 1.2;
        text-align: left;
    }
    .stRadio > div {
        flex-direction: column;
    }
    </style>
    """
    st.markdown(sidebar_style, unsafe_allow_html=True)


def apply_table_styles() -> None:
    """テーブル用のスタイルを適用"""
    table_style = """
    <style>
    .stDataFrame {
        width: 100%;
    }
    .stDataFrame table {
        font-size: 12px;
    }
    .stDataFrame th {
        background-color: #f0f2f6;
        font-weight: bold;
    }
    </style>
    """
    st.markdown(table_style, unsafe_allow_html=True)


def apply_custom_styles() -> None:
    """カスタムスタイルを一括適用"""
    custom_style = """
    <style>
    /* メインコンテンツのスタイリング */
    .main .block-container {
        padding-top: 2rem;
    }
    
    /* ボタンのスタイリング */
    .stButton > button {
        border-radius: 5px;
        border: 1px solid #cccccc;
        background-color: #ffffff;
    }
    
    .stButton > button:hover {
        background-color: #f0f2f6;
        border-color: #999999;
    }
    
    /* メトリクスのスタイリング */
    .metric-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
    }
    
    /* エラーメッセージのスタイリング */
    .stAlert {
        margin: 1rem 0;
    }
    </style>
    """
    st.markdown(custom_style, unsafe_allow_html=True)