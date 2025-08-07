"""
Streamlit UI コンポーネント

再利用可能なUIコンポーネントを提供
"""
import streamlit as st
import os
from typing import Optional, Dict, Any


class StreamlitUI:
    """Streamlit UI コンポーネント管理クラス"""
    
    def __init__(self):
        """UI管理クラスを初期化"""
        self._load_custom_css()
    
    def _load_custom_css(self) -> None:
        """カスタムCSSを読み込み"""
        css_styles = """
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
        .main-title {
            font-size: 2.5rem;
            color: #1e3a8a;
            text-align: center;
            margin-bottom: 2rem;
        }
        .section-header {
            font-size: 1.5rem;
            color: #3b82f6;
            border-bottom: 2px solid #e5e7eb;
            padding-bottom: 0.5rem;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        .status-success {
            background-color: #dcfce7;
            border: 1px solid #16a34a;
            border-radius: 0.5rem;
            padding: 1rem;
            color: #15803d;
        }
        .status-error {
            background-color: #fef2f2;
            border: 1px solid #dc2626;
            border-radius: 0.5rem;
            padding: 1rem;
            color: #dc2626;
        }
        .status-running {
            background-color: #fef3c7;
            border: 1px solid #f59e0b;
            border-radius: 0.5rem;
            padding: 1rem;
            color: #d97706;
        }
        .metric-card {
            background-color: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 0.5rem;
            padding: 1rem;
            text-align: center;
        }
        </style>
        """
        st.markdown(css_styles, unsafe_allow_html=True)
    
    def render_sidebar(self) -> None:
        """サイドバーをレンダリング"""
        sidebar_header = """
        <div style="display: flex; align-items: flex-start;">
            <h3>塾ステ CSVダウンロードツール<br>ストミンくん β版</h3>
        </div>
        """
        st.sidebar.markdown(sidebar_header, unsafe_allow_html=True)
        
        # バージョン情報
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Version:** 1.0.0")
        st.sidebar.markdown("**Status:** β版")
    
    def render_main_title(self, title: str, subtitle: Optional[str] = None) -> None:
        """メインタイトルをレンダリング"""
        st.markdown(f'<h1 class="main-title">{title}</h1>', unsafe_allow_html=True)
        if subtitle:
            st.markdown(f'<p style="text-align: center; color: #6b7280;">{subtitle}</p>', unsafe_allow_html=True)
    
    def render_section_header(self, title: str, icon: str = "") -> None:
        """セクションヘッダーをレンダリング"""
        header_text = f"{icon} {title}" if icon else title
        st.markdown(f'<h2 class="section-header">{header_text}</h2>', unsafe_allow_html=True)
    
    def render_status_card(self, status: str, message: str) -> None:
        """ステータスカードをレンダリング"""
        status_classes = {
            "success": "status-success",
            "error": "status-error", 
            "running": "status-running",
            "pending": "status-pending"
        }
        
        css_class = status_classes.get(status, "status-pending")
        st.markdown(
            f'<div class="{css_class}">{message}</div>',
            unsafe_allow_html=True
        )
    
    def render_metric_cards(self, metrics: Dict[str, Any]) -> None:
        """メトリクスカードをレンダリング"""
        cols = st.columns(len(metrics))
        
        for i, (label, value) in enumerate(metrics.items()):
            with cols[i]:
                st.markdown(
                    f'''
                    <div class="metric-card">
                        <h3 style="margin: 0; color: #374151;">{label}</h3>
                        <p style="margin: 0; font-size: 1.5rem; font-weight: bold; color: #1f2937;">{value}</p>
                    </div>
                    ''',
                    unsafe_allow_html=True
                )
    
    def render_data_table(self, data, title: str = "データ表示", height: int = 400) -> None:
        """データテーブルをレンダリング"""
        if data is not None and not data.empty:
            self.render_section_header(title, "📊")
            
            # メトリクス表示
            metrics = {
                "行数": f"{len(data):,}",
                "列数": len(data.columns),
                "メモリ": f"{data.memory_usage(deep=True).sum() / 1024**2:.1f} MB"
            }
            self.render_metric_cards(metrics)
            
            # テーブル表示
            st.dataframe(
                data,
                use_container_width=True,
                height=height
            )
            
            # データ型情報（展開可能）
            with st.expander("📋 データ型情報"):
                import pandas as pd
                dtype_info = pd.DataFrame({
                    'カラム名': data.columns,
                    'データ型': [str(dtype) for dtype in data.dtypes],
                    'Non-Null数': [data[col].count() for col in data.columns],
                    'Null数': [data[col].isnull().sum() for col in data.columns]
                })
                st.dataframe(dtype_info, use_container_width=True)
        else:
            st.warning("表示するデータがありません")
    
    def render_file_selector(
        self,
        files: list,
        label: str = "ファイルを選択",
        key: str = "file_selector"
    ) -> Optional[str]:
        """ファイルセレクターをレンダリング"""
        if files:
            return st.selectbox(label, options=files, key=key)
        else:
            st.warning("利用可能なファイルがありません")
            return None
    
    def render_action_buttons(
        self,
        buttons: Dict[str, Dict[str, Any]],
        columns: int = 2
    ) -> Dict[str, bool]:
        """アクションボタンをレンダリング"""
        cols = st.columns(columns)
        button_states = {}
        
        for i, (button_id, config) in enumerate(buttons.items()):
            with cols[i % columns]:
                button_states[button_id] = st.button(
                    config.get('label', button_id),
                    type=config.get('type', 'secondary'),
                    key=config.get('key', button_id),
                    help=config.get('help', None),
                    disabled=config.get('disabled', False)
                )
        
        return button_states
    
    def render_progress_bar(self, progress: float, text: str = "") -> None:
        """プログレスバーをレンダリング"""
        st.progress(progress, text=text)
    
    def render_download_button(
        self,
        data: str,
        filename: str,
        mime_type: str = "text/csv",
        label: str = "📥 ダウンロード"
    ) -> None:
        """ダウンロードボタンをレンダリング"""
        st.download_button(
            label=label,
            data=data,
            file_name=filename,
            mime=mime_type
        )