import streamlit as st

# Streamlitのアプリケーション設定
st.set_page_config(page_title="塾ステ CSVダウンロードツール ストミンくん β版", page_icon=":bar_chart:", layout="wide")

#from top_dashboard import Dashboard
from csv_download import csv_download
from subcode_streamlit_loader import load_sql_list_from_spreadsheet
from my_logging import setup_department_logger

# ロガーの設定
LOGGER = setup_department_logger('main')

# サイドバーのタイトルを小さくするためのCSSスタイル
sidebar_header = """
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
<div style="display: flex; align-items: flex-start;">
    <h3>塾ステ CSVダウンロードツール<br>ストミンくん β版</h3>
</div>
"""
st.sidebar.markdown(sidebar_header, unsafe_allow_html=True)

# 親階層の選択肢
parent_options = ["CSVダウンロード", "ダッシュボード（開発中）"]
selected_parent = st.sidebar.radio("メインメニュー", parent_options, index=0, key="parent_radio")

# CSVダウンロードページが選択された時にSQLファイル一覧を取得
if selected_parent == "CSVダウンロード":
    if 'sql_files_dict' not in st.session_state:
        st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()
    sql_files_dict = st.session_state['sql_files_dict']
    sql_file_display_names = list(sql_files_dict.keys())

    # 子階層の選択肢
    child_options = sql_file_display_names
else:  # ダッシュボード
    child_options = ["TOP"]

selected_child = st.sidebar.radio("サブメニュー", child_options, key="child_radio")

# 区切り線
st.sidebar.markdown("---")

# リスト再読み込みボタンを追加（CSVダウンロードが選択されているときのみ）
if selected_parent == "CSVダウンロード":
    if st.sidebar.button("リスト再読み込み"):
        load_sql_list_from_spreadsheet.clear()  # キャッシュをクリア
        st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()
        st.experimental_rerun()

# 選択に応じてCSVファイルまたはダッシュボードを表示
if selected_parent == "CSVダウンロード":
    LOGGER.info(f"Calling csv_download function with {selected_child}")
    csv_download(selected_child)
    LOGGER.info("CSV download function call completed")
else:  # ダッシュボード
    LOGGER.info("Calling dashboard function")
    from bi_main import main as run_dashboard
    run_dashboard()
    LOGGER.info("Dashboard function call completed")