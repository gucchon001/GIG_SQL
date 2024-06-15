import pandas as pd
import streamlit as st
from datetime import datetime
from subcode_streamlit_loader import load_config, load_sql_list, create_dynamic_input_fields, process_and_save_data
from subcode_tkinter_loader import get_sql_file_name, load_sheet_from_spreadsheet, get_filtered_data_from_sheet
from my_logging import setup_department_logger
from io import StringIO
from main_streamlit import execute_sql_file, get_connection

# ロガーの設定
logger = setup_department_logger('main')

# configparserの設定
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# スプレッドシートのシート名を定義
SHEET_NAME = additional_config['main_sheet']
spreadsheet_id = additional_config['spreadsheet_id']
json_keyfile_path = additional_config['json_keyfile_path']

# Streamlitのアプリケーション設定
st.set_page_config(page_title="塾ステ CSVダウンロードツール ストミンくん β版", page_icon=":bar_chart:", layout="wide")

# タイトルとアイコンを配置
col1, col2 = st.columns([1, 6])
with col1:
    st.image("https://icons.iconarchive.com/icons/papirus-team/papirus-apps/512/libreoffice-calc-icon.png", width=100)
with col2:
    st.title("塾ステ CSVダウンロードツール ストミンくん β版")

# サイドバーにSQLファイルリストを表示
sql_files_dict = load_sql_list()
sql_file_display_names = list(sql_files_dict.keys())

# サイドバーで選択されたファイル名が変更された場合、テーブルの表示をリセット
selected_display_name = st.sidebar.radio("SQLファイルを選択してください", sql_file_display_names, on_change=lambda: st.session_state.clear())
selected_sql_file = sql_files_dict[selected_display_name]

# 行数の選択オプション
rows_options = [10, 50, 100, 200, 500, 1000]

#テーブルのスタイル設定
def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

def apply_styles(df, selected_rows):
    # ヘッダ行とデータ行の文字数を制限し、省略表示にする
    df.columns = [truncate_text(col, 20) for col in df.columns]  # ヘッダ行は20文字まで
    df = df.applymap(lambda x: truncate_text(x, 35) if isinstance(x, str) else x)  # データ行は35文字まで
    
    # スタイル設定
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'
    
    styled_df = df.head(selected_rows).style.apply(highlight_header, axis=0).applymap(white_background, subset=pd.IndexSlice[:, :])
    
    return styled_df

# ID列が1始まりになるように調整
def load_and_prepare_data(csv_data, selected_rows):
    df = pd.read_csv(StringIO(csv_data))

    df.index = df.index + 1
    
    styled_df = apply_styles(df, selected_rows)
    
    return styled_df

#
def filter_data():
    input_fields = st.session_state['input_fields']
    input_fields_types = st.session_state['input_fields_types']
    include_header = st.session_state.get('include_header', True)
    conn = get_connection()
    success, csv_data, error_message = process_and_save_data(conn, selected_sql_file, selected_display_name, input_fields, input_fields_types, 'download', include_header, st)

    if success:
        st.session_state['csv_data'] = csv_data
        st.session_state['csv_ready'] = True
        st.session_state['total_records'] = len(csv_data.split('\n')) - 1

if st.button("データの取り込み") or 'csv_data' not in st.session_state:
    logger.info(f"データの取り込みボタンがクリックされました。選択されたSQLファイル: {selected_sql_file}")
    include_header = st.checkbox("ヘッダ行を含める", value=True)
    st.session_state['include_header'] = include_header
    input_fields = st.session_state.get('input_fields', {})
    input_fields_types = st.session_state.get('input_fields_types', {})
    conn = get_connection()
    success, csv_data, error_message = process_and_save_data(conn, selected_sql_file, selected_display_name, input_fields, input_fields_types, 'download', include_header, st)

    
    if success:
        st.session_state['csv_data'] = csv_data
        st.session_state['csv_ready'] = True
        st.session_state['total_records'] = len(csv_data.split('\n')) - 1

if st.session_state.get('csv_ready', False):
    st.session_state['selected_rows'] = st.session_state.get('selected_rows', 10)

    csv_data = st.session_state['csv_data']

    cols_buttons = st.columns([1, 1, 1, 1, 1, 1, 1])
    with cols_buttons[5]:
        if st.button("CSVダウンロード", key="csv_download_button_top"):
            logger.info("CSVダウンロードボタンがクリックされました。")
            st.download_button(
                label="CSVダウンロード",
                data=csv_data.encode('cp932', errors='replace'),
                file_name=f"{selected_sql_file}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    with cols_buttons[6]:
        if st.button("クリップボードコピー", key="clipboard_copy_button_top"):
            logger.info("クリップボードコピーがクリックされました。")
            st.code(csv_data)

    cols_rows = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    with cols_rows[8]:
        st.session_state['selected_rows'] = st.selectbox("行数", rows_options, index=rows_options.index(st.session_state['selected_rows']), key="rows_selectbox_top")
    
    with cols_rows[9]:
        st.write(f"{st.session_state['selected_rows']} / {st.session_state['total_records']}")

    styled_df = load_and_prepare_data(csv_data, st.session_state['selected_rows'])

    st.dataframe(styled_df, use_container_width=True)

if selected_display_name:
    sql_file_name = get_sql_file_name(selected_display_name)
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)
    
    if data:
        input_fields, input_fields_types, options_dict = create_dynamic_input_fields(data)
        st.session_state['input_fields'] = input_fields
        st.session_state['input_fields_types'] = input_fields_types
        st.session_state['options_dict'] = options_dict
        st.button("絞込", on_click=filter_data)
    else:
        st.error("指定されている項目がありません")
