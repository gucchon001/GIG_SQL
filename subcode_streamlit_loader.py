import streamlit as st
import pandas as pd
from datetime import date, datetime
import configparser
from subcode_tkinter_loader import load_sql_list_from_spreadsheet, get_sql_file_name, load_sheet_from_spreadsheet, get_filtered_data_from_sheet
from main_streamlit import execute_sql_file
from subcode_loader import load_sql_from_file, add_conditions_to_sql
from my_logging import setup_department_logger

logger = setup_department_logger('main')

from config_loader import load_config 

config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# SQLファイルリストの読み込み
def load_sql_list():
    sql_files_dict = load_sql_list_from_spreadsheet()
    return sql_files_dict

# 入力フィールドの生成
def create_dynamic_input_fields(data):
    input_fields = {}
    input_fields_types = {}
    options_dict = {}

    if not data:
        st.error("指定されている項目がありません")
        return input_fields, input_fields_types, options_dict

    num_columns = 3
    num_items = len(data)
    items_per_column = (num_items + num_columns - 1) // num_columns

    columns = st.columns(num_columns)

    for i, item in enumerate(data):
        column_index = i // items_per_column
        with columns[column_index]:
            label_text = item['db_item']

            if item['input_type'] == 'FA':
                input_fields[item['db_item']] = st.text_input(label_text, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'FA'

            elif item['input_type'] == 'プルダウン':
                options = ['-'] + [option[1] for option in item['options']]
                input_fields[item['db_item']] = st.selectbox(label_text, options, key=f"input_{item['db_item']}")
                input_fields_types[item['db_item']] = 'プルダウン'
                options_dict[item['db_item']] = item['options']

            elif item['input_type'] == 'ラジオボタン':
                options = [option[1] for option in item['options']]
                radio_index = st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")
                input_fields[item['db_item']] = options[radio_index] if radio_index != None else None
                input_fields_types[item['db_item']] = 'ラジオボタン'
                options_dict[item['db_item']] = item['options']
    
                clear_radio = st.checkbox("選択肢を外す", key=f"clear_radio_{item['db_item']}")
                if clear_radio:
                    input_fields[item['db_item']] = None
                    st.radio(label_text, range(len(options)), format_func=lambda i: options[i], index=None, key=f"radio_{item['db_item']}")

            elif item['input_type'] == 'チェックボックス':
                checkbox_values = {}
                for option in item['options']:
                    checkbox_values[option[0]] = st.checkbox(option[1])
                input_fields[item['db_item']] = checkbox_values
                input_fields_types[item['db_item']] = 'チェックボックス'

            elif item['input_type'] == 'Date':
                start_date = st.date_input(f"開始日", value=date.today())
                end_date = st.date_input(f"終了日", value=date.today())
                input_fields[item['db_item']] = {'start_date': start_date, 'end_date': end_date}
                input_fields_types[item['db_item']] = 'Date'

    st.session_state['input_fields'] = input_fields
    st.session_state['input_fields_types'] = input_fields_types
    st.session_state['options_dict'] = options_dict

    return input_fields, input_fields_types, options_dict

# データの処理と保存
def process_and_save_data(conn, selected_sql_file, selected_display_name, input_fields, input_fields_types, action, include_header, st):
    try:
        if action == 'display':
            # テーブル表示用にSQLファイルを読み込む
            sql_query = load_sql_from_file(selected_sql_file, additional_config['google_folder_id'], additional_config['json_keyfile_path'])
            if sql_query:
                # 入力データに基づいてSQL文に条件を追加
                sql_query_with_conditions = add_conditions_to_sql(sql_query, input_fields, input_fields_types, None, skip_deletion_exclusion=True)
                # テーブル表示用にLIMIT 10を追加
                sql_query_with_conditions_limited = sql_query_with_conditions + ' LIMIT 10'
                logger.info('sql_query_with_conditions_limited:')
                logger.info(sql_query_with_conditions_limited)
                # SQLクエリを実行してデータフレームを取得
                df = pd.read_sql(sql_query_with_conditions_limited, conn)
                # レコード数の計算
                record_count = pd.read_sql(sql_query_with_conditions, conn).shape[0]
                return True, df, None, record_count
            else:
                error_message = f"SQLファイル {selected_sql_file} の読み込みに失敗しました。"
                logger.error(error_message)
                return False, None, error_message, 0
        else:
            success, csv_data, error_message = execute_sql_file(selected_sql_file, input_fields, input_fields_types, action, include_header)
            return success, csv_data, error_message
    except Exception as e:
        st.error(f"予期しないエラーが発生しました: {e}")
        return False, None, str(e), None