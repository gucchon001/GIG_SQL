import os
import pandas as pd
import streamlit as st
from config_loader import load_config
from my_logging import setup_department_logger
from ssh_connection import create_ssh_tunnel
from database_connection import create_database_connection
from subcode_streamlit_loader_2 import load_sql_list_from_spreadsheet
from db_utils import execute_sql_query, get_connection

# 設定ファイルの読み込み
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# ロガーの設定
logger = setup_department_logger('create_datasets')

# カレントディレクトリを基準にしたディレクトリパスを作成
output_dir = os.path.join(os.getcwd(), 'data_Parquet')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# SQLファイルリストの取得
sql_files_dict = load_sql_list_from_spreadsheet()

# データをParquet形式で保存する関数
def save_to_parquet(df, output_path):
    try:
        df.to_parquet(output_path, engine='pyarrow', index=False)
        logger.info(f"データをParquet形式で保存しました: {output_path}")
    except Exception as e:
        logger.error(f"Parquet保存中にエラーが発生しました: {e}")

if __name__ == "__main__":
    st.title("SQL Query Execution and Parquet Export")
    
    for display_name, sql_file_name in sql_files_dict.items():
        df = execute_sql_query(sql_file_name, config_file)
        if df is not None:
            st.write(f"Results for {display_name}")
            st.dataframe(df)  # データフレームを表示
            
            # SQLファイル名から拡張子を.parquetに変更
            base_name = os.path.splitext(sql_file_name)[0]
            output_file_path = os.path.join(output_dir, f"{base_name}.parquet")
            save_to_parquet(df, output_file_path)
        else:
            st.error(f"Failed to load data for {display_name}")
