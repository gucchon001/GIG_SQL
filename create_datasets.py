import os
import pandas as pd
from config_loader import load_config
from my_logging import setup_department_logger
from subcode_streamlit_loader import load_sql_list_from_spreadsheet
from db_utils import execute_sql_query

# 設定ファイルの読み込み
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# ロガーの設定
LOGGER = setup_department_logger('main')

# カレントディレクトリを基準にしたディレクトリパスを作成
output_dir = os.path.join(os.getcwd(), 'data_Parquet')
if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
        LOGGER.info(f"ディレクトリを作成しました: {output_dir}")
    except Exception as e:
        LOGGER.error(f"ディレクトリ作成中にエラーが発生しました: {e}")

# SQLファイルリストの取得
sql_files_dict = load_sql_list_from_spreadsheet()

# データをParquet形式で保存する関数
def save_to_parquet(df, output_path):
    try:
        df.to_parquet(output_path, engine='pyarrow', index=False)
        LOGGER.info(f"データをParquet形式で保存しました: {output_path}")
    except Exception as e:
        LOGGER.error(f"Parquet保存中にエラーが発生しました: {e}")

if __name__ == "__main__":
    for display_name, sql_file_name in sql_files_dict.items():
        df = execute_sql_query(sql_file_name, config_file)
        if df is not None:
            LOGGER.info(f"Results for {display_name}")
            
            # SQLファイル名から拡張子を.parquetに変更
            base_name = os.path.splitext(sql_file_name)[0]
            output_file_path = os.path.join(output_dir, f"{base_name}.parquet")
            save_to_parquet(df, output_file_path)
        else:
            LOGGER.error(f"Failed to load data for {display_name}")