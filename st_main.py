import os
import threading
import time
import traceback
from config_loader import load_config
from my_logging import setup_department_logger
import pandas as pd

# ロガーの設定
logger = setup_department_logger('main')

# グローバル変数
last_activity_time = time.time()
config_file = 'config.ini'
ssh_config, db_config, local_port, additional_config = load_config(config_file)

# 接続を管理するバックグラウンドスレッド
def manage_connections():
    global last_activity_time

    while True:
        current_time = time.time()
        if current_time - last_activity_time > 1800:  # 30分
            logger.info("接続は正常です。")
        time.sleep(60)

# 背景で接続を管理するスレッドを開始
connection_thread = threading.Thread(target=manage_connections, daemon=True)
connection_thread.start()

# Parquetファイルを読み込み、条件を追加して準備する関数
def load_and_prepare_data(parquet_file_name, input_fields, input_fields_types):
    try:
        # Parquetファイルの内容を読み込む
        df = pd.read_parquet(parquet_file_name)
        if df is not None:
            # 入力フィールドに基づいてフィルタリングを実行
            for field, value in input_fields.items():
                if value and value != "-":
                    if isinstance(value, dict) and 'start_date' in value and 'end_date' in value:
                        start_date = value['start_date']
                        end_date = value['end_date']
                        df = df[(df[field] >= start_date) & (df[field] <= end_date)]
                    elif isinstance(value, dict):
                        for option, selected in value.items():
                            if selected:
                                df = df[df[field] == option]
                    else:
                        df = df[df[field] == value]
            
            return df
        else:
            error_message = f"Parquetファイル {parquet_file_name} の読み込みに失敗しました。"
            logger.error(error_message)
            return None
    except Exception as e:
        error_message = f"Parquetファイルの読み込み中にエラーが発生しました: {e}\n{traceback.format_exc()}"
        logger.error(error_message)
        return None
