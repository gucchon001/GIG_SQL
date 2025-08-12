import logging
from logging.handlers import RotatingFileHandler
import configparser
import os
import sys

def setup_department_logger(name, app_type=None):
    logger = logging.getLogger(name)
    
    # 既にハンドラが設定されているか確認
    if not logger.handlers:
        try:
            # 設定ファイルからログ設定を読み込む
            base_dir = os.path.dirname(os.path.abspath(__file__))

            if getattr(sys, 'frozen', False):
                # PyInstallerでビルドされた場合
                base_path = sys._MEIPASS
            else:
                # 通常のPython環境で実行された場合
                base_path = os.path.abspath(".")

            # 新構造の設定ファイルをチェック
            config_file = os.path.join(os.path.dirname(base_dir), 'config', 'settings.ini')
            config = configparser.ConfigParser()
            
            # settings.iniファイルが存在するかチェック
            if os.path.exists(config_file):
                config.read(config_file, encoding='utf-8')
                log_level = config.get('logging', 'level', fallback='INFO')
            else:
                # デフォルト設定
                log_level = 'INFO'

            # アプリケーション別ログファイル名を決定
            if app_type == 'streamlit':
                log_file = 'logs/streamlit.log'
            elif app_type == 'main':
                log_file = 'logs/main.log'
            elif app_type == 'datasets':
                log_file = 'logs/datasets.log'
            else:
                log_file = 'logs/app.log'  # デフォルト

            # logsディレクトリを作成（存在しない場合）
            os.makedirs('logs', exist_ok=True)

            # ログファイルの完全なパスを生成
            log_file_full_path = os.path.join(log_file)

            # ログのフォーマットを設定
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

            # ファイルハンドラの設定（ログローテーション対応）
            file_handler = RotatingFileHandler(
                log_file_full_path, 
                maxBytes=10000000, 
                backupCount=10,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.getLevelName(log_level))
            file_handler.setFormatter(formatter)

            # コンソールハンドラの設定
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.getLevelName(log_level))
            console_handler.setFormatter(formatter)
            
            # コンソール出力のエンコーディングを設定
            if hasattr(console_handler.stream, 'reconfigure'):
                console_handler.stream.reconfigure(encoding='utf-8')

            # ハンドラを追加
            logger.setLevel(logging.getLevelName(log_level))
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            
            print(f"ログ設定完了: logger={name}, level={log_level}, file={log_file}")
            
        except Exception as e:
            # エラーが発生した場合は最低限のコンソール出力設定
            print(f"ログ設定エラー: {e}")
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.setLevel(logging.INFO)
            logger.addHandler(console_handler)
            print("最低限のコンソール出力設定でログを初期化しました")

    return logger
