import logging
from logging.handlers import RotatingFileHandler
import configparser
import os
import sys

def setup_department_logger(name):
    logger = logging.getLogger(name)
    
    # 既にハンドラが設定されているか確認
    if not logger.handlers:
        # 設定ファイルからログ設定を読み込む
        base_dir = os.path.dirname(os.path.abspath(__file__))

        if getattr(sys, 'frozen', False):
            # PyInstallerでビルドされた場合
            base_path = sys._MEIPASS
        else:
            # 通常のPython環境で実行された場合
            base_path = os.path.abspath(".")

        config_file = os.path.join(base_dir, 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')

        # ログレベル、ログファイルのパス、ファイル名を取得
        log_level = config['logging']['level']
        log_file = config['logging']['logfile']

        # ログファイルの完全なパスを生成
        log_file_full_path = os.path.join(log_file)

        # ログのフォーマットを設定
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # ファイルハンドラの設定（ログローテーション対応）
        file_handler = RotatingFileHandler(log_file_full_path, maxBytes=10000000, backupCount=10)
        file_handler.setLevel(logging.getLevelName(log_level))
        file_handler.setFormatter(formatter)

        # コンソールハンドラの設定
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.getLevelName(log_level))
        console_handler.setFormatter(formatter)

        # ハンドラを追加
        logger.setLevel(logging.getLevelName(log_level))
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
