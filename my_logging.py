import logging
from logging.handlers import RotatingFileHandler
import configparser
import os
import sys

def setup_department_logger(name):
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

            config_file = os.path.join(base_dir, 'config.ini')
            config = configparser.ConfigParser()
            
            # config.iniファイルが存在するかチェック
            if os.path.exists(config_file):
                config.read(config_file, encoding='utf-8')
                log_level = config.get('logging', 'level', fallback='DEBUG')
                log_file = config.get('logging', 'logfile', fallback='app.log')
                print(f"config.iniファイルから設定を読み込みました: level={log_level}, logfile={log_file}")
            else:
                # config.iniが見つからない場合のデフォルト設定
                log_level = 'DEBUG'  # DEBUGレベルに変更してより詳細なログを出力
                log_file = 'app.log'
                print(f"config.iniファイルが見つかりません（{config_file}）。デフォルト設定を使用します: level={log_level}, logfile={log_file}")
            
            # 強制的にDEBUGレベルに設定（一時的な措置）
            log_level = 'DEBUG'
            print(f"デバッグのため、ログレベルを強制的にDEBUGに設定: {log_level}")

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
            
            print(f"ログ設定完了: logger={name}, level={log_level}, file={log_file_full_path}")
            print(f"現在の作業ディレクトリ: {os.getcwd()}")
            print(f"ログファイルの絶対パス: {os.path.abspath(log_file_full_path)}")
            print(f"ログファイルが存在するか: {os.path.exists(log_file_full_path)}")
            
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
