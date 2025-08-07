"""
統一ログ管理機能

アプリケーション全体で使用する標準ログシステムを提供
"""
import logging
from logging.handlers import RotatingFileHandler
import configparser
import os
import sys
from typing import Optional


class LoggerManager:
    """ログ管理クラス"""
    
    _instance = None
    _loggers = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance
    
    def get_logger(self, name: str, config_file: str = "config.ini") -> logging.Logger:
        """
        名前付きロガーを取得
        
        Args:
            name: ロガー名
            config_file: 設定ファイルパス
            
        Returns:
            logging.Logger: 設定済みロガー
        """
        if name not in self._loggers:
            self._loggers[name] = self._create_logger(name, config_file)
        return self._loggers[name]
    
    def _create_logger(self, name: str, config_file: str) -> logging.Logger:
        """
        新しいロガーを作成
        
        Args:
            name: ロガー名
            config_file: 設定ファイルパス
            
        Returns:
            logging.Logger: 新しく作成されたロガー
        """
        logger = logging.getLogger(name)
        
        # 既にハンドラが設定されているか確認
        if logger.handlers:
            return logger
        
        try:
            log_config = self._load_log_config(config_file)
            self._setup_logger(logger, log_config)
            
        except Exception as e:
            # エラーが発生した場合は最低限のコンソール出力設定
            print(f"ログ設定エラー: {e}")
            self._setup_fallback_logger(logger)
        
        return logger
    
    def _load_log_config(self, config_file: str) -> dict:
        """
        ログ設定を読み込み
        
        Args:
            config_file: 設定ファイルパス
            
        Returns:
            dict: ログ設定辞書
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        if getattr(sys, 'frozen', False):
            # PyInstallerでビルドされた場合
            base_path = sys._MEIPASS
        else:
            # 通常のPython環境で実行された場合
            base_path = os.path.abspath(".")
        
        # 設定ファイルのパスを決定
        if not os.path.isabs(config_file):
            config_file = os.path.join(base_dir, '..', '..', '..', config_file)
        
        if os.path.exists(config_file):
            config = configparser.ConfigParser()
            config.read(config_file, encoding='utf-8')
            
            return {
                'level': config.get('logging', 'level', fallback='INFO'),
                'logfile': config.get('logging', 'logfile', fallback='app.log'),
                'max_bytes': config.getint('logging', 'max_bytes', fallback=10000000),
                'backup_count': config.getint('logging', 'backup_count', fallback=10)
            }
        else:
            # config.iniが見つからない場合のデフォルト設定
            return {
                'level': 'INFO',
                'logfile': 'app.log',
                'max_bytes': 10000000,
                'backup_count': 10
            }
    
    def _setup_logger(self, logger: logging.Logger, log_config: dict) -> None:
        """
        ロガーを設定
        
        Args:
            logger: 設定対象のロガー
            log_config: ログ設定辞書
        """
        level = getattr(logging, log_config['level'].upper(), logging.INFO)
        logger.setLevel(level)
        
        # ログフォーマット
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        # ファイルハンドラ（ローテーション対応）
        file_handler = RotatingFileHandler(
            log_config['logfile'],
            maxBytes=log_config['max_bytes'],
            backupCount=log_config['backup_count'],
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        
        # コンソールハンドラ
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        
        # ハンドラを追加
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        logger.info(f"ログ設定完了: logger={logger.name}, level={log_config['level']}, file={log_config['logfile']}")
    
    def _setup_fallback_logger(self, logger: logging.Logger) -> None:
        """
        フォールバック用の最低限ログ設定
        
        Args:
            logger: 設定対象のロガー
        """
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.warning("最低限のコンソール出力設定でログを初期化しました")


# シングルトンインスタンス
_logger_manager = LoggerManager()


def get_logger(name: str, config_file: str = "config.ini") -> logging.Logger:
    """
    名前付きロガーを取得
    
    Args:
        name: ロガー名
        config_file: 設定ファイルパス
        
    Returns:
        logging.Logger: 設定済みロガー
    """
    return _logger_manager.get_logger(name, config_file)


def setup_department_logger(name: str) -> logging.Logger:
    """
    旧式のログ設定関数（後方互換性のため）
    
    Args:
        name: ロガー名
        
    Returns:
        logging.Logger: 設定済みロガー
    """
    return get_logger(name)