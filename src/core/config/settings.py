"""
設定管理機能

アプリケーション全体の設定を管理するモジュール
"""
from dataclasses import dataclass
from typing import Dict, Optional
import configparser
import os


@dataclass
class SSHConfig:
    """SSH接続設定"""
    host: str
    user: str
    ssh_key_path: str
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    local_port: int = 3306


@dataclass
class DatabaseConfig:
    """データベース接続設定"""
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class GoogleAPIConfig:
    """Google API設定"""
    credentials_file: str
    spreadsheet_id: str
    drive_folder_id: str
    main_sheet: str
    rawdata_sheet: str
    eachdata_sheet: str


@dataclass
class PathsConfig:
    """ファイルパス設定"""
    csv_base_path: str
    config_file: str


@dataclass
class TuningConfig:
    """パフォーマンス調整設定"""
    chunk_size: int = 10000
    batch_size: int = 1000
    delay: float = 0.1
    max_workers: int = 5


@dataclass
class LoggingConfig:
    """ログ設定"""
    level: str = "DEBUG"
    logfile: str = "app.log"


@dataclass
class AppConfig:
    """アプリケーション設定の統合クラス"""
    environment: str
    debug: bool
    ssh: SSHConfig
    database: DatabaseConfig
    google_api: GoogleAPIConfig
    paths: PathsConfig
    tuning: TuningConfig
    logging: LoggingConfig
    
    @classmethod
    def from_config_file(cls, config_file: str = "config.ini") -> 'AppConfig':
        """
        設定ファイルからアプリケーション設定を読み込み
        
        Args:
            config_file: 設定ファイルのパス
            
        Returns:
            AppConfig: アプリケーション設定オブジェクト
            
        Raises:
            FileNotFoundError: 設定ファイルが見つからない場合
            configparser.Error: 設定ファイル読み込みエラー
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"設定ファイルが見つかりません: {config_file}")
            
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')
        
        # SSH設定
        ssh_config = SSHConfig(
            host=config['SSH']['host'],
            user=config['SSH']['user'],
            ssh_key_path=config['SSH']['ssh_key_path'],
            local_port=3306  # 固定値または設定ファイルから読み込み
        )
        
        # データベース設定
        db_config = DatabaseConfig(
            host=config['MySQL']['host'],
            port=int(config['MySQL']['port']),
            user=config['MySQL']['user'],
            password=config['MySQL']['password'],
            database=config['MySQL']['database']
        )
        
        # Google API設定
        google_config = GoogleAPIConfig(
            credentials_file=config['Credentials']['json_keyfile_path'],
            spreadsheet_id=config['Spreadsheet']['spreadsheet_id'],
            drive_folder_id=config['GoogleDrive']['google_folder_id'],
            main_sheet=config['Spreadsheet']['main_sheet'],
            rawdata_sheet=config['Spreadsheet']['rawdata_sheet'],
            eachdata_sheet=config['Spreadsheet']['eachdata_sheet']
        )
        
        # パス設定
        paths_config = PathsConfig(
            csv_base_path=config['Paths']['csv_base_path'],
            config_file=config_file
        )
        
        # パフォーマンス調整設定
        tuning_config = TuningConfig(
            chunk_size=int(config['Tuning']['chunk_size']),
            batch_size=int(config['Tuning']['batch_size']),
            delay=float(config['Tuning']['delay']),
            max_workers=int(config['Tuning']['max_workers'])
        )
        
        # ログ設定
        logging_config = LoggingConfig(
            level=config.get('logging', 'level', fallback='DEBUG'),
            logfile=config.get('logging', 'logfile', fallback='app.log')
        )
        
        return cls(
            environment=os.getenv('APP_ENV', 'development'),
            debug=os.getenv('DEBUG', 'False').lower() == 'true',
            ssh=ssh_config,
            database=db_config,
            google_api=google_config,
            paths=paths_config,
            tuning=tuning_config,
            logging=logging_config
        )


def load_config(config_file: str = "config.ini") -> tuple:
    """
    旧式の設定読み込み関数（後方互換性のため）
    
    Args:
        config_file: 設定ファイルのパス
        
    Returns:
        tuple: (ssh_config, db_config, local_port, additional_config)
    """
    app_config = AppConfig.from_config_file(config_file)
    
    # 旧形式の辞書に変換
    ssh_config = {
        'host': app_config.ssh.host,
        'user': app_config.ssh.user,
        'ssh_key_path': app_config.ssh.ssh_key_path,
    }
    
    db_config = {
        'host': app_config.database.host,
        'port': app_config.database.port,
        'user': app_config.database.user,
        'password': app_config.database.password,
        'database': app_config.database.database,
    }
    
    additional_config = {
        'spreadsheet_id': app_config.google_api.spreadsheet_id,
        'main_sheet': app_config.google_api.main_sheet,
        'rawdata_sheet': app_config.google_api.rawdata_sheet,
        'eachdata_sheet': app_config.google_api.eachdata_sheet,
        'json_keyfile_path': app_config.google_api.credentials_file,
        'csv_base_path': app_config.paths.csv_base_path,
        'google_folder_id': app_config.google_api.drive_folder_id,
        'chunk_size': app_config.tuning.chunk_size,
        'batch_size': app_config.tuning.batch_size,
        'delay': app_config.tuning.delay,
        'max_workers': app_config.tuning.max_workers,
        'config_file': config_file,
    }
    
    return ssh_config, db_config, app_config.ssh.local_port, additional_config