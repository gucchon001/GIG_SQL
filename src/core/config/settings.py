"""
設定管理機能

アプリケーション全体の設定を管理するモジュール
設定の分離：
- settings.ini: 一般設定（非秘匿）
- secrets.env: 秘匿情報（パスワード、APIキー等）
"""
from dataclasses import dataclass
from typing import Dict, Optional
import configparser
import os
from dotenv import load_dotenv


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
class SlackConfig:
    """Slack通知設定"""
    webhook_url: str
    bot_name: str
    user_id: str
    icon_emoji: str


@dataclass
class BatchConfig:
    """バッチ実行設定"""
    create_datasets: str
    create_datasets_individual: str


@dataclass
class CSVConfig:
    """CSV処理設定"""
    csv_file_paths: str


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
    slack: SlackConfig
    batch: BatchConfig
    csv: CSVConfig
    
    @classmethod
    def from_config_file(cls, config_file: str = "config/settings.ini") -> 'AppConfig':
        """
        設定ファイルからアプリケーション設定を読み込み
        優先順位: 1) 新構造 2) 旧構造（後方互換性）
        """
        # 新構造での設定読み込みを試行
        new_config_path = "config/settings.ini"
        secrets_path = "config/secrets.env"
        
        if os.path.exists(new_config_path) and os.path.exists(secrets_path):
            return cls._load_from_new_structure(new_config_path, secrets_path)
        else:
            # フォールバック: 旧構造
            return cls._load_from_legacy_structure(config_file)
    
    @classmethod
    def _load_from_new_structure(cls, settings_file: str, secrets_file: str) -> 'AppConfig':
        """
        新構造から設定を読み込み（settings.ini + secrets.env）
        """
        # 秘匿情報を環境変数に読み込み
        load_dotenv(secrets_file)
        
        # 一般設定を読み込み
        config = configparser.ConfigParser()
        config.read(settings_file, encoding='utf-8')
        
        return cls._parse_config(config, is_new_structure=True)
    
    @classmethod
    def _load_from_legacy_structure(cls, config_file: str) -> 'AppConfig':
        """
        旧構造（単一config.ini）から設定を読み込み
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"設定ファイルが見つかりません: {config_file}")
            
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')
        
        return cls._parse_config(config, is_new_structure=False)
    
    @classmethod
    def _parse_config(cls, config: configparser.ConfigParser, is_new_structure: bool = True) -> 'AppConfig':
        """
        設定を解析してAppConfigオブジェクトを作成
        """
        # 環境変数を読み込み（secrets.env）
        secrets_path = os.path.join(os.getcwd(), 'config', 'secrets.env')
        load_dotenv(secrets_path)
        
        # SSH設定
        if is_new_structure:
            ssh_key_path = os.getenv('SSH_KEY_PATH', '')
        else:
            ssh_key_path = config['SSH']['ssh_key_path']
            
        ssh_config = SSHConfig(
            host=config['SSH']['host'],
            user=config['SSH']['user'],
            ssh_key_path=ssh_key_path,
            local_port=3306
        )
        
        # データベース設定
        if is_new_structure:
            mysql_password = os.getenv('MYSQL_PASSWORD', '')
        else:
            mysql_password = config['MySQL']['password']
            
        db_config = DatabaseConfig(
            host=config['MySQL']['host'],
            port=int(config['MySQL']['port']),
            user=config['MySQL']['user'],
            password=mysql_password,
            database=config['MySQL']['database']
        )
        
        # Google API設定
        if is_new_structure:
            credentials_file = os.getenv('JSON_KEYFILE_PATH', '')
        else:
            credentials_file = os.getenv('JSON_KEYFILE_PATH', config.get('Credentials', {}).get('json_keyfile_path', ''))
            
        google_config = GoogleAPIConfig(
            credentials_file=credentials_file,
            spreadsheet_id=config['Spreadsheet']['spreadsheet_id'],
            drive_folder_id=config['GoogleDrive']['google_folder_id'],
            main_sheet=config['Spreadsheet']['main_sheet'],
            rawdata_sheet=config['Spreadsheet'].get('rawdata_sheet', 'rawdataシート'),
            eachdata_sheet=config['Spreadsheet']['eachdata_sheet']
        )
        
        # パス設定
        if is_new_structure:
            config_file_path = "config/settings.ini"
        else:
            config_file_path = "config/settings.ini"
            
        paths_config = PathsConfig(
            csv_base_path=config['Paths']['csv_base_path'],
            config_file=config_file_path
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
        
        # Slack設定
        if is_new_structure:
            webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
        else:
            webhook_url = config.get('Slack', 'SLACK_WEBHOOK_URL', fallback='')
            
        slack_config = SlackConfig(
            webhook_url=webhook_url,
            bot_name=config.get('Slack', 'BOT_NAME', fallback='CSVダウンロードツール'),
            user_id=config.get('Slack', 'USER_ID', fallback=''),
            icon_emoji=config.get('Slack', 'ICON_EMOJI', fallback=':ラップトップ:')
        )
        
        # バッチ設定
        batch_config = BatchConfig(
            create_datasets=config.get('batch_exe', 'create_datasets', fallback=''),
            create_datasets_individual=config.get('batch_exe', 'create_datasets_individual', fallback='')
        )
        
        # CSV設定
        csv_config = CSVConfig(
            csv_file_paths=config.get('CSV', 'csv_file_paths', fallback='')
        )
        
        return cls(
            environment=os.getenv('APP_ENV', 'development'),
            debug=os.getenv('DEBUG', 'False').lower() == 'true',
            ssh=ssh_config,
            database=db_config,
            google_api=google_config,
            paths=paths_config,
            tuning=tuning_config,
            logging=logging_config,
            slack=slack_config,
            batch=batch_config,
            csv=csv_config
        )


def load_config(config_file: str = "config/settings.ini") -> tuple:
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
        # Slack設定
        'slack_webhook_url': app_config.slack.webhook_url,
        'slack_bot_name': app_config.slack.bot_name,
        'slack_user_id': app_config.slack.user_id,
        'slack_icon_emoji': app_config.slack.icon_emoji,
        # バッチ設定
        'create_datasets': app_config.batch.create_datasets,
        'create_datasets_individual': app_config.batch.create_datasets_individual,
        # CSV設定
        'csv_file_paths': app_config.csv.csv_file_paths,
    }
    
    return ssh_config, db_config, app_config.ssh.local_port, additional_config