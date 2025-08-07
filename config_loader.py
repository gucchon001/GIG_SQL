"""
設定読み込みモジュール（後方互換性）

新構造（src.core.config.settings）をベースにしつつ、
旧インターフェースとの互換性を保持
"""
import configparser
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    # 新構造の設定管理を優先使用
    from src.core.config.settings import AppConfig
    USE_NEW_CONFIG = True
except ImportError:
    # フォールバック：旧構造のみ使用
    USE_NEW_CONFIG = False
    print("Warning: 新構造の設定管理が見つかりません。旧構造を使用します。")

def load_config(config_file):
    """
    指定された設定ファイルから設定を読み込み、設定値を含む辞書を返します。
    
    新構造がある場合はそちらを優先使用し、フォールバックとして旧構造を使用。

    :param config_file: 設定ファイルのパス
    :return: 設定値を含む辞書
    """
    if USE_NEW_CONFIG:
        try:
            # 新構造の設定管理を使用
            app_config = AppConfig.from_config_file(config_file)
            
            # 旧インターフェースに合わせて変換
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

            local_port = app_config.ssh.local_port

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

            return ssh_config, db_config, local_port, additional_config
            
        except Exception as e:
            print(f"新構造設定読み込みエラー: {e}. 旧構造にフォールバック。")
    
    # フォールバック：旧構造での設定読み込み
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    
    # SSH接続設定の読み込み
    ssh_config = {
        'host': config['SSH']['host'],
        'user': config['SSH']['user'],
        'ssh_key_path': config['SSH']['ssh_key_path'],
    }

    # MySQL接続設定の読み込み
    db_config = {
        'host': config['MySQL']['host'],
        'port': int(config['MySQL']['port']),
        'user': config['MySQL']['user'],
        'password': config['MySQL']['password'],
        'database': config['MySQL']['database'],
    }

    local_port = 3306  # ローカルポートの設定（固定値または設定ファイルから読み込む）

    # 追加の設定値の読み込み
    additional_config = {
        'spreadsheet_id': config['Spreadsheet']['spreadsheet_id'],
        'main_sheet': config['Spreadsheet']['main_sheet'], 
        'rawdata_sheet': config['Spreadsheet']['rawdata_sheet'], 
        'eachdata_sheet': config['Spreadsheet']['eachdata_sheet'], 
        'json_keyfile_path': config['Credentials']['json_keyfile_path'],
        'csv_base_path': config['Paths']['csv_base_path'],
        'google_folder_id': config['GoogleDrive']['google_folder_id'],
        'chunk_size': int(config['Tuning']['chunk_size']),
        'batch_size': int(config['Tuning']['batch_size']),
        'delay': float(config['Tuning']['delay']),
        'max_workers': int(config['Tuning']['max_workers']),
        'config_file': config_file, 
    }

    return ssh_config, db_config, local_port, additional_config