import configparser

def load_config(config_file):
    """
    指定された設定ファイルから設定を読み込み、設定値を含む辞書を返します。

    :param config_file: 設定ファイルのパス
    :return: 設定値を含む辞書
    """
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
        'main_sheet': config['Spreadsheet']['main_sheet'],  # 追加
        'rawdata_sheet': config['Spreadsheet']['rawdata_sheet'],  # 追加
        'json_keyfile_path': config['Credentials']['json_keyfile_path'],
        'csv_base_path': config['Paths']['csv_base_path'],
        'google_folder_id': config['GoogleDrive']['google_folder_id'],
        'chunk_size': int(config['Tuning']['chunk_size']),
        'batch_size': int(config['Tuning']['batch_size']),
        'delay': float(config['Tuning']['delay']),
        'max_workers': int(config['Tuning']['max_workers']),
        'config_file': config_file,  # 設定ファイルのパスを追加
    }

    return ssh_config, db_config, local_port, additional_config