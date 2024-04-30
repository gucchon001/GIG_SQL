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

    return ssh_config, db_config, local_port
