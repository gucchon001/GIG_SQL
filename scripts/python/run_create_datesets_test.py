import sys
import os
# プロジェクトルートをPythonパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from core.config.config_loader import load_config
from core.data.common_create_datasets import main

if __name__ == "__main__":
    config_file = 'config/settings.ini'  # 設定ファイルのパスを指定
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    sheet_name = additional_config['eachdata_sheet']
    execution_column = "テスト実行"
    main(sheet_name, execution_column, config_file)