from config_loader import load_config
from common_exe_functions import main

if __name__ == "__main__":
    config_file = 'config.ini'  # 設定ファイルのパスを指定
    ssh_config, db_config, local_port, config = load_config(config_file)
    sheet_name = config['rawdata_sheet']  # または 'rawdata_sheet'
    execution_column = "テスト実行"  # または "実行対象"
    main(sheet_name, execution_column, config_file)