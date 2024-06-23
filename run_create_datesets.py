from config_loader import load_config
from common_create_datasets import main

if __name__ == "__main__":
    config_file = 'config.ini'  # 設定ファイルのパスを指定
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    sheet_name = additional_config['eachdata_sheet']
    execution_column = "個別リスト"
    main(sheet_name, execution_column, config_file)