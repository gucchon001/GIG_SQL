# run_create_datasets_individual.py

import sys
from config_loader import load_config
from common_create_datasets import main

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_create_datasets_individual.py <table_name>")
        sys.exit(1)
    
    table_name = sys.argv[1]
    config_file = 'config.ini'  # 設定ファイルのパスを指定
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    
    sheet_name = additional_config['eachdata_sheet']
    execution_column = "テスト実行"
    main(sheet_name, execution_column, config_file, table_name)
