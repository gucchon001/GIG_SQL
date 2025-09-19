# run_create_datasets_individual.py

import sys
import os
import io
import locale

# 文字エンコーディング設定を強化
os.environ['PYTHONIOENCODING'] = 'utf-8'
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ロケール設定
try:
    locale.setlocale(locale.LC_ALL, 'C.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except:
        pass  # ロケール設定に失敗しても続行
# プロジェクトルートをPythonパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from core.config.config_loader import load_config
from core.data.common_create_datasets import main

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_create_datasets_individual.py <table_name>")
        sys.exit(1)
    
    table_name = sys.argv[1]
    config_file = 'config/settings.ini'  # 設定ファイルのパスを指定
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    
    sheet_name = additional_config['eachdata_sheet']
    execution_column = "個別リスト"  # 個別リスト列を参照するように変更
    
    try:
        main(sheet_name, execution_column, config_file, table_name)
        print(f"処理完了: {table_name}")
        sys.exit(0)  # 明示的に成功を示す
    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)  # 明示的に失敗を示す
