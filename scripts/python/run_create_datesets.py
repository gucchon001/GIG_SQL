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
    config_file = 'config/settings.ini'  # 設定ファイルのパスを指定
    print(f"[run_create_datesets.py] 開始: {config_file}")
    
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    sheet_name = additional_config['eachdata_sheet']
    execution_column = "個別リスト"
    
    print(f"[run_create_datesets.py] デバッグ情報:")
    print(f"  - sheet_name: {sheet_name}")
    print(f"  - execution_column: {execution_column}")
    print(f"  - csv_base_path: {additional_config.get('csv_base_path', 'NOT_FOUND')}")
    
    main(sheet_name, execution_column, config_file)