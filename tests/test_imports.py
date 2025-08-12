#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
新しい構造でのインポートテスト
"""
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append('src')

def test_core_imports():
    """コアモジュールのインポートテスト"""
    try:
        print("=== Core Modules Import Test ===")
        
        # 設定管理
        from src.core.config.settings import AppConfig
        print("✓ Core config imported")
        
        # ログ管理
        from src.core.logging.logger import get_logger
        print("✓ Core logging imported")
        
        # データベース
        from src.core.database.connection import DatabaseConnection
        from src.core.database.ssh_tunnel import SSHTunnel
        print("✓ Core database imported")
        
        return True
        
    except Exception as e:
        print(f"✗ Core import error: {e}")
        return False

def test_batch_imports():
    """バッチシステムのインポートテスト"""
    try:
        print("\n=== Batch System Import Test ===")
        
        # バッチプロセッサー
        from src.batch_system.processors.batch_processor import BatchProcessor
        print("✓ Batch processor imported")
        
        # バッチメイン（問題が発生する可能性がある部分）
        print("Testing batch main import...")
        from src.batch_system.main import BatchMain
        print("✓ Batch main imported")
        
        return True
        
    except Exception as e:
        print(f"✗ Batch import error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_utils_imports():
    """ユーティリティのインポートテスト"""
    try:
        print("\n=== Utils Import Test ===")
        
        from src.utils.data_utils import DataTypeConverter
        from src.utils.file_utils import CSVExporter
        print("✓ Utils imported")
        
        return True
        
    except Exception as e:
        print(f"✗ Utils import error: {e}")
        return False

def test_config_loading():
    """設定ファイル読み込みテスト"""
    try:
        print("\n=== Config Loading Test ===")
        
        from src.core.config.settings import AppConfig
        config = AppConfig.from_config_file('config.ini')
        
        print(f"✓ Config loaded - Environment: {config.environment}")
        print(f"✓ SSH Host: {config.ssh.host}")
        print(f"✓ Database: {config.database.database}")
        
        return True
        
    except Exception as e:
        print(f"✗ Config loading error: {e}")
        return False

if __name__ == "__main__":
    print("Starting import tests...")
    
    results = []
    results.append(test_core_imports())
    results.append(test_batch_imports())
    results.append(test_utils_imports())
    results.append(test_config_loading())
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {sum(results)}/{len(results)}")
    
    if all(results):
        print("🎉 All imports successful!")
        sys.exit(0)
    else:
        print("❌ Some imports failed!")
        sys.exit(1)