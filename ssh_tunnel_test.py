#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSHトンネル接続テストスクリプト
実際のSSHトンネル接続をテストします
"""

import sys
import os
import time
import traceback
from pathlib import Path

# プロジェクトルートをPythonパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

def test_ssh_tunnel_connection():
    """SSHトンネル接続をテスト"""
    print("=== SSHトンネル接続テスト ===")
    
    try:
        # SSH接続モジュールをインポート
        from core.config.ssh_connection import create_ssh_tunnel
        from core.config.config_loader import load_config
        
        print("[INFO] 設定ファイルを読み込み中...")
        config_file = 'config/settings.ini'
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        
        print("[INFO] SSHトンネルを作成中...")
        print(f"  ローカルポート: {local_port}")
        print(f"  SSHホスト: {ssh_config['host']}")
        print(f"  SSHユーザー: {ssh_config['username']}")
        print(f"  リモートホスト: {db_config['host']}")
        print(f"  リモートポート: {db_config['port']}")
        
        # SSHトンネルを作成
        tunnel = create_ssh_tunnel(ssh_config, db_config, local_port)
        
        if tunnel:
            print("[SUCCESS] SSHトンネルが正常に作成されました！")
            
            # トンネルを少し保持
            print("[INFO] トンネルを5秒間保持中...")
            time.sleep(5)
            
            # トンネルを閉じる
            print("[INFO] SSHトンネルを閉じています...")
            tunnel.stop()
            print("[OK] SSHトンネルが正常に閉じられました")
            
            return True
        else:
            print("[ERROR] SSHトンネルの作成に失敗しました")
            return False
            
    except Exception as e:
        print(f"[ERROR] SSHトンネル接続テスト中にエラーが発生しました:")
        print(f"  エラー詳細: {e}")
        print(f"  エラータイプ: {type(e).__name__}")
        traceback.print_exc()
        return False

def test_database_connection():
    """データベース接続をテスト"""
    print("\n=== データベース接続テスト ===")
    
    try:
        from core.config.database_connection import get_database_connection
        from core.config.config_loader import load_config
        
        print("[INFO] 設定ファイルを読み込み中...")
        config_file = 'config/settings.ini'
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        
        print("[INFO] データベース接続を試行中...")
        
        # SSHトンネル経由でデータベース接続
        conn = get_database_connection(ssh_config, db_config, local_port)
        
        if conn:
            print("[SUCCESS] データベース接続が成功しました！")
            
            # 簡単なクエリを実行
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] == 1:
                print("[OK] テストクエリが正常に実行されました")
            
            conn.close()
            print("[OK] データベース接続が正常に閉じられました")
            return True
        else:
            print("[ERROR] データベース接続に失敗しました")
            return False
            
    except Exception as e:
        print(f"[ERROR] データベース接続テスト中にエラーが発生しました:")
        print(f"  エラー詳細: {e}")
        print(f"  エラータイプ: {type(e).__name__}")
        traceback.print_exc()
        return False

def test_full_pipeline():
    """完全なパイプラインをテスト"""
    print("\n=== 完全パイプラインテスト ===")
    
    try:
        from core.config.config_loader import load_config
        from core.config.ssh_connection import create_ssh_tunnel
        from core.config.database_connection import get_database_connection
        
        print("[INFO] 設定ファイルを読み込み中...")
        config_file = 'config/settings.ini'
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        
        print("[INFO] SSHトンネルを作成中...")
        tunnel = create_ssh_tunnel(ssh_config, db_config, local_port)
        
        if not tunnel:
            print("[ERROR] SSHトンネルの作成に失敗しました")
            return False
        
        print("[INFO] データベース接続を試行中...")
        conn = get_database_connection(ssh_config, db_config, local_port)
        
        if not conn:
            print("[ERROR] データベース接続に失敗しました")
            tunnel.stop()
            return False
        
        print("[SUCCESS] 完全パイプラインが正常に動作しました！")
        
        # リソースをクリーンアップ
        conn.close()
        tunnel.stop()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 完全パイプラインテスト中にエラーが発生しました:")
        print(f"  エラー詳細: {e}")
        print(f"  エラータイプ: {type(e).__name__}")
        traceback.print_exc()
        return False

def main():
    """メイン処理"""
    print("SSHトンネル接続テストスクリプト")
    print("=" * 60)
    
    # 設定ファイルの存在確認
    config_file = Path("config/settings.ini")
    if not config_file.exists():
        print("[ERROR] 設定ファイルが見つかりません: config/settings.ini")
        return
    
    # SSH秘密鍵の存在確認
    ssh_key = Path("config/tomonokai-juku-prod-jump-rsa.pem")
    if not ssh_key.exists():
        print("[ERROR] SSH秘密鍵が見つかりません: config/tomonokai-juku-prod-jump-rsa.pem")
        return
    
    print("[OK] 必要なファイルが存在します")
    
    # テストを実行
    tests = [
        ("SSHトンネル接続", test_ssh_tunnel_connection),
        ("データベース接続", test_database_connection),
        ("完全パイプライン", test_full_pipeline)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"テスト: {test_name}")
        print(f"{'='*60}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"[ERROR] テスト '{test_name}' で予期しないエラー: {e}")
            results.append((test_name, False))
    
    # 結果のまとめ
    print(f"\n{'='*60}")
    print("テスト結果のまとめ")
    print(f"{'='*60}")
    
    for test_name, result in results:
        status = "[SUCCESS]" if result else "[FAILED]"
        print(f"{status} {test_name}")
    
    success_count = sum(1 for _, result in results if result)
    total_count = len(results)
    
    print(f"\n成功: {success_count}/{total_count}")
    
    if success_count == total_count:
        print("[COMPLETE] すべてのテストが成功しました！")
        print("SSHトンネル接続は正常に動作しています。")
    else:
        print("[WARNING] 一部のテストが失敗しました。")
        print("エラーログを確認して問題を特定してください。")

if __name__ == "__main__":
    main()
