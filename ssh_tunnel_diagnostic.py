#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSHトンネル接続診断スクリプト
実行PCでのSSHトンネルエラーの原因を調査します
"""

import os
import sys
import subprocess
import socket
import time
from pathlib import Path

def check_port_usage(port=3306):
    """ポートの使用状況を確認"""
    print(f"=== ポート {port} の使用状況確認 ===")
    try:
        # netstatコマンドでポート使用状況を確認（エンコーディング問題を回避）
        result = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, encoding='cp932', errors='ignore')
        lines = result.stdout.split('\n')
        
        port_in_use = []
        for line in lines:
            if f':{port}' in line and 'LISTENING' in line:
                port_in_use.append(line.strip())
        
        if port_in_use:
            print(f"[WARNING] ポート {port} が使用中です:")
            for line in port_in_use:
                print(f"  {line}")
                
            # プロセスIDを抽出してプロセス情報を取得
            for line in port_in_use:
                parts = line.split()
                if len(parts) >= 5:
                    pid = parts[-1]
                    try:
                        process_result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], 
                                                      capture_output=True, text=True, encoding='cp932', errors='ignore')
                        print(f"  プロセス情報 (PID: {pid}):")
                        for proc_line in process_result.stdout.split('\n')[2:]:
                            if proc_line.strip():
                                print(f"    {proc_line}")
                    except:
                        print(f"    プロセス情報取得失敗 (PID: {pid})")
        else:
            print(f"[OK] ポート {port} は使用されていません")
            
        return len(port_in_use) > 0
        
    except Exception as e:
        print(f"[ERROR] ポート確認中にエラー: {e}")
        return False

def check_ssh_key():
    """SSH秘密鍵の存在と権限を確認"""
    print("\n=== SSH秘密鍵確認 ===")
    key_path = Path("config/tomonokai-juku-prod-jump-rsa.pem")
    
    if key_path.exists():
        print(f"[OK] SSH秘密鍵が存在します: {key_path}")
        
        # ファイルサイズと最終更新日時
        stat = key_path.stat()
        print(f"  ファイルサイズ: {stat.st_size} bytes")
        print(f"  最終更新: {time.ctime(stat.st_mtime)}")
        
        # ファイルの最初の数行を確認
        try:
            with open(key_path, 'r') as f:
                first_line = f.readline().strip()
                if first_line.startswith('-----BEGIN'):
                    print(f"[OK] 秘密鍵形式が正しいです")
                else:
                    print(f"[ERROR] 秘密鍵形式が正しくありません: {first_line[:50]}...")
        except Exception as e:
            print(f"[ERROR] 秘密鍵ファイル読み込みエラー: {e}")
            
    else:
        print(f"[ERROR] SSH秘密鍵が見つかりません: {key_path}")
        
    return key_path.exists()

def test_network_connectivity():
    """ネットワーク接続性をテスト"""
    print("\n=== ネットワーク接続性テスト ===")
    
    # SSH接続先のホストを確認（設定ファイルから読み込み）
    hosts_to_test = [
        "tomonokai-juku-prod-rds.cluster-ro-c3pfvjgrotzt.ap-northeast-1.rds.amazonaws.com",
        "google.com",
        "github.com"
    ]
    
    for host in hosts_to_test:
        try:
            print(f"  {host} への接続テスト...", end="")
            socket.create_connection((host, 443), timeout=5)
            print(" [OK]")
        except Exception as e:
            print(f" [FAIL] {e}")

def check_config_files():
    """設定ファイルの存在と内容を確認"""
    print("\n=== 設定ファイル確認 ===")
    
    config_files = [
        "config/settings.ini",
        "config/secrets.env"
    ]
    
    for config_file in config_files:
        config_path = Path(config_file)
        if config_path.exists():
            print(f"[OK] {config_file} が存在します")
            
            # ファイルサイズを確認
            size = config_path.stat().st_size
            print(f"  ファイルサイズ: {size} bytes")
            
        else:
            print(f"[ERROR] {config_file} が見つかりません")

def check_python_environment():
    """Python環境を確認"""
    print("\n=== Python環境確認 ===")
    print(f"Python バージョン: {sys.version}")
    print(f"実行ディレクトリ: {os.getcwd()}")
    
    # 必要なパッケージの確認
    package_mappings = {
        'sshtunnel': 'sshtunnel',
        'paramiko': 'paramiko',
        'mysql-connector-python': 'mysql.connector'
    }
    
    for package_name, import_name in package_mappings.items():
        try:
            __import__(import_name)
            print(f"[OK] {package_name} がインストールされています")
        except ImportError:
            print(f"[ERROR] {package_name} がインストールされていません")

def kill_existing_tunnels():
    """既存のSSHトンネルプロセスを終了"""
    print("\n=== 既存のSSHトンネルプロセス終了 ===")
    
    try:
        # Pythonプロセスでポート3306を使用しているものを探す
        result = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, encoding='cp932', errors='ignore')
        lines = result.stdout.split('\n')
        
        python_pids = []
        for line in lines:
            if ':3306' in line and 'LISTENING' in line:
                parts = line.split()
                if len(parts) >= 5:
                    pid = parts[-1]
                    # プロセス名を確認
                    try:
                        process_result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], 
                                                      capture_output=True, text=True, encoding='cp932', errors='ignore')
                        if 'python.exe' in process_result.stdout:
                            python_pids.append(pid)
                    except:
                        pass
        
        if python_pids:
            print(f"[INFO] 終了対象のPythonプロセス: {python_pids}")
            for pid in python_pids:
                try:
                    subprocess.run(['taskkill', '/PID', pid, '/F'], 
                                 capture_output=True, text=True, encoding='cp932', errors='ignore')
                    print(f"[OK] PID {pid} を終了しました")
                except Exception as e:
                    print(f"[ERROR] PID {pid} の終了に失敗: {e}")
        else:
            print("[INFO] 終了対象のPythonプロセスはありません")
            
    except Exception as e:
        print(f"[ERROR] プロセス終了処理中にエラー: {e}")

def main():
    """メイン処理"""
    print("SSHトンネル接続診断スクリプト")
    print("=" * 60)
    
    # 1. ポート使用状況確認
    port_in_use = check_port_usage()
    
    # 2. SSH秘密鍵確認
    ssh_key_exists = check_ssh_key()
    
    # 3. ネットワーク接続性テスト
    test_network_connectivity()
    
    # 4. 設定ファイル確認
    check_config_files()
    
    # 5. Python環境確認
    check_python_environment()
    
    # 6. 既存のSSHトンネルプロセス終了
    if port_in_use:
        kill_existing_tunnels()
        
        # 終了後にポートを再確認
        print("\n=== プロセス終了後のポート確認 ===")
        time.sleep(2)
        check_port_usage()
    
    # 診断結果のまとめ
    print("\n" + "=" * 60)
    print("診断結果のまとめ")
    print("=" * 60)
    
    if port_in_use:
        print("[ACTION REQUIRED] ポート3306が使用中です。上記のプロセス終了を実行しました。")
        print("                 再度スクリプトを実行してみてください。")
    else:
        print("[OK] ポート3306は使用されていません。")
    
    if ssh_key_exists:
        print("[OK] SSH秘密鍵が存在します。")
    else:
        print("[ERROR] SSH秘密鍵が見つかりません。")
    
    print("\n推奨される次のステップ:")
    print("1. この診断スクリプトを再度実行してポートが解放されたか確認")
    print("2. 問題が解決しない場合は、システム管理者に連絡")
    print("3. SSH秘密鍵の権限設定を確認")

if __name__ == "__main__":
    main()
