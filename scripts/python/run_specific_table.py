#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指定テーブルのみを実行するスクリプト
既存のプログラムを流用してテーブルを実行
"""

import sys
import os
import pandas as pd
from datetime import datetime
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
from core.data.subcode_loader import (
    load_sql_file_list_from_spreadsheet, 
    get_data_types, 
    apply_data_types_to_df, 
    execute_sql_query_with_conditions,
    load_sql_from_file
)
from core.utils.db_utils import get_connection

def run_specific_table(table_name, output_path="data_Parquet"):
    """
    指定されたテーブルを実行する
    
    Args:
        table_name (str): 実行するテーブル名（CSVファイル呼称）
        output_path (str): 出力先パス
    """
    print(f"\n{'='*60}")
    print(f"指定テーブル実行: {table_name}")
    print(f"{'='*60}")
    
    try:
        # 設定読み込み
        config_file = 'config/settings.ini'
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        
        # データベース接続
        print("[INFO] データベース接続中...")
        conn = get_connection(config_file)
        if not conn:
            print("[ERROR] データベース接続に失敗しました")
            return False
        
        print("[OK] データベース接続成功")
        
        # スプレッドシートからテーブル情報を取得
        spreadsheet_id = additional_config['spreadsheet_id']
        json_keyfile_path = additional_config['json_keyfile_path']
        sheet_name = additional_config['eachdata_sheet']
        execution_column = "個別リスト"
        
        print(f"[INFO] スプレッドシートからテーブル情報を取得中...")
        sql_files_list = load_sql_file_list_from_spreadsheet(
            spreadsheet_id, 
            sheet_name, 
            json_keyfile_path, 
            execution_column=execution_column
        )
        
        # 指定されたテーブルを検索
        target_entry = None
        
        # テーブル名のマッピング（英語名 -> 日本語名）
        table_name_mapping = {
            "invoices_dl": "請求書DL状況",
            "invoice_dl": "請求書DL状況",
            "services": "商品マスタ一覧",
            "companies": "企業一覧",
            "brands": "教室グループ一覧",
            "contracts": "契約・契約更新管理一覧",
            "classrooms": "教室一覧",
            "jobs": "求人一覧",
            "users": "会員一覧",
            "invoices": "請求管理export"
        }
        
        # 英語名の場合は日本語名に変換
        search_name = table_name_mapping.get(table_name, table_name)
        
        for entry in sql_files_list:
            if len(entry) > 11:  # CSVファイル呼称は12番目の要素
                csv_file_name_column = entry[11] if len(entry) > 11 else ""
                if csv_file_name_column == search_name:
                    target_entry = entry
                    break
        
        if not target_entry:
            print(f"[ERROR] テーブル '{table_name}' が見つかりません")
            print("[INFO] 利用可能なテーブル名（日本語名）:")
            for entry in sql_files_list:
                if len(entry) > 11:  # CSVファイル呼称は12番目の要素
                    csv_file_name_column = entry[11] if len(entry) > 11 else ""
                    if csv_file_name_column:
                        print(f"  - {csv_file_name_column}")
            
            print("\n[INFO] 利用可能なテーブル名（英語名）:")
            print("  - invoices_dl (請求書DL状況)")
            print("  - invoice_dl (請求書DL状況)")
            print("  - services (商品マスタ一覧)")
            print("  - companies (企業一覧)")
            print("  - brands (教室グループ一覧)")
            print("  - contracts (契約・契約更新管理一覧)")
            print("  - classrooms (教室一覧)")
            print("  - jobs (求人一覧)")
            print("  - users (会員一覧)")
            print("  - invoices (請求管理export)")
            
            return False
        
        # エントリー情報を展開
        (
            sql_file_name,
            csv_file_name,
            period_condition,
            period_criteria,
            save_path_id,
            output_to_spreadsheet,
            deletion_exclusion,
            paste_format,
            test_execution,
            category,
            main_table_name,
            csv_file_name_column,
            sheet_name_record
        ) = target_entry
        
        print(f"[INFO] 対象テーブル情報:")
        print(f"  SQLファイル: {sql_file_name}")
        print(f"  CSVファイル名: {csv_file_name}")
        print(f"  メインテーブル名: {main_table_name}")
        print(f"  カテゴリ: {category}")
        
        # SQLファイル読み込み
        print(f"[INFO] SQLファイル読み込み: {sql_file_name}")
        google_folder_id = additional_config['google_folder_id']
        
        sql_query = load_sql_from_file(sql_file_name, google_folder_id, json_keyfile_path)
        if not sql_query:
            print(f"[ERROR] SQLファイルの読み込みに失敗: {sql_file_name}")
            return False
        
        print(f"[OK] SQLファイル読み込み成功 ({len(sql_query)} 文字)")
        
        # 期間条件の適用
        if period_condition and period_criteria:
            print(f"[INFO] 期間条件を適用: {period_condition}")
            sql_query = execute_sql_query_with_conditions(
                sql_query, period_condition, period_criteria, category, main_table_name
            )
        
        # データベースクエリ実行
        print("[INFO] データベースクエリ実行中...")
        df = pd.read_sql(sql_query, conn)
        print(f"[OK] データ取得成功: {len(df)} 件")
        
        if len(df) == 0:
            print("[WARN] データが0件です")
            return True
        
        # データ型変換
        print("[INFO] データ型変換中...")
        # 簡単なデータ型変換（get_data_types関数の代替）
        for col in df.columns:
            if df[col].dtype == 'object':
                # 文字列型はそのまま
                pass
            elif 'int' in str(df[col].dtype):
                # 整数型
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif 'datetime' in str(df[col].dtype) or 'timestamp' in str(df[col].dtype):
                # 日時型
                df[col] = pd.to_datetime(df[col], errors='coerce')
        print(f"[OK] データ型変換完了")
        
        # 出力ファイルパス設定
        if not csv_file_name:
            csv_file_name = main_table_name
        
        # ローカルパスまたはNASパスを使用
        if output_path and output_path != "default":
            # 指定された出力パスを使用
            parquet_file_path = os.path.join(output_path, f"{csv_file_name}.parquet")
        else:
            # 設定ファイルのパスを使用（NASパス）
            csv_base_path = additional_config['csv_base_path']
            parquet_file_path = os.path.join(csv_base_path, f"{csv_file_name}.parquet")
        
        print(f"[INFO] 出力先: {parquet_file_path}")
        
        # ディレクトリ作成
        os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
        
        # Parquetファイル保存
        print("[INFO] Parquetファイル保存中...")
        df.to_parquet(parquet_file_path, engine='pyarrow', index=False)
        
        print(f"[OK] Parquetファイル保存成功: {parquet_file_path}")
        print(f"[INFO] 保存件数: {len(df)} 件")
        print(f"[INFO] カラム数: {len(df.columns)} 列")
        
        # データベース接続終了
        conn.close()
        print("[OK] データベース接続終了")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] エラー発生: {e}")
        import traceback
        traceback.print_exc()
        return False

def list_available_tables():
    """利用可能なテーブル一覧を表示"""
    try:
        config_file = 'config/settings.ini'
        ssh_config, db_config, local_port, additional_config = load_config(config_file)
        
        spreadsheet_id = additional_config['spreadsheet_id']
        json_keyfile_path = additional_config['json_keyfile_path']
        sheet_name = additional_config['eachdata_sheet']
        execution_column = "個別リスト"
        
        sql_files_list = load_sql_file_list_from_spreadsheet(
            spreadsheet_id, 
            sheet_name, 
            json_keyfile_path, 
            execution_column=execution_column
        )
        
        print("\n利用可能なテーブル一覧:")
        print("-" * 40)
        for i, entry in enumerate(sql_files_list, 1):
            if len(entry) > 11:  # CSVファイル呼称は12番目の要素
                csv_file_name_column = entry[11] if len(entry) > 11 else ""
                sql_file_name = entry[0]
                if csv_file_name_column:
                    print(f"{i:2d}. {csv_file_name_column} ({sql_file_name})")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] テーブル一覧取得エラー: {e}")
        return False

def main():
    """メイン処理"""
    print("指定テーブル実行スクリプト")
    print("=" * 60)
    
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python run_specific_table.py <テーブル名> [出力パス]")
        print("  python run_specific_table.py --list  # テーブル一覧表示")
        print("\n例（日本語名）:")
        print("  python run_specific_table.py \"請求書DL状況\"")
        print("  python run_specific_table.py \"請求書DL状況\" \"data_Parquet\"")
        print("\n例（英語名）:")
        print("  python run_specific_table.py \"invoice_dl\"")
        print("  python run_specific_table.py \"invoices_dl\"")
        print("  python run_specific_table.py \"services\"")
        print("\nその他:")
        print("  python run_specific_table.py --list")
        sys.exit(1)
    
    if sys.argv[1] == "--list":
        # テーブル一覧表示
        if list_available_tables():
            sys.exit(0)
        else:
            sys.exit(1)
    
    table_name = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "data_Parquet"
    
    if run_specific_table(table_name, output_path):
        print(f"\n{'='*60}")
        print(f"[SUCCESS] テーブル '{table_name}' の実行が完了しました")
        print(f"{'='*60}")
        sys.exit(0)
    else:
        print(f"\n{'='*60}")
        print(f"[ERROR] テーブル '{table_name}' の実行に失敗しました")
        print(f"{'='*60}")
        sys.exit(1)

if __name__ == "__main__":
    main()
