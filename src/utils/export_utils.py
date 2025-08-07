"""
エクスポート処理ユーティリティモジュール

CSV・Parquetファイルのエクスポート、チャンク処理、データ変換等
"""
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional, List, Dict, Any
from decimal import Decimal
import shutil
from src.core.logging.logger import get_logger
from src.core.google_api.auth import retry_on_exception

logger = get_logger(__name__)


def save_chunk_to_csv(chunk: pd.DataFrame, file_path: str, include_header: bool = True) -> None:
    """
    DataFrameチャンクをCSVファイルに保存
    
    Args:
        chunk (pd.DataFrame): 保存するデータチャンク
        file_path (str): 保存先ファイルパス
        include_header (bool): ヘッダーを含めるか
    """
    try:
        mode = 'w' if include_header else 'a'
        with open(file_path, mode, encoding='utf-8', newline='') as file:
            chunk.to_csv(file, index=False, header=include_header)
        logger.debug(f"チャンクをCSVに保存: {file_path}")
    except Exception as e:
        logger.error(f"CSVチャンク保存エラー: {e}")
        raise


def process_dataframe_in_chunks(df: pd.DataFrame, chunk_size: int, file_path: str, 
                               delay: Optional[float] = None) -> int:
    """
    DataFrameをチャンクごとに処理してファイルに保存
    
    Args:
        df (pd.DataFrame): 処理対象DataFrame
        chunk_size (int): チャンクサイズ
        file_path (str): 保存先ファイルパス
        delay (Optional[float]): チャンク間の待機時間（秒）
        
    Returns:
        int: 処理されたレコード数
    """
    total_records = len(df)
    logger.info(f"チャンク処理開始: 総レコード数={total_records}, チャンクサイズ={chunk_size}")
    
    chunk_file_paths = []
    
    try:
        for i in range(0, total_records, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk_file_path = f"{file_path}.chunk_{i // chunk_size}"
            
            # 最初のチャンクのみヘッダーを含める
            include_header = (i == 0)
            save_chunk_to_csv(chunk, chunk_file_path, include_header)
            chunk_file_paths.append(chunk_file_path)
            
            logger.info(f"チャンク処理完了: {i + len(chunk)}/{total_records}")
            
            # 遅延処理
            if delay and i + chunk_size < total_records:
                logger.info(f"Waiting for {delay} seconds before processing the next chunk.")
                time.sleep(delay)
        
        # チャンクファイルを結合
        combine_chunk_files(chunk_file_paths, file_path)
        
        logger.info(f"チャンク処理完了: 総レコード数={total_records}")
        return total_records
        
    except Exception as e:
        logger.error(f"チャンク処理エラー: {e}")
        # 失敗時はチャンクファイルを削除
        for chunk_file in chunk_file_paths:
            if os.path.exists(chunk_file):
                os.remove(chunk_file)
        raise


def combine_chunk_files(chunk_file_paths: List[str], output_file_path: str) -> None:
    """
    複数のチャンクファイルを1つのファイルに結合
    
    Args:
        chunk_file_paths (List[str]): チャンクファイルパスのリスト
        output_file_path (str): 出力ファイルパス
    """
    try:
        with open(output_file_path, 'w', encoding='utf-8', newline='') as output_file:
            for i, chunk_file_path in enumerate(chunk_file_paths):
                with open(chunk_file_path, 'r', encoding='utf-8') as chunk_file:
                    if i == 0:
                        # 最初のファイルは全てコピー（ヘッダー含む）
                        shutil.copyfileobj(chunk_file, output_file)
                    else:
                        # 2番目以降はヘッダーをスキップ
                        next(chunk_file)  # ヘッダー行をスキップ
                        shutil.copyfileobj(chunk_file, output_file)
                
                # チャンクファイルを削除
                os.remove(chunk_file_path)
        
        logger.info(f"チャンクファイル結合完了: {output_file_path}")
        
    except Exception as e:
        logger.error(f"チャンクファイル結合エラー: {e}")
        raise


@retry_on_exception
def csvfile_export(conn, sql_query: str, csv_file_path: str, main_table_name: str, 
                  category: str, json_keyfile_path: str, spreadsheet_id: str, 
                  csv_file_name: str, csv_file_name_column: str, sheet_name: str, 
                  chunk_size: Optional[int] = None, delay: Optional[float] = None) -> int:
    """
    SQLクエリ結果をCSVファイルにエクスポート
    
    Args:
        conn: データベース接続
        sql_query (str): 実行するSQLクエリ
        csv_file_path (str): CSVファイル保存パス
        main_table_name (str): メインテーブル名
        category (str): カテゴリ
        json_keyfile_path (str): Google API認証JSONファイルパス
        spreadsheet_id (str): スプレッドシートID
        csv_file_name (str): CSVファイル名
        csv_file_name_column (str): ファイル名カラム
        sheet_name (str): シート名
        chunk_size (Optional[int]): チャンクサイズ
        delay (Optional[float]): 遅延時間
        
    Returns:
        int: エクスポートされたレコード数
    """
    try:
        logger.info(f"CSVエクスポート開始: {csv_file_name}")
        
        # SQLクエリ実行
        df = pd.read_sql(sql_query, conn)
        record_count = len(df)
        
        logger.info(f"データ取得完了: {record_count}件")
        
        if record_count == 0:
            logger.warning("データが0件でした")
            return 0
        
        # データ型変換
        df = apply_data_type_conversion(df, logger)
        
        # ディレクトリ作成
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        # チャンク処理またはファイル出力
        if chunk_size and record_count > chunk_size:
            temp_file_path = f"{csv_file_path}.temp"
            record_count = process_dataframe_in_chunks(df, chunk_size, temp_file_path, delay=delay)
            
            # 一時ファイルを最終ファイルに移動
            shutil.move(temp_file_path, csv_file_path)
        else:
            df.to_csv(csv_file_path, index=False, encoding='cp932', errors='replace')
        
        logger.info(f"CSVエクスポート完了: {csv_file_path}, レコード数: {record_count}")
        return record_count
        
    except Exception as e:
        logger.error(f"CSVエクスポートエラー: {e}")
        raise


@retry_on_exception  
def parquetfile_export(conn, sql_query: str, parquet_file_path: str, main_table_name: str,
                      category: str, json_keyfile_path: str, spreadsheet_id: str,
                      parquet_file_name: str, csv_file_name_column: str, sheet_name: str,
                      chunk_size: Optional[int] = None, delay: Optional[float] = None) -> int:
    """
    SQLクエリ結果をParquetファイルにエクスポート
    
    Args:
        conn: データベース接続
        sql_query (str): 実行するSQLクエリ
        parquet_file_path (str): Parquetファイル保存パス
        main_table_name (str): メインテーブル名
        category (str): カテゴリ
        json_keyfile_path (str): Google API認証JSONファイルパス
        spreadsheet_id (str): スプレッドシートID
        parquet_file_name (str): Parquetファイル名
        csv_file_name_column (str): ファイル名カラム
        sheet_name (str): シート名
        chunk_size (Optional[int]): チャンクサイズ
        delay (Optional[float]): 遅延時間
        
    Returns:
        int: エクスポートされたレコード数
    """
    try:
        logger.info(f"Parquetエクスポート開始: {parquet_file_name}")
        
        # SQLクエリ実行
        df = pd.read_sql(sql_query, conn)
        record_count = len(df)
        
        logger.info(f"データ取得完了: {record_count}件")
        
        if record_count == 0:
            logger.warning("データが0件でした")
            return 0
        
        # データ型変換
        df = apply_data_type_conversion(df, logger)
        
        # ディレクトリ作成
        os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
        
        # Parquetファイル保存
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file_path)
        
        logger.info(f"Parquetエクスポート完了: {parquet_file_path}, レコード数: {record_count}")
        return record_count
        
    except Exception as e:
        logger.error(f"Parquetエクスポートエラー: {e}")
        raise


def apply_data_type_conversion(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    DataFrameのデータ型変換を適用
    
    Args:
        df (pd.DataFrame): 変換対象DataFrame
        logger: ロガー
        
    Returns:
        pd.DataFrame: 変換後DataFrame
    """
    try:
        # 数値型の列を特定してDecimalから適切な型に変換
        for col in df.columns:
            if df[col].dtype == 'object':
                # Decimalオブジェクトを数値に変換
                if df[col].apply(lambda x: isinstance(x, Decimal)).any():
                    df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
                    
        # 日付列の変換（必要に応じて）
        date_columns = df.select_dtypes(include=['datetime64']).columns
        if len(date_columns) > 0:
            logger.info(f"以下の列の型変換を行いました: {', '.join(date_columns)}")
            
        return df
        
    except Exception as e:
        logger.error(f"データ型変換エラー: {e}")
        return df


def get_column_letter(column_index: int) -> str:
    """
    列インデックスからExcel列文字を取得
    
    Args:
        column_index (int): 列インデックス（0から開始）
        
    Returns:
        str: Excel列文字（A, B, C, ...）
    """
    result = ""
    while column_index >= 0:
        result = chr(column_index % 26 + ord('A')) + result
        column_index = column_index // 26 - 1
    return result


def setup_test_environment(test_execution: bool, output_to_spreadsheet: bool, 
                          save_path_id: str, csv_file_name: str, 
                          spreadsheet_id: str, json_keyfile_path: str) -> Dict[str, Any]:
    """
    テスト環境のセットアップ
    
    Args:
        test_execution (bool): テスト実行フラグ
        output_to_spreadsheet (bool): スプレッドシート出力フラグ
        save_path_id (str): 保存先パスID
        csv_file_name (str): CSVファイル名
        spreadsheet_id (str): スプレッドシートID
        json_keyfile_path (str): JSON認証ファイルパス
        
    Returns:
        Dict[str, Any]: テスト環境設定
    """
    config = {
        'test_execution': test_execution,
        'output_to_spreadsheet': output_to_spreadsheet,
        'save_path_id': save_path_id,
        'csv_file_name': csv_file_name,
        'spreadsheet_id': spreadsheet_id,
        'json_keyfile_path': json_keyfile_path
    }
    
    if test_execution:
        logger.info("テスト環境でのエクスポートを実行します")
        # テスト用の設定変更
        config['save_path_id'] = f"test_{save_path_id}"
        config['csv_file_name'] = f"test_{csv_file_name}"
    
    logger.debug(f"テスト環境設定: {config}")
    return config