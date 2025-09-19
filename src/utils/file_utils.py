"""
ファイル操作ユーティリティ

CSV、Parquet、その他ファイル操作の汎用機能を提供
"""
import os
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional, Dict, Any, Iterator
import sys

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.logging.logger import get_logger


class CSVExporter:
    """CSV出力管理クラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def export_dataframe_to_csv(
        self,
        df: pd.DataFrame,
        file_path: str,
        encoding: str = 'utf-8-sig',
        chunk_size: Optional[int] = None
    ) -> bool:
        """
        データフレームをCSVファイルに出力
        
        Args:
            df: 出力対象のデータフレーム
            file_path: 出力先ファイルパス
            encoding: 文字エンコーディング
            chunk_size: チャンクサイズ（大容量データ対応）
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            # ディレクトリが存在しない場合は作成
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                self.logger.info(f"ディレクトリを作成: {directory}")
            
            if chunk_size and len(df) > chunk_size:
                # チャンク単位で出力
                self._export_chunked_csv(df, file_path, encoding, chunk_size)
            else:
                # 一括出力
                df.to_csv(file_path, index=False, encoding=encoding)
            
            file_size = os.path.getsize(file_path) / 1024**2  # MB
            self.logger.info(f"CSV出力完了: {file_path} ({len(df)} 行, {file_size:.1f}MB)")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV出力エラー: {file_path}, {e}")
            return False
    
    def _export_chunked_csv(
        self,
        df: pd.DataFrame,
        file_path: str,
        encoding: str,
        chunk_size: int
    ) -> None:
        """
        チャンク単位でCSV出力
        
        Args:
            df: 出力対象のデータフレーム
            file_path: 出力先ファイルパス
            encoding: 文字エンコーディング
            chunk_size: チャンクサイズ
        """
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = min(chunk_start + chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]
            
            # 最初のチャンクの場合はヘッダーも含める
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            
            chunk_df.to_csv(file_path, mode=mode, header=header, index=False, encoding=encoding)
            
            self.logger.debug(f"チャンク出力: {i+1}/{total_chunks} ({chunk_start+1}〜{chunk_end}行)")


class ParquetExporter:
    """Parquet出力管理クラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def export_dataframe_to_parquet(
        self,
        df: pd.DataFrame,
        file_path: str,
        compression: str = 'snappy'
    ) -> bool:
        """
        データフレームをParquetファイルに出力
        
        Args:
            df: 出力対象のデータフレーム
            file_path: 出力先ファイルパス
            compression: 圧縮方式
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            # ディレクトリが存在しない場合は作成
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                self.logger.info(f"ディレクトリを作成: {directory}")
            
            # Parquetテーブルに変換
            table = pa.Table.from_pandas(df)
            
            # Parquetファイルに書き込み
            pq.write_table(table, file_path, compression=compression)
            
            file_size = os.path.getsize(file_path) / 1024**2  # MB
            self.logger.info(f"Parquet出力完了: {file_path} ({len(df)} 行, {file_size:.1f}MB)")
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet出力エラー: {file_path}, {e}")
            return False
    
    def read_parquet_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Parquetファイルを読み込み
        
        Args:
            file_path: ファイルパス
            
        Returns:
            pd.DataFrame: 読み込んだデータフレーム（失敗時はNone）
        """
        try:
            df = pd.read_parquet(file_path)
            self.logger.info(f"Parquet読み込み完了: {file_path} ({len(df)} 行)")
            return df
            
        except Exception as e:
            self.logger.error(f"Parquet読み込みエラー: {file_path}, {e}")
            return None


class FileManager:
    """ファイル管理統合クラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.csv_exporter = CSVExporter()
        self.parquet_exporter = ParquetExporter()
    
    def cleanup_old_files(self, directory: str, days_old: int = 30) -> int:
        """
        古いファイルをクリーンアップ
        
        Args:
            directory: 対象ディレクトリ
            days_old: 削除対象の日数
            
        Returns:
            int: 削除したファイル数
        """
        import time
        
        if not os.path.exists(directory):
            self.logger.warning(f"ディレクトリが存在しません: {directory}")
            return 0
        
        deleted_count = 0
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) < cutoff_time:
                        try:
                            os.remove(file_path)
                            deleted_count += 1
                            self.logger.info(f"古いファイルを削除: {file_path}")
                        except Exception as e:
                            self.logger.error(f"ファイル削除エラー: {file_path}, {e}")
            
            self.logger.info(f"ファイルクリーンアップ完了: {deleted_count} 件削除")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"ファイルクリーンアップエラー: {e}")
            return 0
    
    def get_directory_size(self, directory: str) -> float:
        """
        ディレクトリのサイズを取得（MB）
        
        Args:
            directory: 対象ディレクトリ
            
        Returns:
            float: ディレクトリサイズ（MB）
        """
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        total_size += os.path.getsize(file_path)
            
            size_mb = total_size / 1024**2
            self.logger.debug(f"ディレクトリサイズ: {directory} = {size_mb:.1f}MB")
            return size_mb
            
        except Exception as e:
            self.logger.error(f"ディレクトリサイズ取得エラー: {directory}, {e}")
            return 0.0


# 後方互換性のための関数
def csvfile_export(
    conn,
    sql_query: str,
    csv_file_name: str,
    save_path: str,
    save_path_id: str,
    chunk_size: int = 10000
) -> None:
    """旧式のCSV出力関数（後方互換性のため）"""
    try:
        # SQLクエリを実行してデータフレームを取得
        df = pd.read_sql(sql_query, conn)
        
        # ファイルパスを構築
        full_path = os.path.join(save_path, save_path_id, csv_file_name)
        
        # CSV出力
        exporter = CSVExporter()
        exporter.export_dataframe_to_csv(df, full_path, chunk_size=chunk_size)
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"CSV出力エラー: {csv_file_name}, {e}")
        raise


def parquetfile_export(
    conn,
    sql_query: str,
    parquet_file_name: str,
    output_dir: str
) -> None:
    """旧式のParquet出力関数（後方互換性のため）"""
    try:
        # SQLクエリを実行してデータフレームを取得
        df = pd.read_sql(sql_query, conn)
        
        # ファイルパスを構築
        full_path = os.path.join(output_dir, parquet_file_name)
        
        # Parquet出力
        exporter = ParquetExporter()
        exporter.export_dataframe_to_parquet(df, full_path)
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Parquet出力エラー: {parquet_file_name}, {e}")
        raise