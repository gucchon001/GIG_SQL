"""
データ処理ユーティリティ

データ型変換、フォーマット、バリデーション等の汎用機能を提供
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from decimal import Decimal
from datetime import datetime, date
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.logging.logger import get_logger


class DataTypeConverter:
    """データ型変換管理クラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def apply_data_types_to_dataframe(
        self,
        df: pd.DataFrame,
        data_types: Dict[str, str],
        encoding: str = 'utf-8'
    ) -> pd.DataFrame:
        """
        データフレームに指定されたデータ型を適用
        
        Args:
            df: 対象のデータフレーム
            data_types: データ型マッピング
            encoding: 文字エンコーディング
            
        Returns:
            pd.DataFrame: 型変換後のデータフレーム
        """
        converted_columns = []
        
        for column, data_type in data_types.items():
            if column in df.columns:
                try:
                    if data_type == 'txt':
                        df[column] = df[column].apply(
                            lambda x: x.encode(encoding).decode(encoding) if isinstance(x, str) else x
                        )
                        converted_columns.append(column)
                        
                    elif data_type == 'int':
                        df[column] = pd.to_numeric(df[column], errors='raise').astype('Int64')
                        converted_columns.append(column)
                        
                    elif data_type == 'float':
                        df[column] = pd.to_numeric(df[column], errors='raise').astype(float)
                        converted_columns.append(column)
                        
                    elif data_type == 'date':
                        df[column] = df[column].astype(str)
                        converted_columns.append(column)
                        
                    elif data_type == 'datetime':
                        df[column] = df[column].astype(str)
                        converted_columns.append(column)
                        
                except ValueError as e:
                    self.logger.error(f"データ型変換エラー - 列: '{column}', 型: '{data_type}', エラー: {e}")
                    raise ValueError(f"列 '{column}' の型 '{data_type}' への変換に失敗: {e}")
                    
                except Exception as e:
                    self.logger.error(f"予期しないエラー - 列: '{column}', エラー: {e}")
                    raise Exception(f"列 '{column}' の処理中に予期しないエラー: {e}")
        
        if converted_columns:
            self.logger.info(f"データ型変換完了: {', '.join(converted_columns)}")
        else:
            self.logger.info("データ型変換は実行されませんでした")
        
        return df
    
    def format_dates_for_parquet(self, df: pd.DataFrame, data_types: Dict[str, str]) -> pd.DataFrame:
        """
        Parquet保存用に日付フォーマットを統一
        
        Args:
            df: 対象のデータフレーム
            data_types: データ型マッピング
            
        Returns:
            pd.DataFrame: フォーマット後のデータフレーム
        """
        for column, data_type in data_types.items():
            if column in df.columns:
                try:
                    if data_type == 'date':
                        df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d')
                        self.logger.info(f"日付フォーマット変換: '{column}' -> date")
                        
                    elif data_type == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
                        self.logger.info(f"日時フォーマット変換: '{column}' -> datetime")
                        
                except pd.errors.OutOfBoundsDatetime as e:
                    self.logger.error(f"日付範囲外エラー: {e} (列: {column})")
                    df[column] = pd.NaT
                    
                except Exception as e:
                    self.logger.error(f"日付フォーマットエラー: {e} (列: {column})")
                    df[column] = pd.NaT
        
        return df


class DataValidator:
    """データバリデーションクラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def validate_dataframe(self, df: pd.DataFrame, rules: Dict[str, Any]) -> List[str]:
        """
        データフレームをバリデーション
        
        Args:
            df: 検証対象のデータフレーム
            rules: バリデーションルール
            
        Returns:
            List[str]: エラーメッセージのリスト
        """
        errors = []
        
        # 基本チェック
        if df.empty:
            errors.append("データフレームが空です")
            return errors
        
        # 列数チェック
        if 'min_columns' in rules:
            if len(df.columns) < rules['min_columns']:
                errors.append(f"列数不足: {len(df.columns)} < {rules['min_columns']}")
        
        # 行数チェック
        if 'min_rows' in rules:
            if len(df) < rules['min_rows']:
                errors.append(f"行数不足: {len(df)} < {rules['min_rows']}")
        
        # 必須列チェック
        if 'required_columns' in rules:
            missing_cols = set(rules['required_columns']) - set(df.columns)
            if missing_cols:
                errors.append(f"必須列が不足: {', '.join(missing_cols)}")
        
        # NULL値チェック
        if 'no_null_columns' in rules:
            for col in rules['no_null_columns']:
                if col in df.columns and df[col].isnull().any():
                    null_count = df[col].isnull().sum()
                    errors.append(f"NULL値が含まれています: {col} ({null_count} 件)")
        
        # データ型チェック
        if 'column_types' in rules:
            for col, expected_type in rules['column_types'].items():
                if col in df.columns:
                    actual_type = str(df[col].dtype)
                    if expected_type not in actual_type:
                        errors.append(f"データ型不一致: {col} (期待: {expected_type}, 実際: {actual_type})")
        
        if errors:
            self.logger.warning(f"データ検証エラー: {len(errors)} 件")
            for error in errors:
                self.logger.warning(f"  - {error}")
        else:
            self.logger.info("データ検証が正常に完了しました")
        
        return errors
    
    def check_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        データ品質をチェック
        
        Args:
            df: 検証対象のデータフレーム
            
        Returns:
            Dict[str, Any]: データ品質レポート
        """
        quality_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'null_counts': {},
            'duplicate_rows': df.duplicated().sum(),
            'column_types': {},
            'numeric_stats': {}
        }
        
        # 列ごとのNULL数とデータ型
        for col in df.columns:
            quality_report['null_counts'][col] = df[col].isnull().sum()
            quality_report['column_types'][col] = str(df[col].dtype)
            
            # 数値列の統計
            if df[col].dtype in ['int64', 'float64', 'Int64']:
                quality_report['numeric_stats'][col] = {
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'std': df[col].std()
                }
        
        self.logger.info(f"データ品質チェック完了: {quality_report['total_rows']} 行, {quality_report['total_columns']} 列")
        return quality_report


class FileUtils:
    """ファイル操作ユーティリティクラス"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def ensure_directory_exists(self, directory_path: str) -> bool:
        """
        ディレクトリの存在を確認し、なければ作成
        
        Args:
            directory_path: ディレクトリパス
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
                self.logger.info(f"ディレクトリを作成: {directory_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"ディレクトリ作成エラー: {directory_path}, {e}")
            return False
    
    def get_safe_filename(self, filename: str) -> str:
        """
        安全なファイル名を生成
        
        Args:
            filename: 元のファイル名
            
        Returns:
            str: 安全なファイル名
        """
        import re
        # 危険な文字を除去
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)
        safe_name = safe_name.strip()
        
        # 空文字列の場合はデフォルト名
        if not safe_name:
            safe_name = "unnamed_file"
        
        return safe_name
    
    def get_file_size_mb(self, file_path: str) -> float:
        """
        ファイルサイズをMB単位で取得
        
        Args:
            file_path: ファイルパス
            
        Returns:
            float: ファイルサイズ（MB）
        """
        try:
            size_bytes = os.path.getsize(file_path)
            return size_bytes / 1024**2
        except Exception as e:
            self.logger.error(f"ファイルサイズ取得エラー: {file_path}, {e}")
            return 0.0


# 後方互換性のための関数
def apply_data_types_to_df(df: pd.DataFrame, data_types: Dict[str, str], logger, encoding: str = 'utf-8') -> pd.DataFrame:
    """旧式のデータ型適用関数（後方互換性のため）"""
    converter = DataTypeConverter()
    return converter.apply_data_types_to_dataframe(df, data_types, encoding)


def get_data_types(worksheet) -> Dict[str, str]:
    """旧式のデータ型取得関数（後方互換性のため）"""
    from src.core.google_api.sheets_client import GoogleSheetsClient
    
    # 一時的な実装（将来的にはより良い方法で実装）
    client = GoogleSheetsClient("config/settings.ini")  # 設定から取得するように改善予定
    return client.get_data_types_from_sheet(worksheet)