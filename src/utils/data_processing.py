"""
データ処理ユーティリティモジュール

データの読み込み、フィルタリング、変換処理
"""
import pandas as pd
import os
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def format_dates(df: pd.DataFrame, data_types: Dict[str, str]) -> pd.DataFrame:
    """
    DataFrameの日付フォーマットを統一
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        data_types (Dict[str, str]): データタイプ辞書
        
    Returns:
        pd.DataFrame: フォーマット済みDataFrame
    """
    for column, data_type in data_types.items():
        if column in df.columns:
            try:
                if data_type == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d')
                elif data_type == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
                logger.info(f"列 '{column}' の日付フォーマットを '{data_type}' に変換しました。")
            except pd.errors.OutOfBoundsDatetime as e:
                logger.error(f"OutOfBoundsDatetimeエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # エラー発生時にはNaTに変換
            except Exception as e:
                logger.error(f"日付フォーマット中にエラーが発生しました: {e} (列: {column})")
                df[column] = pd.NaT  # その他のエラー発生時にもNaTに変換
    return df


def load_and_filter_parquet(parquet_file_path: str, input_fields: Dict[str, Any], 
                           input_fields_types: Dict[str, str], 
                           options_dict: Dict[str, List]) -> Optional[pd.DataFrame]:
    """
    Parquetファイルを読み込み、条件でフィルタリング
    
    Args:
        parquet_file_path (str): Parquetファイルパス
        input_fields (Dict[str, Any]): 入力フィールド
        input_fields_types (Dict[str, str]): フィールドタイプ
        options_dict (Dict[str, List]): オプション辞書
        
    Returns:
        Optional[pd.DataFrame]: フィルタリング済みDataFrame
    """
    if not os.path.exists(parquet_file_path):
        logger.error(f"Parquetファイルが見つかりません: {parquet_file_path}")
        return None
    
    try:
        # Parquetファイル読み込み
        df = pd.read_parquet(parquet_file_path)
        # インデックスの降順で並べ替え（最新データを上位表示）
        df = df.sort_index(ascending=False)
        logger.info(f"Parquetファイル読み込み完了: {len(df)}件（降順ソート済み）")
        
        # フィルタリング条件を適用
        filtered_df = apply_filters(df, input_fields, input_fields_types)
        
        logger.info(f"フィルタリング完了: {len(filtered_df)}件")
        return filtered_df
        
    except Exception as e:
        logger.error(f"Parquetファイル処理エラー: {e}")
        return None


def apply_filters(df: pd.DataFrame, input_fields: Dict[str, Any], 
                 input_fields_types: Dict[str, str]) -> pd.DataFrame:
    """
    DataFrameにフィルタリング条件を適用
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        input_fields (Dict[str, Any]): 入力フィールド
        input_fields_types (Dict[str, str]): フィールドタイプ
        
    Returns:
        pd.DataFrame: フィルタリング済みDataFrame
    """
    filtered_df = df.copy()
    filter_count = 0
    
    for field_name, field_value in input_fields.items():
        if field_name not in df.columns:
            continue
            
        field_type = input_fields_types.get(field_name, 'text')
        
        # 各フィールドタイプに応じたフィルタリング
        if field_type == 'text' and field_value and str(field_value).strip():
            mask = filtered_df[field_name].astype(str).str.contains(
                str(field_value), case=False, na=False
            )
            filtered_df = filtered_df[mask]
            filter_count += 1
            logger.debug(f"テキストフィルタ適用: {field_name} = {field_value}")
            
        elif field_type == 'date' and isinstance(field_value, dict):
            start_date = field_value.get('start_date')
            end_date = field_value.get('end_date')
            
            if start_date or end_date:
                date_col = pd.to_datetime(filtered_df[field_name], errors='coerce')
                
                if start_date:
                    mask = date_col >= pd.to_datetime(start_date)
                    filtered_df = filtered_df[mask]
                    filter_count += 1
                    
                if end_date:
                    mask = date_col <= pd.to_datetime(end_date)
                    filtered_df = filtered_df[mask]
                    filter_count += 1
                    
                logger.debug(f"日付フィルタ適用: {field_name} = {start_date} ~ {end_date}")
                
        elif field_type == 'select' and isinstance(field_value, dict):
            selected_options = [k for k, v in field_value.items() if v]
            
            if selected_options:
                mask = filtered_df[field_name].isin(selected_options)
                filtered_df = filtered_df[mask]
                filter_count += 1
                logger.debug(f"選択フィルタ適用: {field_name} = {selected_options}")
    
    logger.info(f"フィルタリング完了: {filter_count}個の条件を適用")
    # フィルタリング後に降順ソート（legacy版と同じ動作）
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_index(ascending=False)
        logger.debug("フィルタリング後に降順ソートを適用")
    return filtered_df


def load_and_prepare_data(df: pd.DataFrame, page_number: int, page_size: int) -> pd.DataFrame:
    """
    データの準備とページネーション
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        page_number (int): ページ番号
        page_size (int): ページサイズ
        
    Returns:
        pd.DataFrame: ページネーション済みDataFrame
    """
    if df is None or df.empty:
        logger.warning("データが空です")
        return pd.DataFrame()
    
    # ページネーション計算
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    
    # データ切り出し
    paged_df = df.iloc[start_idx:end_idx].copy()
    
    logger.debug(f"データ準備完了: ページ{page_number}, {len(paged_df)}件")
    return paged_df


def get_parquet_file_last_modified(parquet_file_path: str) -> Optional[str]:
    """
    Parquetファイルの最終更新日時を取得
    
    Args:
        parquet_file_path (str): Parquetファイルパス
        
    Returns:
        Optional[str]: 最終更新日時文字列
    """
    if os.path.exists(parquet_file_path):
        last_modified_timestamp = os.path.getmtime(parquet_file_path)
        last_modified_datetime = datetime.fromtimestamp(last_modified_timestamp)
        last_modified_str = last_modified_datetime.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Parquetファイル '{parquet_file_path}' の最終更新日時: {last_modified_str}")
        return last_modified_str
    else:
        logger.warning(f"Parquetファイル '{parquet_file_path}' が存在しません。")
        return None


def clean_data_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    表示用にデータをクリーニング
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        
    Returns:
        pd.DataFrame: クリーニング済みDataFrame
    """
    if df.empty:
        return df
    
    cleaned_df = df.copy()
    
    # NaN値を空文字に変換
    cleaned_df = cleaned_df.fillna('')
    
    # 数値型の列で整数表現できるものは整数に変換
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype in ['float64', 'float32']:
            # すべて整数に変換可能かチェック
            try:
                if cleaned_df[col].apply(lambda x: float(x).is_integer() if x != '' else True).all():
                    cleaned_df[col] = cleaned_df[col].astype('Int64')
            except:
                pass  # 変換できない場合はそのまま
    
    logger.debug("データクリーニング完了")
    return cleaned_df


def validate_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> Dict[str, bool]:
    """
    データタイプの妥当性を検証
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        expected_types (Dict[str, str]): 期待されるデータタイプ
        
    Returns:
        Dict[str, bool]: 各列の妥当性結果
    """
    validation_results = {}
    
    for column, expected_type in expected_types.items():
        if column not in df.columns:
            validation_results[column] = False
            continue
            
        try:
            if expected_type == 'date':
                pd.to_datetime(df[column], errors='raise')
            elif expected_type == 'int':
                pd.to_numeric(df[column], errors='raise')
            elif expected_type == 'float':
                pd.to_numeric(df[column], errors='raise')
            
            validation_results[column] = True
            
        except:
            validation_results[column] = False
            logger.warning(f"データタイプ検証失敗: {column} (期待: {expected_type})")
    
    logger.info(f"データタイプ検証完了: {sum(validation_results.values())}/{len(validation_results)} 成功")
    return validation_results


def replace_null_values(val: Any) -> str:
    """
    NULL値を空文字列に置換
    
    Args:
        val (Any): 置換対象値
        
    Returns:
        str: 置換後文字列
    """
    if pd.isna(val) or val is None:
        return ""
    return str(val)


def prepare_csv_data(df: pd.DataFrame, input_fields_types: Dict[str, str]) -> pd.DataFrame:
    """
    CSV出力用にDataFrameを準備
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        input_fields_types (Dict[str, str]): フィールドタイプ辞書
        
    Returns:
        pd.DataFrame: CSV用DataFrame
    """
    csv_df = df.copy()
    
    # NULL値を空文字列に置換
    csv_df = csv_df.map(replace_null_values)
    
    # データタイプに応じた変換
    for column, data_type in input_fields_types.items():
        if column in csv_df.columns:
            if data_type in ['date', 'datetime']:
                # 日付型の処理は既存のformat_dates関数を使用
                pass
    
    # 数値型の調整（complex型も安全に処理）
    def safe_convert(x):
        try:
            # 複素数型の場合は実部のみを使用
            if isinstance(x, complex):
                x = x.real
            # NaN、無限大をチェック
            if pd.isna(x) or (isinstance(x, (float, int)) and not np.isfinite(x)):
                return ""
            # 整数値の場合は整数として表示
            if isinstance(x, (float, int)) and float(x).is_integer():
                return str(int(x))
            # その他は文字列として処理
            return str(x)
        except (ValueError, TypeError, OverflowError):
            # 変換エラーの場合は空文字列
            return ""
    
    csv_df = csv_df.map(safe_convert)
    
    logger.info(f"CSV用データ準備完了: {csv_df.shape}")
    return csv_df


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrameのデータ型を最適化
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        
    Returns:
        pd.DataFrame: 最適化済みDataFrame
    """
    optimized_df = df.copy()
    
    for col in optimized_df.columns:
        col_type = optimized_df[col].dtype
        
        if col_type == 'object':
            # 文字列列の最適化
            try:
                # 数値変換可能かチェック
                numeric_col = pd.to_numeric(optimized_df[col], errors='coerce')
                if not numeric_col.isna().all():
                    if numeric_col.eq(numeric_col.astype(int), fill_value=False).all():
                        optimized_df[col] = numeric_col.astype('Int64')
                    else:
                        optimized_df[col] = numeric_col.astype('float32')
                    continue
            except:
                pass
            
            # カテゴリ型に変換（ユニーク値が少ない場合）
            unique_ratio = optimized_df[col].nunique() / len(optimized_df[col])
            if unique_ratio < 0.5:
                optimized_df[col] = optimized_df[col].astype('category')
        
        elif col_type in ['int64']:
            # 整数型の最適化
            col_min, col_max = optimized_df[col].min(), optimized_df[col].max()
            if col_min >= -128 and col_max <= 127:
                optimized_df[col] = optimized_df[col].astype('int8')
            elif col_min >= -32768 and col_max <= 32767:
                optimized_df[col] = optimized_df[col].astype('int16')
            elif col_min >= -2147483648 and col_max <= 2147483647:
                optimized_df[col] = optimized_df[col].astype('int32')
        
        elif col_type in ['float64']:
            # 浮動小数点型の最適化
            optimized_df[col] = optimized_df[col].astype('float32')
    
    memory_before = df.memory_usage(deep=True).sum() / 1024 / 1024
    memory_after = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
    
    logger.info(f"データ型最適化完了: {memory_before:.1f}MB → {memory_after:.1f}MB ({memory_before/memory_after:.1f}x削減)")
    return optimized_df


def load_parquet_file(file_path: str, num_rows: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Parquetファイルを読み込み
    
    Args:
        file_path (str): ファイルパス
        num_rows (Optional[int]): 読み込み行数制限
        
    Returns:
        Optional[pd.DataFrame]: 読み込み済みDataFrame
    """
    if not os.path.exists(file_path):
        logger.error(f"ファイルが見つかりません: {file_path}")
        return None
    
    try:
        if num_rows:
            # pyarrowを使用して行数制限付き読み込み
            import pyarrow.parquet as pq
            table = pq.read_table(file_path)
            df = table.to_pandas()
            # インデックスの降順で並べ替えてから指定行数を取得
            df = df.sort_index(ascending=False)
            df = df.head(num_rows)
        else:
            df = pd.read_parquet(file_path)
            # インデックスの降順で並べ替え（最新データを上位表示）
            df = df.sort_index(ascending=False)
        
        logger.info(f"Parquetファイル読み込み完了: {df.shape}（降順ソート済み）")
        return df
        
    except Exception as e:
        logger.error(f"Parquetファイル読み込みエラー: {e}")
        return None