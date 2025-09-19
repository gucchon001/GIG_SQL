"""
データユーティリティのテスト
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock

from src.utils.data_utils import DataTypeConverter, DataValidator


class TestDataTypeConverter:
    """DataTypeConverter クラスのテスト"""
    
    def test_apply_data_types_text(self, sample_dataframe):
        """テキスト型変換テスト"""
        converter = DataTypeConverter()
        data_types = {'name': 'txt'}
        
        result = converter.apply_data_types_to_dataframe(sample_dataframe, data_types)
        
        # テキスト変換は元の値を維持
        assert result['name'].tolist() == sample_dataframe['name'].tolist()
    
    def test_apply_data_types_integer(self, sample_dataframe):
        """整数型変換テスト"""
        converter = DataTypeConverter()
        data_types = {'age': 'int'}
        
        result = converter.apply_data_types_to_dataframe(sample_dataframe, data_types)
        
        assert result['age'].dtype == 'Int64'
        assert result['age'].tolist() == [25, 30, 35, 40, 45]
    
    def test_apply_data_types_float(self):
        """浮動小数点型変換テスト"""
        df = pd.DataFrame({'value': [1, 2, 3, 4, 5]})
        converter = DataTypeConverter()
        data_types = {'value': 'float'}
        
        result = converter.apply_data_types_to_dataframe(df, data_types)
        
        assert result['value'].dtype == 'float64'
    
    def test_apply_data_types_date(self, sample_dataframe):
        """日付型変換テスト"""
        converter = DataTypeConverter()
        data_types = {'created_at': 'date'}
        
        result = converter.apply_data_types_to_dataframe(sample_dataframe, data_types)
        
        # 日付は文字列型に変換される
        assert result['created_at'].dtype == 'object'
    
    def test_apply_data_types_invalid_column(self, sample_dataframe):
        """存在しない列の変換テスト"""
        converter = DataTypeConverter()
        data_types = {'nonexistent_column': 'int'}
        
        # エラーは発生せず、元のデータフレームが返される
        result = converter.apply_data_types_to_dataframe(sample_dataframe, data_types)
        
        assert result.equals(sample_dataframe)
    
    def test_apply_data_types_conversion_error(self):
        """型変換エラーテスト"""
        df = pd.DataFrame({'text_data': ['abc', 'def', 'ghi']})
        converter = DataTypeConverter()
        data_types = {'text_data': 'int'}
        
        with pytest.raises(ValueError):
            converter.apply_data_types_to_dataframe(df, data_types)
    
    def test_format_dates_for_parquet(self):
        """Parquet用日付フォーマットテスト"""
        df = pd.DataFrame({
            'date_col': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'datetime_col': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00'])
        })
        
        converter = DataTypeConverter()
        data_types = {
            'date_col': 'date',
            'datetime_col': 'datetime'
        }
        
        result = converter.format_dates_for_parquet(df, data_types)
        
        assert result['date_col'].iloc[0] == '2023/01/01'
        assert result['datetime_col'].iloc[0] == '2023/01/01 10:00:00'


class TestDataValidator:
    """DataValidator クラスのテスト"""
    
    def test_validate_dataframe_success(self, sample_dataframe):
        """データフレーム検証成功テスト"""
        validator = DataValidator()
        rules = {
            'min_columns': 3,
            'min_rows': 3,
            'required_columns': ['id', 'name', 'age']
        }
        
        errors = validator.validate_dataframe(sample_dataframe, rules)
        
        assert len(errors) == 0
    
    def test_validate_dataframe_empty(self):
        """空のデータフレーム検証テスト"""
        empty_df = pd.DataFrame()
        validator = DataValidator()
        rules = {}
        
        errors = validator.validate_dataframe(empty_df, rules)
        
        assert len(errors) == 1
        assert "データフレームが空です" in errors[0]
    
    def test_validate_dataframe_insufficient_columns(self, sample_dataframe):
        """列数不足検証テスト"""
        validator = DataValidator()
        rules = {'min_columns': 10}
        
        errors = validator.validate_dataframe(sample_dataframe, rules)
        
        assert len(errors) == 1
        assert "列数不足" in errors[0]
    
    def test_validate_dataframe_insufficient_rows(self, sample_dataframe):
        """行数不足検証テスト"""
        validator = DataValidator()
        rules = {'min_rows': 10}
        
        errors = validator.validate_dataframe(sample_dataframe, rules)
        
        assert len(errors) == 1
        assert "行数不足" in errors[0]
    
    def test_validate_dataframe_missing_columns(self, sample_dataframe):
        """必須列不足検証テスト"""
        validator = DataValidator()
        rules = {'required_columns': ['id', 'name', 'salary']}
        
        errors = validator.validate_dataframe(sample_dataframe, rules)
        
        assert len(errors) == 1
        assert "必須列が不足" in errors[0]
        assert "salary" in errors[0]
    
    def test_validate_dataframe_null_values(self):
        """NULL値検証テスト"""
        df_with_nulls = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', None, 'Charlie', 'David', 'Eve']
        })
        
        validator = DataValidator()
        rules = {'no_null_columns': ['id', 'name']}
        
        errors = validator.validate_dataframe(df_with_nulls, rules)
        
        assert len(errors) == 2
        assert any("id" in error for error in errors)
        assert any("name" in error for error in errors)
    
    def test_validate_dataframe_data_types(self, sample_dataframe):
        """データ型検証テスト"""
        validator = DataValidator()
        rules = {
            'column_types': {
                'id': 'int',
                'name': 'object',
                'age': 'float'  # 実際はint64だが、floatを期待
            }
        }
        
        errors = validator.validate_dataframe(sample_dataframe, rules)
        
        # age列のデータ型不一致がエラーとして検出される
        assert len(errors) == 1
        assert "age" in errors[0]
        assert "データ型不一致" in errors[0]
    
    def test_check_data_quality(self, sample_dataframe):
        """データ品質チェックテスト"""
        validator = DataValidator()
        
        quality_report = validator.check_data_quality(sample_dataframe)
        
        assert quality_report['total_rows'] == 5
        assert quality_report['total_columns'] == 5
        assert quality_report['duplicate_rows'] == 0
        assert isinstance(quality_report['memory_usage_mb'], float)
        assert 'null_counts' in quality_report
        assert 'column_types' in quality_report
        
        # 数値列の統計が含まれることを確認
        assert 'numeric_stats' in quality_report
        assert 'id' in quality_report['numeric_stats']
        assert 'age' in quality_report['numeric_stats']
    
    def test_check_data_quality_with_duplicates(self):
        """重複データを含むデータ品質チェックテスト"""
        df_with_duplicates = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'name': ['A', 'B', 'B', 'C']
        })
        
        validator = DataValidator()
        quality_report = validator.check_data_quality(df_with_duplicates)
        
        assert quality_report['duplicate_rows'] == 1