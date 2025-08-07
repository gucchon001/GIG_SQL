"""
バッチプロセッサーのテスト
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from src.batch_system.processors.batch_processor import BatchProcessor


class TestBatchProcessor:
    """BatchProcessor クラスのテスト"""
    
    def test_batch_processor_initialization(self, mock_config):
        """バッチプロセッサーの初期化テスト"""
        processor = BatchProcessor(mock_config)
        
        assert processor.config == mock_config
        assert processor.ssh_tunnel is None
        assert processor.db_connection is None
    
    @patch('src.batch_system.processors.batch_processor.load_sql_file_list_from_spreadsheet')
    def test_load_sql_file_list_success(self, mock_load_list, mock_config, sample_sql_files_list):
        """SQLファイルリスト読み込み成功テスト"""
        mock_load_list.return_value = sample_sql_files_list
        
        processor = BatchProcessor(mock_config)
        result = processor._load_sql_file_list('test_sheet', 'test_column')
        
        assert result == sample_sql_files_list
        mock_load_list.assert_called_once_with(
            mock_config.google_api.spreadsheet_id,
            'test_sheet',
            mock_config.google_api.credentials_file,
            'test_column'
        )
    
    @patch('src.batch_system.processors.batch_processor.load_sql_file_list_from_spreadsheet')
    def test_load_sql_file_list_failure(self, mock_load_list, mock_config):
        """SQLファイルリスト読み込み失敗テスト"""
        mock_load_list.side_effect = Exception("API Error")
        
        processor = BatchProcessor(mock_config)
        
        with pytest.raises(Exception):
            processor._load_sql_file_list('test_sheet', 'test_column')
    
    @patch('src.batch_system.processors.batch_processor.SSHTunnel')
    @patch('src.batch_system.processors.batch_processor.DatabaseConnection')
    def test_establish_connections_success(self, mock_db_class, mock_ssh_class, mock_config):
        """接続確立成功テスト"""
        # SSHトンネルのモック
        mock_ssh_instance = Mock()
        mock_ssh_instance.start.return_value = True
        mock_ssh_instance.get_local_bind_port.return_value = 3307
        mock_ssh_class.return_value = mock_ssh_instance
        
        # データベース接続のモック
        mock_db_instance = Mock()
        mock_connection = Mock()
        mock_db_instance.create_connection.return_value = mock_connection
        mock_db_class.return_value = mock_db_instance
        
        processor = BatchProcessor(mock_config)
        result = processor._establish_connections()
        
        assert result is True
        assert processor.ssh_tunnel == mock_ssh_instance
        assert processor.db_connection == mock_db_instance
        mock_ssh_instance.start.assert_called_once()
        mock_db_instance.create_connection.assert_called_once()
    
    @patch('src.batch_system.processors.batch_processor.SSHTunnel')
    def test_establish_connections_ssh_failure(self, mock_ssh_class, mock_config):
        """SSH接続失敗テスト"""
        mock_ssh_instance = Mock()
        mock_ssh_instance.start.return_value = False
        mock_ssh_class.return_value = mock_ssh_instance
        
        processor = BatchProcessor(mock_config)
        result = processor._establish_connections()
        
        assert result is False
    
    @patch('src.batch_system.processors.batch_processor.SSHTunnel')
    @patch('src.batch_system.processors.batch_processor.DatabaseConnection')
    def test_establish_connections_db_failure(self, mock_db_class, mock_ssh_class, mock_config):
        """データベース接続失敗テスト"""
        # SSHトンネルは成功
        mock_ssh_instance = Mock()
        mock_ssh_instance.start.return_value = True
        mock_ssh_instance.get_local_bind_port.return_value = 3307
        mock_ssh_class.return_value = mock_ssh_instance
        
        # データベース接続は失敗
        mock_db_instance = Mock()
        mock_db_instance.create_connection.return_value = None
        mock_db_class.return_value = mock_db_instance
        
        processor = BatchProcessor(mock_config)
        result = processor._establish_connections()
        
        assert result is False
        mock_ssh_instance.stop.assert_called_once()
    
    def test_cleanup_connections(self, mock_config):
        """接続クリーンアップテスト"""
        processor = BatchProcessor(mock_config)
        
        # モック接続を設定
        mock_connection = Mock()
        processor._current_connection = mock_connection
        
        mock_ssh_tunnel = Mock()
        processor.ssh_tunnel = mock_ssh_tunnel
        
        # クリーンアップ実行
        processor._cleanup_connections()
        
        mock_connection.close.assert_called_once()
        mock_ssh_tunnel.stop.assert_called_once()
    
    def test_process_sql_files_empty_list(self, mock_config):
        """空のSQLファイルリスト処理テスト"""
        processor = BatchProcessor(mock_config)
        result = processor._process_sql_files([])
        
        assert result == []
    
    @patch.object(BatchProcessor, '_process_single_sql_file')
    def test_process_sql_files_success(self, mock_process_single, mock_config, sample_sql_files_list):
        """SQLファイルリスト処理成功テスト"""
        mock_process_single.side_effect = [
            "★成功★ test1.sql",
            "★成功★ test2.sql"
        ]
        
        processor = BatchProcessor(mock_config)
        result = processor._process_sql_files(sample_sql_files_list)
        
        assert len(result) == 2
        assert all("★成功★" in r for r in result)
        assert mock_process_single.call_count == 2
    
    @patch.object(BatchProcessor, '_process_single_sql_file')
    def test_process_sql_files_with_error(self, mock_process_single, mock_config, sample_sql_files_list):
        """SQLファイル処理でエラーが発生するテスト"""
        mock_process_single.side_effect = [
            "★成功★ test1.sql",
            Exception("SQL Error")
        ]
        
        processor = BatchProcessor(mock_config)
        result = processor._process_sql_files(sample_sql_files_list)
        
        assert len(result) == 2
        assert "★成功★" in result[0]
        assert "★失敗★" in result[1]


@pytest.mark.integration
class TestBatchProcessorIntegration:
    """バッチプロセッサーの統合テスト"""
    
    @patch('src.batch_system.processors.batch_processor.load_sql_file_list_from_spreadsheet')
    @patch.object(BatchProcessor, '_establish_connections')
    @patch.object(BatchProcessor, '_process_sql_files')
    @patch.object(BatchProcessor, '_cleanup_connections')
    def test_execute_batch_full_flow(
        self,
        mock_cleanup,
        mock_process,
        mock_establish,
        mock_load_list,
        mock_config,
        sample_sql_files_list
    ):
        """バッチ実行の全体フロー統合テスト"""
        # モックの設定
        mock_load_list.return_value = sample_sql_files_list
        mock_establish.return_value = True
        mock_process.return_value = ["★成功★ test1.sql", "★成功★ test2.sql"]
        
        processor = BatchProcessor(mock_config)
        result = processor.execute_batch('test_sheet', 'test_column')
        
        # 実行結果の検証
        assert result is True
        mock_load_list.assert_called_once_with('test_sheet', 'test_column')
        mock_establish.assert_called_once()
        mock_process.assert_called_once_with(sample_sql_files_list)
        mock_cleanup.assert_called_once()
    
    @patch('src.batch_system.processors.batch_processor.load_sql_file_list_from_spreadsheet')
    @patch.object(BatchProcessor, '_establish_connections')
    def test_execute_batch_connection_failure(
        self,
        mock_establish,
        mock_load_list,
        mock_config,
        sample_sql_files_list
    ):
        """接続失敗時のバッチ実行テスト"""
        mock_load_list.return_value = sample_sql_files_list
        mock_establish.return_value = False
        
        processor = BatchProcessor(mock_config)
        result = processor.execute_batch('test_sheet', 'test_column')
        
        assert result is False