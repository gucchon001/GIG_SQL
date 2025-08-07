"""
コア設定機能のテスト
"""
import pytest
import tempfile
import configparser
import os
from unittest.mock import patch, mock_open

from src.core.config.settings import AppConfig, DatabaseConfig, SSHConfig


class TestAppConfig:
    """AppConfig クラスのテスト"""
    
    def test_app_config_creation(self, mock_config):
        """設定オブジェクトの作成テスト"""
        assert mock_config.environment == 'test'
        assert mock_config.debug is True
        assert mock_config.ssh.host == 'test-host'
        assert mock_config.database.user == 'test_user'
    
    def test_from_config_file_success(self):
        """設定ファイルからの読み込み成功テスト"""
        config_content = """
[SSH]
host = test-server
user = testuser
ssh_key_path = /path/to/key

[MySQL]
host = db-server
port = 3306
user = dbuser
password = dbpass
database = testdb

[Spreadsheet]
spreadsheet_id = test_sheet_id
main_sheet = main
rawdata_sheet = rawdata
eachdata_sheet = eachdata

[Credentials]
json_keyfile_path = /path/to/creds.json

[GoogleDrive]
google_folder_id = test_folder_id

[Paths]
csv_base_path = /test/csv

[Tuning]
chunk_size = 5000
batch_size = 500
delay = 0.2
max_workers = 3

[logging]
level = INFO
logfile = test.log
"""
        
        with patch('builtins.open', mock_open(read_data=config_content)):
            with patch('os.path.exists', return_value=True):
                config = AppConfig.from_config_file('test_config.ini')
                
                assert config.ssh.host == 'test-server'
                assert config.database.database == 'testdb'
                assert config.tuning.chunk_size == 5000
    
    def test_from_config_file_not_found(self):
        """設定ファイルが見つからない場合のテスト"""
        with patch('os.path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                AppConfig.from_config_file('nonexistent.ini')
    
    def test_from_config_file_invalid_format(self):
        """設定ファイル形式が不正な場合のテスト"""
        invalid_content = "invalid config content"
        
        with patch('builtins.open', mock_open(read_data=invalid_content)):
            with patch('os.path.exists', return_value=True):
                with pytest.raises(configparser.Error):
                    AppConfig.from_config_file('invalid_config.ini')


class TestDatabaseConfig:
    """DatabaseConfig クラスのテスト"""
    
    def test_database_config_creation(self):
        """データベース設定の作成テスト"""
        db_config = DatabaseConfig(
            host='localhost',
            port=3306,
            user='testuser',
            password='testpass',
            database='testdb'
        )
        
        assert db_config.host == 'localhost'
        assert db_config.port == 3306
        assert db_config.user == 'testuser'
        assert db_config.password == 'testpass'
        assert db_config.database == 'testdb'


class TestSSHConfig:
    """SSHConfig クラスのテスト"""
    
    def test_ssh_config_creation(self):
        """SSH設定の作成テスト"""
        ssh_config = SSHConfig(
            host='ssh-server',
            user='sshuser',
            ssh_key_path='/path/to/key'
        )
        
        assert ssh_config.host == 'ssh-server'
        assert ssh_config.user == 'sshuser'
        assert ssh_config.ssh_key_path == '/path/to/key'
        assert ssh_config.db_host == "127.0.0.1"  # デフォルト値
        assert ssh_config.db_port == 3306  # デフォルト値
        assert ssh_config.local_port == 3306  # デフォルト値
    
    def test_ssh_config_custom_values(self):
        """SSH設定のカスタム値テスト"""
        ssh_config = SSHConfig(
            host='custom-server',
            user='customuser',
            ssh_key_path='/custom/key',
            db_host='custom-db',
            db_port=3307,
            local_port=3308
        )
        
        assert ssh_config.db_host == 'custom-db'
        assert ssh_config.db_port == 3307
        assert ssh_config.local_port == 3308