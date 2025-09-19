"""
pytest設定とフィクスチャ

テスト全体で使用する共通設定とフィクスチャを定義
"""
import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, MagicMock
import sys

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.config.settings import AppConfig, DatabaseConfig, SSHConfig, GoogleAPIConfig


@pytest.fixture
def mock_config():
    """テスト用のモック設定"""
    return AppConfig(
        environment='test',
        debug=True,
        ssh=SSHConfig(
            host='test-host',
            user='test-user',
            ssh_key_path='/path/to/test/key'
        ),
        database=DatabaseConfig(
            host='localhost',
            port=3306,
            user='test_user',
            password='test_password',
            database='test_db'
        ),
        google_api=GoogleAPIConfig(
            credentials_file='/path/to/test/credentials.json',
            spreadsheet_id='test_spreadsheet_id',
            drive_folder_id='test_folder_id',
            main_sheet='test_main_sheet',
            rawdata_sheet='test_rawdata_sheet',
            eachdata_sheet='test_eachdata_sheet'
        ),
        paths=Mock(csv_base_path='/test/csv/path', config_file='test_config.ini'),
        tuning=Mock(chunk_size=1000, batch_size=100, delay=0.1, max_workers=2),
        logging=Mock(level='DEBUG', logfile='test.log')
    )


@pytest.fixture
def sample_dataframe():
    """テスト用のサンプルデータフレーム"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'david@test.com', 'eve@test.com'],
        'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])
    })


@pytest.fixture
def large_dataframe():
    """大容量データ用のサンプルデータフレーム"""
    import numpy as np
    
    size = 10000
    return pd.DataFrame({
        'id': range(1, size + 1),
        'value': np.random.randn(size),
        'category': np.random.choice(['A', 'B', 'C'], size),
        'timestamp': pd.date_range('2023-01-01', periods=size, freq='H')
    })


@pytest.fixture
def temp_directory():
    """一時ディレクトリ"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_database_connection():
    """モックデータベース接続"""
    mock_conn = Mock()
    mock_conn.is_connected.return_value = True
    mock_conn.ping.return_value = True
    mock_conn.close.return_value = None
    return mock_conn


@pytest.fixture
def mock_ssh_tunnel():
    """モックSSHトンネル"""
    mock_tunnel = Mock()
    mock_tunnel.start.return_value = True
    mock_tunnel.stop.return_value = None
    mock_tunnel.is_active = True
    mock_tunnel.local_bind_port = 3307
    mock_tunnel.get_local_bind_port.return_value = 3307
    return mock_tunnel


@pytest.fixture
def mock_google_sheets_client():
    """モックGoogle Sheetsクライアント"""
    mock_client = Mock()
    mock_worksheet = Mock()
    mock_worksheet.get_all_records.return_value = [
        {
            'sqlファイル名': 'test.sql',
            'CSVファイル名/SSシート名': 'test.csv',
            '実行対象': 'TRUE',
            'カテゴリ': 'テスト'
        }
    ]
    mock_client.get_worksheet.return_value = mock_worksheet
    return mock_client


@pytest.fixture
def mock_google_drive_client():
    """モックGoogle Driveクライアント"""
    mock_client = Mock()
    mock_client.load_sql_file.return_value = "SELECT * FROM test_table;"
    mock_client.search_files.return_value = [
        {'id': 'test_file_id', 'name': 'test.sql'}
    ]
    return mock_client


@pytest.fixture
def sample_sql_files_list():
    """テスト用SQLファイルリスト"""
    return [
        (
            'test1.sql',      # sqlファイル名
            'test1.csv',      # CSVファイル名
            '前日',           # 取得期間
            '登録日時',       # 取得基準
            'test_path',      # 保存先PATH/ID
            'CSV',            # 出力先
            'FALSE',          # 削除R除外
            '値のみ',         # スプシ貼り付け形式
            'FALSE',          # テスト
            'データ',         # カテゴリ
            'test_table',     # メインテーブル
            'test1',          # CSVファイル呼称
            'test_sheet'      # シート名
        ),
        (
            'test2.sql',
            'test2.csv',
            '当日',
            '更新日時',
            'test_path',
            'スプシ',
            'TRUE',
            '数式',
            'TRUE',
            'マスタ',
            'master_table',
            'test2',
            'master_sheet'
        )
    ]


@pytest.fixture(autouse=True)
def setup_test_environment():
    """テスト環境の自動セットアップ"""
    # テスト用のログレベルを設定
    import logging
    logging.getLogger().setLevel(logging.WARNING)
    
    # テスト開始前の処理
    yield
    
    # テスト終了後の清理処理
    pass


# テスト用のマーカー定義
def pytest_configure(config):
    """pytestの設定"""
    config.addinivalue_line(
        "markers", "unit: 単体テスト用マーカー"
    )
    config.addinivalue_line(
        "markers", "integration: 統合テスト用マーカー"
    )
    config.addinivalue_line(
        "markers", "slow: 実行時間が長いテスト用マーカー"
    )
    config.addinivalue_line(
        "markers", "external: 外部サービス依存テスト用マーカー"
    )