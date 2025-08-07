# 開発者向けガイド

## 開発環境セットアップ

### 1. 必要なツール
- **Python**: 3.9+
- **IDE**: VSCode / PyCharm 推奨
- **Git**: バージョン管理
- **PowerShell**: スクリプト実行

### 2. 開発用依存関係
```txt
# requirements-dev.txt
# 基本依存関係
-r requirements.txt

# 開発用ツール
pytest>=7.0.0
flake8>=4.0.0
black>=22.0.0
mypy>=0.950
pre-commit>=2.17.0
pytest-cov>=3.0.0
```

### 3. IDE設定（VSCode）
```json
// .vscode/settings.json
{
    "python.defaultInterpreterPath": "./venv/Scripts/python.exe",
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"]
}
```

## コードスタイル・規約

### 1. Python コーディング規約
- **PEP 8** に準拠
- **Black** による自動フォーマット
- **flake8** による静的解析

### 2. 命名規約
```python
# ファイル名: snake_case
user_data_processor.py

# クラス名: PascalCase
class DataProcessor:
    pass

# 関数・変数名: snake_case
def process_user_data():
    user_count = 100

# 定数: UPPER_SNAKE_CASE
MAX_RETRY_COUNT = 3
```

### 3. ドキュメンテーション
```python
def process_sql_file(sql_file: str, output_format: str = 'csv') -> bool:
    """
    SQLファイルを処理してデータを出力する
    
    Args:
        sql_file (str): SQLファイルのパス
        output_format (str): 出力形式 ('csv' または 'parquet')
    
    Returns:
        bool: 処理成功時True、失敗時False
    
    Raises:
        FileNotFoundError: SQLファイルが見つからない場合
        DatabaseConnectionError: DB接続エラーの場合
    """
    pass
```

## プロジェクト構造

### 現在の構造
```
sourcecode/
├── main.py                    # エントリーポイント
├── common_exe_functions.py    # 共通処理（1270行）
├── subcode_loader.py          # データ処理コア（巨大）
├── config_loader.py           # 設定読み込み
├── my_logging.py              # ログ管理
├── streamlit_app.py           # WebUI
├── run.ps1                    # 実行スクリプト
└── config.ini                 # 設定ファイル
```

### 推奨構造（リファクタリング後）
```
src/
├── batch_system/              # 定期バッチシステム
│   ├── __init__.py
│   ├── main.py
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── csv_processor.py
│   │   └── spreadsheet_processor.py
│   └── config/
│       ├── __init__.py
│       └── batch_config.py
├── streamlit_system/          # ストミンシステム
│   ├── __init__.py
│   ├── app.py
│   ├── data_sources/
│   │   ├── __init__.py
│   │   └── parquet_generator.py
│   └── ui/
│       ├── __init__.py
│       └── components.py
├── core/                      # 共通コア機能
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py
│   │   └── ssh_tunnel.py
│   ├── google_api/
│   │   ├── __init__.py
│   │   ├── drive_client.py
│   │   └── sheets_client.py
│   ├── logging/
│   │   ├── __init__.py
│   │   └── logger.py
│   └── config/
│       ├── __init__.py
│       └── settings.py
├── utils/                     # ユーティリティ
│   ├── __init__.py
│   ├── file_utils.py
│   └── data_utils.py
└── tests/                     # テストコード
    ├── __init__.py
    ├── test_batch_system/
    ├── test_streamlit_system/
    ├── test_core/
    └── fixtures/
```

## 主要モジュールの設計

### 1. データ処理基盤
```python
# core/data_processor.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd

class DataProcessor(ABC):
    """データ処理の基底クラス"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
    
    @abstractmethod
    def process(self, source: str) -> pd.DataFrame:
        """データ処理の抽象メソッド"""
        pass

class SQLDataProcessor(DataProcessor):
    """SQL実行によるデータ処理"""
    
    def process(self, sql_file: str) -> pd.DataFrame:
        sql_query = self._load_sql_file(sql_file)
        return self._execute_query(sql_query)
    
    def _load_sql_file(self, file_path: str) -> str:
        """SQLファイルの読み込み"""
        pass
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        """SQLクエリの実行"""
        pass
```

### 2. 設定管理
```python
# core/config/settings.py
from dataclasses import dataclass
from typing import Dict, Optional
import os

@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

@dataclass
class GoogleAPIConfig:
    credentials_file: str
    spreadsheet_id: str
    drive_folder_id: str

@dataclass
class AppConfig:
    environment: str
    debug: bool
    database: DatabaseConfig
    google_api: GoogleAPIConfig
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """環境変数から設定を読み込み"""
        return cls(
            environment=os.getenv('APP_ENV', 'development'),
            debug=os.getenv('DEBUG', 'False').lower() == 'true',
            database=DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT', '3306')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME')
            ),
            google_api=GoogleAPIConfig(
                credentials_file=os.getenv('GOOGLE_CREDENTIALS'),
                spreadsheet_id=os.getenv('SPREADSHEET_ID'),
                drive_folder_id=os.getenv('DRIVE_FOLDER_ID')
            )
        )
```

### 3. エラーハンドリング
```python
# core/exceptions.py
class CSVToolException(Exception):
    """基底例外クラス"""
    
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.error_code = error_code
        self.message = message

class DatabaseConnectionError(CSVToolException):
    """データベース接続エラー"""
    pass

class GoogleAPIError(CSVToolException):
    """Google API関連エラー"""
    pass

class SQLExecutionError(CSVToolException):
    """SQL実行エラー"""
    pass

# core/error_handler.py
class ErrorHandler:
    def __init__(self, logger, notifier=None):
        self.logger = logger
        self.notifier = notifier
    
    def handle_error(self, error: Exception, context: Dict = None) -> Dict:
        """統一エラーハンドリング"""
        error_info = {
            'error_type': type(error).__name__,
            'message': str(error),
            'context': context or {}
        }
        
        self.logger.error(f"エラー発生: {error_info}")
        
        if self.notifier and isinstance(error, CSVToolException):
            self.notifier.send_error_notification(error_info)
        
        return error_info
```

## テスト戦略

### 1. テスト構成
```python
# tests/conftest.py
import pytest
from unittest.mock import Mock
from core.config.settings import AppConfig

@pytest.fixture
def mock_config():
    """テスト用設定"""
    return AppConfig(
        environment='test',
        debug=True,
        database=Mock(),
        google_api=Mock()
    )

@pytest.fixture
def sample_dataframe():
    """テスト用データフレーム"""
    import pandas as pd
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['test1', 'test2', 'test3']
    })
```

### 2. 単体テスト例
```python
# tests/test_core/test_data_processor.py
import pytest
from unittest.mock import Mock, patch
from core.data_processor import SQLDataProcessor

class TestSQLDataProcessor:
    
    def test_process_success(self, mock_config):
        """正常系テスト"""
        processor = SQLDataProcessor(mock_config, Mock())
        
        with patch.object(processor, '_load_sql_file') as mock_load:
            with patch.object(processor, '_execute_query') as mock_execute:
                mock_load.return_value = "SELECT * FROM test"
                mock_execute.return_value = Mock()
                
                result = processor.process('test.sql')
                
                assert result is not None
                mock_load.assert_called_once_with('test.sql')
                mock_execute.assert_called_once()
    
    def test_process_file_not_found(self, mock_config):
        """異常系テスト - ファイル未発見"""
        processor = SQLDataProcessor(mock_config, Mock())
        
        with patch.object(processor, '_load_sql_file') as mock_load:
            mock_load.side_effect = FileNotFoundError("File not found")
            
            with pytest.raises(FileNotFoundError):
                processor.process('nonexistent.sql')
```

### 3. 統合テスト例
```python
# tests/test_integration/test_batch_workflow.py
import pytest
from batch_system.main import BatchProcessor

class TestBatchWorkflow:
    
    @pytest.mark.integration
    def test_full_batch_execution(self, test_database, test_config):
        """バッチ処理全体のテスト"""
        processor = BatchProcessor(test_config)
        
        # テストデータセットアップ
        self._setup_test_data(test_database)
        
        # バッチ実行
        result = processor.run()
        
        # 結果検証
        assert result.success
        assert result.processed_files > 0
        
        # 出力ファイル確認
        output_files = self._get_output_files()
        assert len(output_files) > 0
```

## デバッグ・トラブルシューティング

### 1. ログ設定（デバッグ用）
```python
# core/logging/debug_logger.py
import logging
import sys

def setup_debug_logging():
    """デバッグ用ログ設定"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('debug.log')
        ]
    )
```

### 2. プロファイリング
```python
# utils/profiling.py
import cProfile
import pstats
from functools import wraps

def profile_function(func):
    """関数実行時間を測定するデコレータ"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            result = func(*args, **kwargs)
        finally:
            profiler.disable()
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            stats.print_stats(10)  # 上位10件表示
        
        return result
    return wrapper
```

### 3. メモリ使用量監視
```python
# utils/memory_monitor.py
import psutil
import pandas as pd

def monitor_memory_usage(func):
    """メモリ使用量を監視するデコレータ"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        result = func(*args, **kwargs)
        
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_diff = memory_after - memory_before
        
        print(f"Memory usage: {memory_before:.1f}MB -> {memory_after:.1f}MB (diff: {memory_diff:+.1f}MB)")
        
        return result
    return wrapper
```

## コントリビューション ガイドライン

### 1. ブランチ戦略
```
main           # 本番用ブランチ
├── develop    # 開発用ブランチ
├── feature/*  # 機能開発ブランチ
├── bugfix/*   # バグ修正ブランチ
└── hotfix/*   # 緊急修正ブランチ
```

### 2. コミットメッセージ規約
```
type(scope): subject

body

footer

# 例
feat(batch): 新しいデータ処理機能を追加

- SQLクエリの並列実行を実装
- エラーハンドリングを改善
- ログ出力を詳細化

Closes #123
```

### 3. プルリクエスト チェックリスト
- [ ] テストが通過している
- [ ] コードレビューが完了している
- [ ] ドキュメントが更新されている
- [ ] 後方互換性が保たれている
- [ ] セキュリティ上の問題がない

### 4. コードレビュー ポイント
- コードの可読性・保守性
- テストの網羅性
- パフォーマンスへの影響
- セキュリティ上の問題
- 設計パターンの適用

## パフォーマンス最適化

### 1. データベース最適化
```python
# 接続プール使用例
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)
```

### 2. 並列処理
```python
# 並列データ処理例
import concurrent.futures
import pandas as pd

def process_sql_files_parallel(sql_files, max_workers=5):
    """SQLファイルを並列処理"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_sql_file, sql_file): sql_file 
            for sql_file in sql_files
        }
        
        results = []
        for future in concurrent.futures.as_completed(future_to_file):
            sql_file = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing {sql_file}: {e}")
        
        return results
```

### 3. メモリ効率化
```python
# チャンク処理による大容量データ対応
def process_large_dataset(query, chunk_size=10000):
    """大容量データセットのチャンク処理"""
    offset = 0
    while True:
        chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
        chunk_df = pd.read_sql(chunk_query, connection)
        
        if chunk_df.empty:
            break
            
        yield chunk_df
        offset += chunk_size
```