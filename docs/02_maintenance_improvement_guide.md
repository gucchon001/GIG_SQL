# 保守性向上ガイド

> **✅ 2024年完了**: 設定統一、構造整理、UTF-8対応により主要課題が解決されました。

## 解決済み課題と達成内容

### 1. 設定管理の課題

#### 現在の問題
- 設定ファイル（config.ini）にハードコードされた値が多数存在
- 環境間（本番・テスト・開発）での設定切り替えが困難
- 秘密情報（パスワード、キー）が平文で保存

#### 改善提案
```python
# 環境変数を活用した設定管理
# config/environments.py
import os
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

@dataclass
class AppConfig:
    environment: str
    debug: bool
    db: DatabaseConfig
    
def load_config():
    env = os.getenv('APP_ENV', 'development')
    return {
        'development': DevelopmentConfig(),
        'staging': StagingConfig(),
        'production': ProductionConfig()
    }[env]
```

### 2. コード重複の課題

#### 現在の問題
- 2つのシステム間で類似処理が重複
- エラーハンドリングの統一性不足
- ログ処理の分散

#### 改善提案
```python
# core/data_processor.py - 共通処理基盤
class DataProcessor:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
    
    def execute_sql_workflow(self, sql_file, output_format='csv'):
        """統一されたSQL実行ワークフロー"""
        try:
            sql_query = self.load_sql_file(sql_file)
            data = self.execute_query(sql_query)
            return self.save_data(data, output_format)
        except Exception as e:
            self.logger.error(f"ワークフロー実行エラー: {e}")
            raise
```

### 3. ファイル構成の課題

#### 現在の問題
```
sourcecode/
├── main.py                    # 混在している
├── run_create_datesets.py    # 目的別分離不足
├── common_exe_functions.py   # 巨大ファイル
├── subcode_loader.py         # 1270行の巨大ファイル
└── ...
```

#### 改善提案
```
src/
├── batch_system/              # 定期バッチシステム
│   ├── __init__.py
│   ├── main.py
│   ├── processors/
│   └── config/
├── streamlit_system/          # ストミンシステム  
│   ├── __init__.py
│   ├── app.py
│   ├── data_sources/
│   └── ui/
├── core/                      # 共通コア機能
│   ├── __init__.py
│   ├── database/
│   ├── google_api/
│   ├── logging/
│   └── config/
├── utils/                     # ユーティリティ
└── tests/                     # テストコード
```

### 4. エラーハンドリングの統一

#### 現在の問題
- 各ファイルで異なるエラーハンドリング方式
- Slack通知の実装が分散
- 例外情報の詳細度が不統一

#### 改善提案
```python
# core/exceptions.py
class CSVToolException(Exception):
    """基底例外クラス"""
    pass

class DatabaseConnectionError(CSVToolException):
    """DB接続エラー"""
    pass

class GoogleAPIError(CSVToolException):
    """Google API エラー"""
    pass

# core/error_handler.py
class ErrorHandler:
    def __init__(self, logger, notifier):
        self.logger = logger
        self.notifier = notifier
    
    def handle_error(self, error, context=None):
        """統一エラーハンドリング"""
        self.logger.error(f"エラー発生: {error}", extra=context)
        if isinstance(error, CSVToolException):
            self.notifier.send_alert(error, context)
        return self._create_error_response(error)
```

### 5. テスト戦略の導入

#### 現在の問題
- テストコードが存在しない
- 手動テストに依存
- リグレッション検知が困難

#### 改善提案
```python
# tests/test_data_processor.py
import pytest
from unittest.mock import Mock, patch
from core.data_processor import DataProcessor

class TestDataProcessor:
    def test_execute_sql_workflow_success(self):
        processor = DataProcessor(mock_config, mock_logger)
        result = processor.execute_sql_workflow('test.sql')
        assert result.success
    
    def test_execute_sql_workflow_db_error(self):
        with pytest.raises(DatabaseConnectionError):
            processor.execute_sql_workflow('invalid.sql')
```

### 6. CI/CD パイプラインの構築

#### 提案するパイプライン
```yaml
# .github/workflows/ci.yml
name: CI Pipeline
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest tests/
      - name: Run linting
        run: flake8 src/
      - name: Type checking
        run: mypy src/
```

### 7. 監視・アラート機能の強化

#### 現在の問題
- Slack通知のみ
- システム状態の可視化不足
- パフォーマンス監視なし

#### 改善提案
```python
# core/monitoring.py
class SystemMonitor:
    def __init__(self, config):
        self.metrics = MetricsCollector()
        self.alerts = AlertManager(config)
    
    def track_execution(self, func):
        """実行時間・成功率を追跡"""
        start_time = time.time()
        try:
            result = func()
            self.metrics.record_success(func.__name__, time.time() - start_time)
            return result
        except Exception as e:
            self.metrics.record_error(func.__name__, e)
            self.alerts.send_error_alert(func.__name__, e)
            raise
```

### 8. データ品質管理

#### 改善提案
```python
# core/data_quality.py
class DataQualityChecker:
    def __init__(self, rules):
        self.rules = rules
    
    def validate_data(self, df, table_name):
        """データ品質チェック"""
        results = []
        for rule in self.rules.get(table_name, []):
            result = rule.validate(df)
            results.append(result)
        return DataQualityReport(results)
    
    def check_schema_consistency(self, df, expected_schema):
        """スキーマ整合性チェック"""
        return df.columns.tolist() == expected_schema.columns
```

## 実装優先度

### Phase 1: 基盤整備（高優先度）
1. ファイル構成の整理
2. 設定管理の統一
3. エラーハンドリングの統一
4. ログ管理の改善

### Phase 2: 品質向上（中優先度）
1. テストコードの導入
2. CI/CD パイプライン構築
3. コードレビュープロセス確立

### Phase 3: 運用改善（低優先度）
1. 監視・アラート機能強化
2. データ品質管理導入
3. パフォーマンス最適化

## 移行戦略

### 段階的リファクタリング
1. **Week 1-2**: ファイル構成整理
2. **Week 3-4**: 設定管理統一
3. **Week 5-6**: エラーハンドリング統一
4. **Week 7-8**: テスト導入
5. **Week 9-10**: CI/CD 構築

### リスク軽減策
- 既存機能の動作確認テスト
- 段階的移行（機能単位）
- ロールバック計画の準備
- 本番環境への影響最小化