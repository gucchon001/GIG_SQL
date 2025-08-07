"""
バッチシステム固有の設定管理

バッチ処理に特化した設定とユーティリティを提供
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum


class BatchMode(Enum):
    """バッチ実行モード"""
    PRODUCTION = "production"
    TEST = "test"
    RAWDATA = "rawdata"


class OutputFormat(Enum):
    """出力形式"""
    CSV = "csv"
    PARQUET = "parquet"
    SPREADSHEET = "spreadsheet"


@dataclass
class BatchJobConfig:
    """個別バッチジョブの設定"""
    sql_file_name: str
    csv_file_name: str
    period_condition: Optional[str] = None
    period_criteria: Optional[str] = None
    save_path_id: Optional[str] = None
    output_to_spreadsheet: bool = False
    deletion_exclusion: bool = False
    paste_format: Optional[str] = None
    test_execution: bool = False
    category: Optional[str] = None
    main_table_name: Optional[str] = None
    csv_file_name_column: Optional[str] = None
    sheet_name: Optional[str] = None


@dataclass
class BatchExecutionConfig:
    """バッチ実行の設定"""
    mode: BatchMode
    sheet_name: str
    execution_column: str
    output_formats: List[OutputFormat]
    max_parallel_jobs: int = 1
    retry_count: int = 3
    timeout_seconds: int = 3600
    enable_notifications: bool = True


class BatchConfigValidator:
    """バッチ設定の検証クラス"""
    
    @staticmethod
    def validate_job_config(job_config: BatchJobConfig) -> List[str]:
        """
        ジョブ設定を検証
        
        Args:
            job_config: 検証するジョブ設定
            
        Returns:
            List[str]: エラーメッセージのリスト（空なら検証成功）
        """
        errors = []
        
        if not job_config.sql_file_name:
            errors.append("SQLファイル名が指定されていません")
        
        if not job_config.csv_file_name:
            errors.append("CSVファイル名が指定されていません")
        
        if job_config.output_to_spreadsheet and not job_config.sheet_name:
            errors.append("スプレッドシート出力が有効ですが、シート名が指定されていません")
        
        return errors
    
    @staticmethod
    def validate_execution_config(exec_config: BatchExecutionConfig) -> List[str]:
        """
        実行設定を検証
        
        Args:
            exec_config: 検証する実行設定
            
        Returns:
            List[str]: エラーメッセージのリスト（空なら検証成功）
        """
        errors = []
        
        if not exec_config.sheet_name:
            errors.append("シート名が指定されていません")
        
        if not exec_config.execution_column:
            errors.append("実行列が指定されていません")
        
        if exec_config.max_parallel_jobs < 1:
            errors.append("最大並列ジョブ数は1以上である必要があります")
        
        if exec_config.retry_count < 0:
            errors.append("リトライ回数は0以上である必要があります")
        
        if exec_config.timeout_seconds < 1:
            errors.append("タイムアウト時間は1秒以上である必要があります")
        
        return errors


class BatchMetrics:
    """バッチ処理のメトリクス管理"""
    
    def __init__(self):
        self.total_jobs = 0
        self.successful_jobs = 0
        self.failed_jobs = 0
        self.start_time = None
        self.end_time = None
        self.job_details = []
    
    def start_batch(self, total_jobs: int) -> None:
        """バッチ処理開始"""
        from datetime import datetime
        self.total_jobs = total_jobs
        self.start_time = datetime.now()
        self.successful_jobs = 0
        self.failed_jobs = 0
        self.job_details = []
    
    def record_job_success(self, job_name: str, duration_seconds: float) -> None:
        """ジョブ成功を記録"""
        self.successful_jobs += 1
        self.job_details.append({
            'name': job_name,
            'status': 'success',
            'duration': duration_seconds
        })
    
    def record_job_failure(self, job_name: str, error_message: str) -> None:
        """ジョブ失敗を記録"""
        self.failed_jobs += 1
        self.job_details.append({
            'name': job_name,
            'status': 'failure',
            'error': error_message
        })
    
    def finish_batch(self) -> None:
        """バッチ処理終了"""
        from datetime import datetime
        self.end_time = datetime.now()
    
    def get_summary(self) -> Dict:
        """バッチ処理のサマリーを取得"""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'total_jobs': self.total_jobs,
            'successful_jobs': self.successful_jobs,
            'failed_jobs': self.failed_jobs,
            'success_rate': self.successful_jobs / self.total_jobs if self.total_jobs > 0 else 0,
            'duration_seconds': duration,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'job_details': self.job_details
        }