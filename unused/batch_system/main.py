"""
バッチシステム統合エントリーポイント

定期バッチ、テスト実行、生データ処理を統合管理
"""
import sys
import os
from typing import Optional, Tuple
from enum import Enum

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.config.settings import AppConfig
from src.core.logging.logger import get_logger

# パッケージ実行と直接実行の両方に対応
try:
    from src.batch_system.processors.batch_processor import BatchProcessor
except ImportError:
    from batch_system.processors.batch_processor import BatchProcessor


class ExecutionMode(Enum):
    """実行モード"""
    PRODUCTION = "production"    # 本番実行
    TEST = "test"               # テスト実行
    RAWDATA = "rawdata"         # 生データ処理


class BatchMain:
    """バッチシステムメインクラス"""
    
    def __init__(self, config_file: str = "config.ini"):
        """
        バッチシステムを初期化
        
        Args:
            config_file: 設定ファイルパス
        """
        self.config = AppConfig.from_config_file(config_file)
        self.logger = get_logger(__name__)
        self.processor = BatchProcessor(self.config)
    
    def run(self, mode: ExecutionMode, execution_column: Optional[str] = None) -> bool:
        """
        バッチ処理を実行
        
        Args:
            mode: 実行モード
            execution_column: 実行対象列名（指定されない場合はモードから自動決定）
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            # モードに応じた設定
            sheet_name, exec_column = self._get_execution_config(mode, execution_column)
            
            self.logger.info(f"バッチ処理開始: mode={mode.value}, sheet={sheet_name}, column={exec_column}")
            
            # バッチ処理実行
            success = self.processor.execute_batch(sheet_name, exec_column)
            
            if success:
                self.logger.info("バッチ処理が正常に完了しました")
            else:
                self.logger.error("バッチ処理中にエラーが発生しました")
            
            return success
            
        except Exception as e:
            self.logger.error(f"バッチ処理中に予期しないエラー: {e}")
            return False
    
    def _get_execution_config(self, mode: ExecutionMode, execution_column: Optional[str]) -> Tuple[str, str]:
        """
        実行モードに応じた設定を取得
        
        Args:
            mode: 実行モード
            execution_column: 実行対象列名
            
        Returns:
            tuple: (sheet_name, execution_column)
        """
        if mode == ExecutionMode.PRODUCTION:
            sheet_name = self.config.google_api.main_sheet
            exec_column = execution_column or "実行対象"
        elif mode == ExecutionMode.TEST:
            sheet_name = self.config.google_api.main_sheet
            exec_column = execution_column or "テスト実行"
        elif mode == ExecutionMode.RAWDATA:
            sheet_name = self.config.google_api.rawdata_sheet
            exec_column = execution_column or "テスト実行"
        else:
            raise ValueError(f"サポートされていない実行モード: {mode}")
        
        return sheet_name, exec_column


def main_production(config_file: str = "config.ini") -> bool:
    """本番バッチ実行（旧main.py互換）"""
    batch_main = BatchMain(config_file)
    return batch_main.run(ExecutionMode.PRODUCTION)


def main_test(config_file: str = "config.ini") -> bool:
    """テストバッチ実行（旧main_test.py互換）"""
    batch_main = BatchMain(config_file)
    return batch_main.run(ExecutionMode.TEST)


def main_rawdata(config_file: str = "config.ini") -> bool:
    """生データ処理実行（旧main_rawdata.py互換）"""
    batch_main = BatchMain(config_file)
    return batch_main.run(ExecutionMode.RAWDATA)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="バッチシステム実行")
    parser.add_argument(
        "--mode", 
        choices=["production", "test", "rawdata"],
        default="production",
        help="実行モード"
    )
    parser.add_argument(
        "--config", 
        default="config.ini",
        help="設定ファイルパス"
    )
    parser.add_argument(
        "--execution-column",
        help="実行対象列名（オプション）"
    )
    
    args = parser.parse_args()
    
    try:
        mode = ExecutionMode(args.mode)
        batch_main = BatchMain(args.config)
        success = batch_main.run(mode, args.execution_column)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)