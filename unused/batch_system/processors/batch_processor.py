"""
バッチ処理エンジン

SQLクエリ実行とCSV出力を管理するメインプロセッサー
"""
import traceback
import sys
import os
from typing import List, Dict, Any, Optional, Tuple

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.config.settings import AppConfig
from src.core.database.connection import DatabaseConnection
from src.core.database.ssh_tunnel import SSHTunnel
from src.core.logging.logger import get_logger
from datetime import datetime, timedelta, date
import time
import pyarrow as pa
import pyarrow.parquet as pq

# 新構造モジュールからインポート
from src.core.google_api.sheets_utils import (
    load_sql_file_list_from_spreadsheet,
    load_sql_from_file,
    export_to_spreadsheet,
    write_to_log_sheet
)
from src.batch_system.sql_utils import add_conditions_to_sql
from src.utils.export_utils import csvfile_export, parquetfile_export, setup_test_environment

# 一時的に残す（移行中）
from subcode_loader import set_period_condition
import slack_notify


class BatchProcessor:
    """バッチ処理エンジンクラス"""
    
    def __init__(self, config: AppConfig):
        """
        バッチプロセッサーを初期化
        
        Args:
            config: アプリケーション設定
        """
        self.config = config
        self.logger = get_logger(__name__)
        self.ssh_tunnel: Optional[SSHTunnel] = None
        self.db_connection: Optional[DatabaseConnection] = None
    
    def execute_batch(self, sheet_name: str, execution_column: str) -> bool:
        """
        バッチ処理を実行
        
        Args:
            sheet_name: Googleスプレッドシートのシート名
            execution_column: 実行対象の列名
            
        Returns:
            bool: 成功時True、失敗時False
        """
        self.logger.info("=" * 120)
        self.logger.info("バッチ処理開始")
        self.logger.info(f"シート名: '{sheet_name}'")
        self.logger.info(f"実行列: '{execution_column}'")
        self.logger.info("=" * 120)
        
        results = []
        
        try:
            # 1. SQLファイルリストを取得
            sql_files = self._load_sql_file_list(sheet_name, execution_column)
            if not sql_files:
                self.logger.warning("処理対象のSQLファイルがありません")
                return True
            
            self.logger.info(f"処理対象ファイル数: {len(sql_files)}")
            
            # 2. SSH接続とデータベース接続を確立
            if not self._establish_connections():
                return False
            
            # 3. SQLファイルを処理
            results = self._process_sql_files(sql_files)
            
            return True
            
        except Exception as e:
            self.logger.error(f"バッチ処理中にエラーが発生: {e}")
            self.logger.error(traceback.format_exc())
            self._send_error_notification(e)
            return False
            
        finally:
            self._cleanup_connections()
            self._log_results(results)
    
    def _load_sql_file_list(self, sheet_name: str, execution_column: str) -> List[Tuple]:
        """
        GoogleスプレッドシートからSQLファイルリストを読み込み
        
        Args:
            sheet_name: シート名
            execution_column: 実行対象列名
            
        Returns:
            List[Tuple]: SQLファイル情報のリスト
        """
        try:
            sql_files = load_sql_file_list_from_spreadsheet(
                self.config.google_api.spreadsheet_id,
                sheet_name,
                self.config.google_api.credentials_file,
                execution_column
            )
            self.logger.info(f"SQLファイルリストを読み込み完了: {len(sql_files)} 件")
            return sql_files
            
        except Exception as e:
            self.logger.error(f"SQLファイルリスト読み込みエラー: {e}")
            self._send_error_notification(e)
            raise
    
    def _establish_connections(self) -> bool:
        """
        SSH接続とデータベース接続を確立
        
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            # SSH設定を準備
            ssh_config = {
                'host': self.config.ssh.host,
                'user': self.config.ssh.user,
                'ssh_key_path': self.config.ssh.ssh_key_path,
                'db_host': self.config.database.host,
                'db_port': self.config.database.port,
                'local_port': self.config.ssh.local_port
            }
            
            # SSH接続を確立
            self.ssh_tunnel = SSHTunnel(ssh_config)
            if not self.ssh_tunnel.start():
                self.logger.error("SSH接続の確立に失敗")
                return False
            
            self.logger.info("SSH接続を確立しました")
            
            # データベース接続を確立
            db_config = {
                'host': self.config.database.host,
                'port': self.config.database.port,
                'user': self.config.database.user,
                'password': self.config.database.password,
                'database': self.config.database.database
            }
            
            self.db_connection = DatabaseConnection(db_config, self.ssh_tunnel.get_local_bind_port())
            conn = self.db_connection.create_connection()
            
            if not conn:
                self.logger.error("データベース接続の確立に失敗")
                return False
                
            # 接続をテスト用に保存（一時的な実装）
            self._current_connection = conn
            self.logger.info("データベース接続を確立しました")
            return True
            
        except Exception as e:
            self.logger.error(f"接続確立エラー: {e}")
            self._send_error_notification(e)
            return False
    
    def _process_sql_files(self, sql_files: List[Tuple]) -> List[str]:
        """
        SQLファイルを順次処理
        
        Args:
            sql_files: SQLファイル情報のリスト
            
        Returns:
            List[str]: 処理結果のリスト
        """
        results = []
        
        for file_info in sql_files:
            try:
                result = self._process_single_sql_file(file_info)
                results.append(result)
                
                # パフォーマンス調整のための遅延
                if self.config.tuning.delay > 0:
                    time.sleep(self.config.tuning.delay)
                    
            except Exception as e:
                error_msg = f"★失敗★ {file_info}: {str(e)}"
                self.logger.error(error_msg)
                results.append(error_msg)
        
        return results
    
    def _process_single_sql_file(self, file_info: Tuple) -> str:
        """
        単一のSQLファイルを処理
        
        Args:
            file_info: SQLファイル情報のタプル
            
        Returns:
            str: 処理結果メッセージ
        """
        try:
            # ファイル情報を展開
            (
                sql_file_name,
                csv_file_name,
                period_condition,
                period_criteria,
                save_path_id,
                output_to_spreadsheet,
                deletion_exclusion,
                paste_format,
                test_execution,
                category,
                main_table_name,
                csv_file_name_column,
                sheet_name
            ) = file_info
            
        except ValueError as e:
            error_msg = f"★失敗★ {file_info}: ファイル情報の展開エラー"
            self.logger.error(error_msg)
            return error_msg
        
        try:
            start_time = datetime.now()
            self.logger.info(f"処理開始: {sql_file_name} -> {csv_file_name}")
            
            # 旧来の処理関数を呼び出し（subcode_loaderの関数を使用）
            # TODO: これらの関数も将来的にはリファクタリング対象
            
            # SQLファイルを読み込み  
            sql_query = load_sql_from_file(
                sql_file_name,
                self.config.google_api.drive_folder_id,
                self.config.google_api.credentials_file
            )
            
            # 期間条件を適用
            if period_condition and period_condition.strip():
                sql_query = set_period_condition(
                    period_condition, 
                    period_criteria, 
                    sql_query, 
                    category
                )
                
                # 削除除外条件を適用（マスタ以外）
                if category != 'マスタ':
                    input_values, input_fields_types = {}, {}
                    sql_query = add_conditions_to_sql(
                        sql_query,
                        input_values,
                        input_fields_types,
                        deletion_exclusion
                    )
            

            
            # CSV出力パスの設定
            # save_path_idがGoogle Drive IDの場合はベースパスを使用
            if save_path_id and save_path_id.strip() and not save_path_id.startswith('1') and len(save_path_id) < 50:
                # 通常のローカルパス
                csv_file_path = os.path.join(save_path_id, csv_file_name)
            else:
                # Google Drive IDまたは無効なパスの場合はベースパスを使用
                csv_file_path = os.path.join(self.config.paths.csv_base_path, csv_file_name)
            
            csvfile_export(
                self._current_connection,
                sql_query,
                csv_file_path,
                main_table_name,
                category,
                self.config.google_api.credentials_file,
                self.config.google_api.spreadsheet_id,
                csv_file_name,
                csv_file_name_column,
                sheet_name,
                chunk_size=self.config.tuning.chunk_size,
                delay=self.config.tuning.delay
            )
            
            # スプレッドシート出力（必要な場合）
            if output_to_spreadsheet and output_to_spreadsheet.strip().upper() == 'TRUE':
                export_to_spreadsheet(
                    self._current_connection,
                    sql_query,
                    self.config.google_api.spreadsheet_id,
                    sheet_name,
                    self.config.google_api.credentials_file,
                    paste_format
                )
            
            elapsed_time = datetime.now() - start_time
            success_msg = f"★成功★ {sql_file_name} -> {csv_file_name} (処理時間: {elapsed_time})"
            self.logger.info(success_msg)
            return success_msg
            
        except Exception as e:
            error_msg = f"★失敗★ {sql_file_name}: {str(e)}"
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg
    
    def _cleanup_connections(self) -> None:
        """接続をクリーンアップ"""
        try:
            if hasattr(self, '_current_connection') and self._current_connection:
                self._current_connection.close()
                self.logger.info("データベース接続を閉じました")
                
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.logger.info("SSHトンネルを閉じました")
                
        except Exception as e:
            self.logger.error(f"接続クリーンアップエラー: {e}")
    
    def _send_error_notification(self, error: Exception) -> None:
        """エラー通知を送信"""
        try:
            # 設定を旧形式に変換してslack_notifyに渡す
            config_dict = {
                'spreadsheet_id': self.config.google_api.spreadsheet_id,
                'json_keyfile_path': self.config.google_api.credentials_file,
                # 他の必要な設定があれば追加
            }
            slack_notify.send_slack_error_message(error, config=config_dict)
        except Exception as e:
            self.logger.error(f"Slack通知送信エラー: {e}")
    
    def _log_results(self, results: List[str]) -> None:
        """処理結果をログ出力"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("処理結果一覧:")
        self.logger.info("=" * 60)
        
        for result in results:
            self.logger.info(result)
        
        success_count = len([r for r in results if "★成功★" in r])
        failure_count = len([r for r in results if "★失敗★" in r])
        
        self.logger.info("=" * 60)
        self.logger.info(f"成功: {success_count} 件, 失敗: {failure_count} 件")
        self.logger.info("=" * 60)