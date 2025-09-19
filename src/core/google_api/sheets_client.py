"""
Google Sheets API クライント

Google Sheets APIを使用したスプレッドシート操作機能を提供
"""
import sys
import os
from typing import Optional, List, Dict, Any, Tuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.logging.logger import get_logger


class GoogleSheetsClient:
    """Google Sheets API クライアントクラス"""
    
    def __init__(self, credentials_file: str):
        """
        Google Sheets クライアントを初期化
        
        Args:
            credentials_file: 認証情報JSONファイルのパス
        """
        self.credentials_file = credentials_file
        self.logger = get_logger(__name__)
        self.scopes = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self._gc = None
    
    def _authenticate(self):
        """Google Sheets認証を実行"""
        if self._gc is None:
            try:
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    self.credentials_file,
                    self.scopes
                )
                self._gc = gspread.authorize(credentials)
                self.logger.info("Google Sheets認証が完了しました")
            except Exception as e:
                self.logger.error(f"Google Sheets認証エラー: {e}")
                raise
    
    def _get_client(self):
        """Google Sheets クライアントオブジェクトを取得"""
        if self._gc is None:
            self._authenticate()
        return self._gc
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def get_worksheet(self, spreadsheet_id: str, sheet_name: str):
        """
        ワークシートオブジェクトを取得
        
        Args:
            spreadsheet_id: スプレッドシートID
            sheet_name: シート名
            
        Returns:
            gspread.Worksheet: ワークシートオブジェクト
        """
        try:
            gc = self._get_client()
            spreadsheet = gc.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            self.logger.info(f"ワークシート取得完了: {sheet_name}")
            return worksheet
            
        except Exception as e:
            self.logger.error(f"ワークシート取得エラー: {sheet_name}, {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def load_sql_file_list(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        execution_column: str
    ) -> List[Tuple]:
        """
        スプレッドシートからSQLファイルリストを読み込み
        
        Args:
            spreadsheet_id: スプレッドシートID
            sheet_name: シート名
            execution_column: 実行対象列名
            
        Returns:
            List[Tuple]: SQLファイル情報のタプルリスト
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
            records = worksheet.get_all_records()
            
            self.logger.info(f"シート読み込み完了: {sheet_name}, {len(records)} 件")
            
            # 列名定義
            column_mapping = {
                'sql_file': 'sqlファイル名',
                'csv_file': 'CSVファイル名/SSシート名',
                'period_condition': '取得期間',
                'period_criteria': '取得基準',
                'save_path': '保存先PATH/ID',
                'output_spreadsheet': '出力先',
                'deletion_exclusion': '削除R除外',
                'paste_format': 'スプシ貼り付け形式',
                'test_execution': 'テスト',
                'category': 'カテゴリ',
                'main_table': 'メインテーブル',
                'csv_name_column': 'CSVファイル呼称',
                'sheet_name_column': 'シート名'
            }
            
            sql_files = []
            today = datetime.now().date()
            
            for record in records:
                # 実行対象をチェック
                execution_flag = str(record.get(execution_column, '')).strip().upper()
                if execution_flag in ['TRUE', '1', 'YES']:
                    # データを取得してタプルに変換
                    file_info = (
                        record.get(column_mapping['sql_file'], ''),
                        record.get(column_mapping['csv_file'], ''),
                        record.get(column_mapping['period_condition'], ''),
                        record.get(column_mapping['period_criteria'], ''),
                        record.get(column_mapping['save_path'], ''),
                        record.get(column_mapping['output_spreadsheet'], ''),
                        record.get(column_mapping['deletion_exclusion'], ''),
                        record.get(column_mapping['paste_format'], ''),
                        record.get(column_mapping['test_execution'], ''),
                        record.get(column_mapping['category'], ''),
                        record.get(column_mapping['main_table'], ''),
                        record.get(column_mapping['csv_name_column'], ''),
                        record.get(column_mapping['sheet_name_column'], '')
                    )
                    sql_files.append(file_info)
            
            self.logger.info(f"実行対象SQLファイル: {len(sql_files)} 件")
            return sql_files
            
        except Exception as e:
            self.logger.error(f"SQLファイルリスト読み込みエラー: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def write_data_to_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        data: List[List[Any]],
        paste_format: str = "値のみ"
    ) -> bool:
        """
        スプレッドシートにデータを書き込み
        
        Args:
            spreadsheet_id: スプレッドシートID
            sheet_name: シート名
            data: 書き込みデータ
            paste_format: 貼り付け形式
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
            
            # シートをクリア
            worksheet.clear()
            
            # データを書き込み
            if data:
                # バッチアップデートで効率的に書き込み
                worksheet.update(f'A1:{self._get_range_notation(len(data), len(data[0]))}', data)
                
                self.logger.info(f"データ書き込み完了: {sheet_name}, {len(data)} 行")
                return True
            else:
                self.logger.warning(f"書き込みデータが空です: {sheet_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"データ書き込みエラー: {sheet_name}, {e}")
            return False
    
    def _get_range_notation(self, rows: int, cols: int) -> str:
        """
        範囲記法を生成 (例: A1:C10)
        
        Args:
            rows: 行数
            cols: 列数
            
        Returns:
            str: 範囲記法
        """
        from openpyxl.utils import get_column_letter
        end_col = get_column_letter(cols)
        return f"A1:{end_col}{rows}"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=60, min=60, max=300),
        before_sleep=before_sleep_log
    )
    def write_log_entry(
        self,
        log_spreadsheet_id: str,
        log_sheet_name: str,
        csv_file_name: str,
        sheet_name: str,
        main_table_name: str,
        category: str,
        record_count: int,
        result: str,
        error_log: Optional[str] = None,
        save_path_id: Optional[str] = None
    ) -> bool:
        """
        ログエントリをスプレッドシートに書き込み
        
        Args:
            log_spreadsheet_id: ログスプレッドシートID
            log_sheet_name: ログシート名
            csv_file_name: CSVファイル名
            sheet_name: シート名
            main_table_name: メインテーブル名
            category: カテゴリ
            record_count: レコード数
            result: 実行結果
            error_log: エラーログ
            save_path_id: 保存先パスID
            
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            worksheet = self.get_worksheet(log_spreadsheet_id, log_sheet_name)
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_data = [
                csv_file_name,
                sheet_name,
                main_table_name,
                category,
                save_path_id or '',
                record_count,
                result,
                error_log or '',
                timestamp,
                ''  # 予備列
            ]
            
            # ログデータの型チェック
            self.logger.debug("ログエントリ書き込みデータの型チェック:")
            for idx, item in enumerate(row_data):
                item_type = type(item).__name__
                self.logger.debug(f"  項目{idx+1}: {item_type} = {str(item)[:100]}")
                if item_type not in ['str', 'int', 'float', 'bool', 'NoneType']:
                    self.logger.warning(f"想定外の型が検出: 項目{idx+1}: {item_type} = {item}")
            
            # ログを追加
            worksheet.append_row(row_data)
            
            self.logger.info(f"ログエントリ書き込み完了: {csv_file_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"ログエントリ書き込みエラー: {e}")
            return False
    
    def get_data_types_from_sheet(self, worksheet) -> Dict[str, str]:
        """
        スプレッドシートからデータ型情報を取得
        
        Args:
            worksheet: ワークシートオブジェクト
            
        Returns:
            Dict[str, str]: データ型マッピング
        """
        try:
            headers = worksheet.row_values(1)
            data_types = {}
            
            db_item_col = None
            data_type_col = None
            
            # 列インデックスを特定
            for i, header in enumerate(headers):
                if header == 'DB項目':
                    db_item_col = i
                elif header == 'DATA_TYPE':
                    data_type_col = i
            
            if db_item_col is not None and data_type_col is not None:
                last_row = len(worksheet.col_values(db_item_col + 1))
                
                db_item_range = worksheet.range(2, db_item_col + 1, last_row, db_item_col + 1)
                data_type_range = worksheet.range(2, data_type_col + 1, last_row, data_type_col + 1)
                
                db_items = [cell.value for cell in db_item_range]
                data_types_list = [cell.value for cell in data_type_range]
                
                data_types = dict(zip(db_items, data_types_list))
            
            self.logger.info(f"データ型情報取得完了: {len(data_types)} 項目")
            return data_types
            
        except Exception as e:
            self.logger.error(f"データ型情報取得エラー: {e}")
            return {}


# 後方互換性のための関数
def load_sql_file_list_from_spreadsheet(
    spreadsheet_id: str,
    sheet_name: str,
    json_keyfile_path: str,
    execution_column: str
) -> List[Tuple]:
    """
    旧式のSQLファイルリスト読み込み関数（後方互換性のため）
    """
    client = GoogleSheetsClient(json_keyfile_path)
    return client.load_sql_file_list(spreadsheet_id, sheet_name, execution_column)


def write_to_log_sheet(
    csv_file_name_column: str,
    sheet_name: str,
    main_table_name: str,
    category: str,
    record_count: int,
    json_keyfile_path: str,
    result: str,
    error_log: Optional[str] = None,
    save_path_id: Optional[str] = None
) -> None:
    """
    旧式のログ書き込み関数（後方互換性のため）
    """
    log_spreadsheet_id = '1iqDqeGXAovNQfnuuOi2xLzJIrmXOE1FKOgrSLgG0SOw'
    log_sheet_name = 'log'
    
    client = GoogleSheetsClient(json_keyfile_path)
    client.write_log_entry(
        log_spreadsheet_id,
        log_sheet_name,
        csv_file_name_column,
        sheet_name,
        main_table_name,
        category,
        record_count,
        result,
        error_log,
        save_path_id
    )