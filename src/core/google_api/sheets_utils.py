"""
Google Sheets ユーティリティモジュール

スプレッドシートへの読み書き、データタイプ取得、ログ出力等
"""
import gspread
from googleapiclient.discovery import build
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from src.core.logging.logger import get_logger
from src.core.google_api.auth import authenticate_google_api, retry_on_exception

logger = get_logger(__name__)


@retry_on_exception
def load_sql_file_list_from_spreadsheet(spreadsheet_id: str, sheet_name: str, 
                                       json_keyfile_path: str, execution_column: str) -> List[str]:
    """
    指定されたGoogleスプレッドシートからSQLファイルのリストを読み込む
    
    Args:
        spreadsheet_id (str): スプレッドシートのID
        sheet_name (str): 読み込むシートの名前
        json_keyfile_path (str): Google APIの認証情報が含まれるJSONファイルのパス
        execution_column (str): 実行対象の列名
        
    Returns:
        List[str]: 実行対象とマークされたSQLファイル名のリスト
    """
    try:
        # Google Sheets APIの認証
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = authenticate_google_api(json_keyfile_path, scopes)
        gc = gspread.authorize(credentials)
        
        # スプレッドシートを開く
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # データを取得
        data = worksheet.get_all_records()
        logger.info(f"Loaded sheet: {sheet_name} with {len(data)} records.")
        
        # 実行対象のSQLファイルをフィルタリング
        sql_files = []
        for row in data:
            if str(row.get(execution_column, '')).strip().lower() == 'true':
                sql_file = row.get('sqlファイル名', '')
                if sql_file:
                    sql_files.append(sql_file)
        
        logger.info(f"実行対象件数: {len(sql_files)}件")
        return sql_files
        
    except Exception as e:
        logger.error(f"スプレッドシートからSQLファイルリスト読み込みエラー: {e}")
        raise


@retry_on_exception
def load_sql_from_file(file_path: str, google_folder_id: str, json_keyfile_path: str) -> str:
    """
    GoogleドライブからSQLファイルを読み込む
    
    Args:
        file_path (str): SQLファイル名
        google_folder_id (str): GoogleドライブフォルダID
        json_keyfile_path (str): 認証JSONファイルパス
        
    Returns:
        str: SQLクエリ内容
    """
    try:
        logger.info(f"SQLファイル読み込み開始: {file_path}")
        
        # Google Drive APIの認証
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = authenticate_google_api(json_keyfile_path, scopes)
        service = build('drive', 'v3', credentials=credentials)
        
        logger.info(f"GoogleドライブのフォルダID: {google_folder_id}")
        logger.info(f"SQLファイル名: {file_path}")
        
        # フォルダ内のファイルを検索
        query = f"name='{file_path}' and parents in '{google_folder_id}'"
        results = service.files().list(q=query, fields='files(id, name)').execute()
        files = results.get('files', [])
        
        logger.info(f"ファイルリスト結果: {files}")
        
        if not files:
            raise FileNotFoundError(f"SQLファイルが見つかりません: {file_path}")
        
        file_id = files[0]['id']
        logger.info(f"ファイルID取得成功: {file_id}")
        
        # ファイル内容を取得
        file_content = service.files().get_media(fileId=file_id).execute()
        sql_content = file_content.decode('utf-8')
        
        logger.info(f"SQLファイル読み込み成功 - 文字数: {len(sql_content)}")
        logger.info(f"SQL内容（最初の200文字）: {sql_content[:200]}")
        
        return sql_content
        
    except Exception as e:
        logger.error(f"SQLファイル読み込みエラー: {e}")
        raise


@retry_on_exception
def export_to_spreadsheet(conn, sql_query: str, save_path_id: str, sheet_name: str,
                         json_keyfile_path: str, paste_format: str, main_sheet_name: str,
                         csv_file_name_column: str, main_table_name: str, category: str,
                         chunk_size: int = 10000, delay: float = 0) -> int:
    """
    SQLクエリ結果をGoogleスプレッドシートにエクスポート
    
    Args:
        conn: データベース接続
        sql_query (str): 実行するSQLクエリ
        save_path_id (str): スプレッドシートID
        sheet_name (str): シート名
        json_keyfile_path (str): 認証JSONファイルパス
        paste_format (str): 貼り付け形式
        main_sheet_name (str): メインシート名
        csv_file_name_column (str): ファイル名カラム
        main_table_name (str): メインテーブル名
        category (str): カテゴリ
        chunk_size (int): チャンクサイズ
        delay (float): 遅延時間
        
    Returns:
        int: エクスポートされたレコード数
    """
    try:
        logger.info(f"スプレッドシートエクスポート開始: {sheet_name}")
        
        # SQLクエリ実行
        df = pd.read_sql(sql_query, conn)
        record_count = len(df)
        
        if record_count == 0:
            logger.warning("データが0件でした")
            return 0
        
        # Google Sheets APIの認証
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = authenticate_google_api(json_keyfile_path, scopes)
        gc = gspread.authorize(credentials)
        
        # スプレッドシートを開く
        spreadsheet = gc.open_by_key(save_path_id)
        
        # シートを取得または作成
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()  # 既存データをクリア
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=record_count + 1, cols=len(df.columns))
        
        # データをスプレッドシートに書き込み
        # ヘッダー行を設定
        header = df.columns.tolist()
        worksheet.append_row(header)
        
        # データを行ごとに追加（チャンクサイズ考慮）
        for i in range(0, record_count, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            rows = chunk.values.tolist()
            
            # データ型を文字列に変換（スプレッドシート用）
            for row in rows:
                for j, cell in enumerate(row):
                    if pd.isna(cell):
                        row[j] = ""
                    else:
                        row[j] = str(cell)
            
            # 行を追加
            for row in rows:
                worksheet.append_row(row)
            
            logger.info(f"スプレッドシート書き込み進捗: {min(i + chunk_size, record_count)}/{record_count}")
            
            if delay > 0:
                import time
                time.sleep(delay)
        
        logger.info(f"スプレッドシートエクスポート完了: {record_count}件")
        return record_count
        
    except Exception as e:
        logger.error(f"スプレッドシートエクスポートエラー: {e}")
        raise


def get_data_types(worksheet) -> Dict[str, str]:
    """
    ワークシートからデータタイプを取得
    
    Args:
        worksheet: Google Sheetsワークシート
        
    Returns:
        Dict[str, str]: データタイプ辞書
    """
    try:
        # データタイプ行（通常は2行目）を取得
        data_types_row = worksheet.row_values(2)
        headers = worksheet.row_values(1)
        
        data_types = {}
        for i, header in enumerate(headers):
            if i < len(data_types_row):
                data_types[header] = data_types_row[i]
            else:
                data_types[header] = 'text'  # デフォルト
        
        logger.debug(f"データタイプ取得: {data_types}")
        return data_types
        
    except Exception as e:
        logger.error(f"データタイプ取得エラー: {e}")
        return {}


def apply_data_types_to_df(df: pd.DataFrame, data_types: Dict[str, str], 
                          logger, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    DataFrameにデータタイプを適用
    
    Args:
        df (pd.DataFrame): 対象DataFrame
        data_types (Dict[str, str]): データタイプ辞書
        logger: ロガー
        encoding (str): エンコーディング
        
    Returns:
        pd.DataFrame: データタイプ適用後DataFrame
    """
    try:
        for column, data_type in data_types.items():
            if column in df.columns:
                if data_type == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d')
                elif data_type == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
                elif data_type == 'int':
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif data_type == 'float':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                
                logger.debug(f"列 '{column}' の型変換完了: {data_type}")
        
        return df
        
    except Exception as e:
        logger.error(f"データタイプ適用エラー: {e}")
        return df


@retry_on_exception
def write_to_log_sheet(csv_file_name_column: str, sheet_name: str, main_table_name: str,
                      category: str, record_count: int, json_keyfile_path: str,
                      result: str, error_log: Optional[str] = None, 
                      save_path_id: Optional[str] = None) -> None:
    """
    ログシートに実行結果を書き込み
    
    Args:
        csv_file_name_column (str): CSVファイル名カラム
        sheet_name (str): シート名
        main_table_name (str): メインテーブル名
        category (str): カテゴリ
        record_count (int): レコード数
        json_keyfile_path (str): 認証JSONファイルパス
        result (str): 実行結果
        error_log (Optional[str]): エラーログ
        save_path_id (Optional[str]): 保存先パスID
    """
    try:
        logger.info("ログシートへの書き込み開始")
        
        # 現在時刻
        current_time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        
        # ログ行を作成
        log_row = [
            current_time,  # 実行日時
            csv_file_name_column,  # ファイル名
            sheet_name,  # シート名
            main_table_name,  # テーブル名
            category,  # カテゴリ
            record_count,  # レコード数
            result,  # 結果
            error_log or "",  # エラーログ
        ]
        
        # ログシートに書き込み（実装に応じて適切なログシートIDを使用）
        if save_path_id:
            scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = authenticate_google_api(json_keyfile_path, scopes)
            gc = gspread.authorize(credentials)
            
            spreadsheet = gc.open_by_key(save_path_id)
            log_sheet = spreadsheet.worksheet("実行ログ")  # ログシート名
            log_sheet.append_row(log_row)
            
            logger.info("ログシートへの書き込み完了")
        
    except Exception as e:
        logger.error(f"ログシート書き込みエラー: {e}")
        # ログ書き込みエラーは処理を止めない