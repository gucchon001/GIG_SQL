from oauth2client.service_account import ServiceAccountCredentials
import gspread
from googleapiclient.discovery import build
from google.oauth2 import service_account
import csv
import datetime
import os
import re
import io
import tkinter as tk
from tkinter import filedialog
import tkinter as tk
from tkinter import messagebox
from decimal import Decimal
from my_logging import setup_department_logger

LOGGER = setup_department_logger('main')

def load_sql_file_list_from_spreadsheet(spreadsheet_id, sheet_name, json_keyfile_path):
    """
    指定されたGoogleスプレッドシートからSQLファイルのリストを読み込む関数

    :param spreadsheet_id: スプレッドシートのID。
    :param sheet_name: 読み込むシートの名前。
    :param json_keyfile_path: Google APIの認証情報が含まれるJSONファイルのパス。
    :return: 実行対象とマークされたSQLファイル名のリスト。
    """
    SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    EXECUTION_COLUMN = '実行対象'
    SQL_FILE_COLUMN = 'sqlファイル名'
    CSV_FILE_COLUMN = 'CSVファイル名/SSシート名'
    FILENAME_FORMAT_COLUMN = '保存ファイル名形式'
    PERIOD_CONDITION_COLUMN = '取得期間'
    PERIOD_CRITERIA_COLUMN = '取得基準'
    SPREADSHEET_COLUMN = '出力先'
    SAVE_PATH_COLUMN = '保存先PATH/ID'
    DELETION_EXCLUSION_COLUMN = '削除R除外'
    PASTE_FORMAT_COLUMN = 'スプシ貼り付け形式' 
    TEST_COLUMN = 'テスト' 

    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, SCOPES)
    gc = gspread.authorize(credentials)
    worksheet = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
    records = worksheet.get_all_records()

    sql_and_csv_files = []
    for record in records:
        if record[EXECUTION_COLUMN] == 'TRUE':
            sql_file_name = record[SQL_FILE_COLUMN]
            csv_file_name = record[CSV_FILE_COLUMN]
            filename_format = record[FILENAME_FORMAT_COLUMN]
            period_condition = record[PERIOD_CONDITION_COLUMN]
            period_criteria = record[PERIOD_CRITERIA_COLUMN]
            save_path_id = record[SAVE_PATH_COLUMN]
            output_to_spreadsheet = record[SPREADSHEET_COLUMN]
            deletion_exclusion = record[DELETION_EXCLUSION_COLUMN]
            paste_format = record[PASTE_FORMAT_COLUMN] 
            test_execution = record[TEST_COLUMN] 

            if filename_format:
                now = datetime.datetime.now()
                filename_without_ext = os.path.splitext(csv_file_name)[0]  # 拡張子を除いたファイル名
                filename_patterns = {
                    'yyyyMMdd_{filename}': now.strftime('%Y%m%d_') + filename_without_ext,
                    '{filename}_yyyy-MM-dd': filename_without_ext + '_' + now.strftime('%Y-%m-%d'),
                    '{filename}yyyy-MM-dd': filename_without_ext + now.strftime('%Y-%m-%d'),
                    '{filename}_yyyyMMddhhmmss': filename_without_ext + '_' + now.strftime('%Y%m%d%H%M%S'),
                    '{filename}_yyyy-MM': filename_without_ext + '_' + now.strftime('%Y-%m')
                }
                for pattern, value in filename_patterns.items():
                    if pattern in filename_format:
                        csv_file_name = filename_format.replace(pattern, value) + '.csv'
                        break
                else:
                    csv_file_name = filename_format + '.csv'
            sql_and_csv_files.append((sql_file_name, csv_file_name, period_condition, period_criteria, save_path_id, output_to_spreadsheet, deletion_exclusion, paste_format, test_execution))

    return sql_and_csv_files


''''
#SQLファイルの読み込み関数
def load_sql_from_file(file_path):
    """
    指定されたファイルパスからSQL文を読み込み、その内容を返します。

    :param file_path: SQLファイルのパス
    :return: ファイルから読み込まれたSQL文の文字列
    """
    print(file_path)
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_query = file.read()
            print(f"SQLファイルを読み込みました")
            return sql_query
    except Exception as e:
        print(f"SQLファイルの読み込みに失敗しました: {e}")
        return None
'''''

def load_sql_from_file(file_path, google_folder_id, json_keyfile_path):
    """
    指定されたファイルパスからGoogleドライブ上のSQLファイルを読み込み、その内容を返します。

    :param file_path: SQLファイルのパス（Googleドライブ上）
    :param google_folder_id: GoogleドライブのフォルダID
    :param json_keyfile_path: Google APIの認証情報が含まれるJSONファイルのパス
    :return: ファイルから読み込まれたSQL文の文字列
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(json_keyfile_path, scopes=['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=credentials)

        query = f"'{google_folder_id}' in parents and name = '{file_path}'"
        file_list_results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = file_list_results.get('files', [])

        if files:
            file_id = files[0]['id']
            file_content = service.files().get_media(fileId=file_id, supportsAllDrives=True).execute().decode('utf-8')
            sql_query = file_content.strip()
            return sql_query
        else:
            print(f"File not found: {file_path}")
            return None
    except Exception as e:
        print(f"Error loading SQL file: {e}")
        raise
        return None

#csvファイルの保存（全件取得）
def csvfile_export(conn, sql_query, csv_file_path, save_path_id=None):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        fetchall = cursor.fetchall()

        # '保存先PATH/ID'がある場合はそこに保存、そうでない場合は指定されたパスに保存
        final_csv_file_path = save_path_id if save_path_id else csv_file_path

        with open(final_csv_file_path, 'w', newline='', encoding='cp932', errors='replace') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([i[0] for i in cursor.description])  # カラムヘッダーを書き込み
            for row in fetchall:
                modified_row = [
                    str(cell).replace('\ufe0f', '【unicode文字】')
                              .replace('\u0111', '【unicode文字】')
                              .replace('\u20e3', '【unicode文字】')
                    for cell in row
                ]
                writer.writerow(modified_row)

        print(f"結果が {final_csv_file_path} に保存されました。")
    except Exception as e:
        print(f"クエリ実行またはCSVファイル書き込み時にエラーが発生しました: {e}")
        raise
    finally:
        cursor.close()

# 複数のパターンにマッチする正規表現を定義
def extract_columns_mapping(sql_query):
    patterns = [
        re.compile(r"(\w+)\.(\w+) AS \"(.*?)\""),  # 通常のカラムマッピング
        re.compile(r"DATE_FORMAT\((\w+)\.(\w+),\s*'.*?'\)\s*AS \"(.*?)\""),  # DATE_FORMAT関数
        re.compile(r"CASE.*?END AS \"(.*?)\"")  # CASE文
    ]
    
    mapping = {}
    for pattern in patterns:
        matches = pattern.findall(sql_query)
        for match in matches:
            if len(match) == 3:  # DATE_FORMATや通常のカラム
                column_alias = match[2]
                column_full_name = f"{match[0]}.{match[1]}"
            elif len(match) == 1:  # CASE文
                column_alias = match[0]
                column_full_name = column_alias  # CASE文の場合、エイリアス名をそのまま使う
            mapping[column_alias] = column_full_name

    return mapping

#元SQLファイル文に指定条件を挿入
def add_conditions_to_sql(sql_query, input_values, input_fields_types, deletion_exclusion, skip_deletion_exclusion=False):
    try:
        columns_mapping = extract_columns_mapping(sql_query)
        print("columns_mapping:", columns_mapping)

        additional_conditions = []

        for db_item, values in input_values.items():
            if db_item in columns_mapping and values:
                column_name = columns_mapping[db_item]

                if input_fields_types[db_item] == 'Date' and isinstance(values, dict):
                    start_date, end_date = values.get('start_date'), values.get('end_date')
                    if start_date and end_date:
                        condition = f"{column_name} BETWEEN STR_TO_DATE('{start_date}', '%Y/%m/%d') AND STR_TO_DATE('{end_date}', '%Y/%m/%d')"
                        additional_conditions.append(condition)
                elif input_fields_types[db_item] == 'FA' and values.strip():
                    condition = f"{column_name} LIKE '%{values}%'"
                    additional_conditions.append(condition)
                elif values:
                    if isinstance(values, list):
                        placeholders = ', '.join([f"'{value}'" for value in values])
                        condition = f"{column_name} IN ({placeholders})"
                        additional_conditions.append(condition)
                    else:
                        condition = f"{column_name} = '{values}'"
                        additional_conditions.append(condition)

        sql_parts = sql_query.split('FROM clause')
        if len(sql_parts) != 2:
            raise ValueError("「FROM句」コメントが見つからないか、複数存在します")

        from_clause_onward = sql_parts[1]
        alias_match = re.search(r"FROM\s+[\w\.]+\s+(\w+)", from_clause_onward, re.IGNORECASE)
        alias_name = alias_match.group(1) if alias_match else ''

        deleted_at_column = f"{alias_name}.deleted_at" if alias_name else "deleted_at"

        if not skip_deletion_exclusion:
            deletion_exclusion = str(deletion_exclusion).upper()
            if deletion_exclusion == 'TRUE':
                additional_conditions.append(f"{deleted_at_column} IS NULL")
            elif deletion_exclusion == 'FALSE':
                pass
            else:
                raise ValueError("Invalid value for deletion_exclusion. It should be either True or False.")

        if sql_query.strip().endswith(";"):
            sql_query = sql_query.strip()[:-1]
        else:
            sql_query = sql_query.strip()

        lines = sql_query.split('\n')

        upper_lines = [line.upper() for line in lines if 'CASE WHEN' not in line.upper()]
        upper_query = '\n'.join(upper_lines)

        from_index = upper_query.find("FROM")

        if from_index != -1:
            before_from_lines = lines[:lines.index(next(line for line in lines if "FROM" in line.upper())) + 1]
            after_from_lines = lines[lines.index(next(line for line in lines if "FROM" in line.upper())) + 1:]

            where_index = upper_query.find("WHERE", from_index)

            if where_index != -1:
                before_where_lines = after_from_lines[:upper_lines.index(next(line for line in upper_lines if "WHERE" in line.upper()))]
                after_where_lines = after_from_lines[upper_lines.index(next(line for line in upper_lines if "WHERE" in line.upper())):]

                if additional_conditions:
                    modified_after_from_lines = before_where_lines + [f"AND {' AND '.join(additional_conditions)}"] + after_where_lines
                else:
                    modified_after_from_lines = before_where_lines + after_where_lines
            else:
                if additional_conditions:
                    modified_after_from_lines = after_from_lines + [f"WHERE {' AND '.join(additional_conditions)}"]
                else:
                    modified_after_from_lines = after_from_lines

            modified_lines = before_from_lines + modified_after_from_lines
            sql_query = '\n'.join(modified_lines)

        else:
            raise ValueError("FROM句が見つかりませんでした")

        sql_query = sql_query.strip() + ";"

        print("Modified SQL Query:", sql_query)
        return sql_query
    except Exception as e:
        LOGGER.error(f"add_conditions_to_sql関数内でエラーが発生しました: {e}")
        raise

#個別CSVエクスポート
def csvfile_export_with_timestamp(conn, sql_query, sql_file_path, include_header):
    root = tk.Tk()
    root.withdraw()  # Tkのルートウィンドウを表示しない

    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        fetchall = cursor.fetchall()

        # レコードの件数を取得
        record_count = len(fetchall)

        # 推定ファイルサイズを計算
        estimated_size = sum(len(str(cell)) for row in fetchall for cell in row)
        estimated_size_mb = estimated_size / (1024 * 1024)  # バイト数をメガバイトに変換
        estimated_size_kb = estimated_size / 1024  # バイト数をキロバイトに変換

        # 推定ダウンロード時間を計算（仮定：平均ダウンロード速度を1MB/sとする）
        estimated_time = estimated_size_mb

        # ファイルサイズと推定ダウンロード時間のメッセージを作成
        if estimated_size_mb >= 1:
            size_message = f"推定ファイルサイズ: {estimated_size_mb:.2f} MB"
        else:
            size_message = f"推定ファイルサイズ: {estimated_size_kb:.2f} KB"

        if estimated_time < 1:
            time_message = f"推定ダウンロード時間: {max(0.001, estimated_time):.3f}秒"
        elif estimated_time < 60:
            time_message = f"推定ダウンロード時間: {estimated_time:.0f}秒"
        else:
            minutes, seconds = divmod(estimated_time, 60)
            time_message = f"推定ダウンロード時間: {minutes:.0f}分{seconds:.0f}秒"

        # 確認ポップアップを表示
        confirm = messagebox.askyesno("CSVファイル保存", f"抽出レコード数は{record_count}件です。\n{size_message}\n{time_message}\nこのままcsvファイルに保存をしますか？")

        if confirm:
            # ファイル保存ダイアログを開く
            current_time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            sql_name = os.path.splitext(os.path.basename(sql_file_path))[0]
            suggested_filename = f"{current_time_str}_{sql_name}.csv"
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile=suggested_filename, filetypes=[("CSV files", "*.csv")])

            if file_path:
                with open(file_path, 'w', newline='', encoding='cp932', errors='replace') as csv_file:
                    writer = csv.writer(csv_file)
                    if include_header:
                        writer.writerow([i[0] for i in cursor.description])  # カラムヘッダーを条件に応じて書き込み
                    for row in fetchall:
                        # Unicode文字を【unicode文字】に置換
                        modified_row = [
                            str(cell)
                            .replace('\ufe0f', '【unicode文字】')
                            .replace('\u0111', '【unicode文字】')
                            .replace('\u20e3', '【unicode文字】')
                            for cell in row
                        ]
                        writer.writerow(modified_row)
                print(f"結果が {file_path} に保存されました。")
            else:
                print("ファイル保存がキャンセルされました。")
        else:
            print("csvファイルの保存がキャンセルされました。")

    except Exception as e:
        print(f"クエリ実行またはCSVファイル書き込み時にエラーが発生しました: {e}")
        raise  # エラーを呼び出し元に伝える
    finally:
        cursor.close()
        root.destroy()  # Tkインスタンスを破棄

#クリップボードにコピー
def copy_to_clipboard(conn, sql_query, include_header):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        fetchall = cursor.fetchall()

        # レコードの件数を取得
        record_count = len(fetchall)

        # StringIOを使用してメモリ上にCSV（タブ区切り）データを作成
        output = io.StringIO()
        writer = csv.writer(output, delimiter='\t', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if include_header:
            headers = [i[0] for i in cursor.description]
            writer.writerow(headers)

        for row in fetchall:
            writer.writerow(row)

        clipboard_content = output.getvalue()
        root = tk.Tk()
        root.withdraw()  # ウィンドウを表示せずに処理
        root.clipboard_clear()  # クリップボードをクリア
        root.clipboard_append(clipboard_content)  # クリップボードにデータを追加

        print(f"クリップボードに{record_count}件のレコードがコピーされました。")
        messagebox.showinfo("クリップボードコピー", f"クリップボードに{record_count}件のレコードがコピーされました。")

    except Exception as e:
        print(f"クエリ実行時にエラーが発生しました: {e}")
        raise  # エラーを呼び出し元に伝える
    finally:
        cursor.close()
        output.close()
        root.destroy()  # クリップボードの処理後にTkインスタンスを破棄

def set_period_condition(period_condition, period_criteria, sql_query):
    try:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        three_days_ago = today - datetime.timedelta(days=3)

        if period_criteria == '登録日時':
            column_name = 'created_at'
        elif period_criteria == '更新日時':
            column_name = 'updated_at'
        else:
            return sql_query

        sql_parts = sql_query.split('FROM clause')
        if len(sql_parts) != 2:
            raise ValueError("「FROM句」コメントが見つからないか、複数存在します")

        from_clause_onward = sql_parts[1]
        alias_match = re.search(r"FROM\s+[\w\.]+\s+(\w+)", from_clause_onward, re.IGNORECASE)
        alias_name = alias_match.group(1) if alias_match else ''

        column_name = f"{alias_name}.{column_name}" if alias_name else column_name

        if period_condition == '当日':
            condition = f"DATE({column_name}) = '{today}'"
        elif period_condition == '前日':
            condition = f"DATE({column_name}) = '{yesterday}'"
        elif period_condition == '前日まで累積':
            condition = f"DATE({column_name}) <= '{yesterday}'"
        elif period_condition.endswith('～前日まで累積'):
            start_date_str = period_condition.split('～')[0].strip()
            start_date = datetime.datetime.strptime(start_date_str, '%Y年%m月%d日').date()
            condition = f"DATE({column_name}) BETWEEN '{start_date}' AND '{yesterday}'"
        elif period_condition.endswith('日前時点を1日分'):
            days_ago = int(period_condition.split('日前')[0])
            target_date = today - datetime.timedelta(days=days_ago)
            condition = f"DATE({column_name}) = '{target_date}'"
        else:
            return sql_query

        if sql_query.strip().endswith(";"):
            sql_query = sql_query.strip()[:-1]
        else:
            sql_query = sql_query.strip()

        lines = sql_query.split('\n')

        upper_lines = [line.upper() for line in lines if 'CASE WHEN' not in line.upper()]
        upper_query = '\n'.join(upper_lines)

        from_index = upper_query.find("FROM")

        if from_index != -1:
            before_from_lines = lines[:lines.index(next(line for line in lines if "FROM" in line.upper())) + 1]
            after_from_lines = lines[lines.index(next(line for line in lines if "FROM" in line.upper())) + 1:]

            where_index = upper_query.find("WHERE", from_index)

            if where_index != -1:
                before_where_lines = after_from_lines[:upper_lines.index(next(line for line in upper_lines if "WHERE" in line.upper()))]
                after_where_lines = after_from_lines[upper_lines.index(next(line for line in upper_lines if "WHERE" in line.upper())):]

                modified_after_from_lines = before_where_lines + [f"AND {condition}"] + after_where_lines
            else:
                modified_after_from_lines = after_from_lines + [f"WHERE {condition}"]

            modified_lines = before_from_lines + modified_after_from_lines
            sql_query = '\n'.join(modified_lines)

        else:
            raise ValueError("FROM句が見つかりませんでした")

        sql_query = sql_query.strip() + ";"

        print('Modified SQL query:')
        print(sql_query)

        return sql_query
    except Exception as e:
        LOGGER.error(f"set_period_condition関数内でエラーが発生しました: {e}")
        raise

#スプシ貼り付け
def get_column_letter(column_index):
    # 1から始まるインデックスに対応するため、1を引く
    column_index -= 1
    letter = ''
    while column_index >= 0:
        letter = chr(column_index % 26 + 65) + letter
        column_index = column_index // 26 - 1
    return letter

#スプシ貼り付け
def export_to_spreadsheet(conn, sql_query, save_path_id, sheet_name, json_keyfile_path, paste_format, main_sheet_name):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        fetchall = cursor.fetchall()

        # Decimalオブジェクトを文字列に変換
        converted_data = []
        for row in fetchall:
            converted_row = [str(cell) if isinstance(cell, Decimal) else cell for cell in row]
            converted_data.append(converted_row)

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
        gc = gspread.authorize(credentials)

        spreadsheet = gc.open_by_key(save_path_id)
        
        if sheet_name == main_sheet_name:
            # 実行シートの場合は処理をスキップ
            print(f"Skipping export to main sheet: {sheet_name}")
            return

        worksheet = spreadsheet.worksheet(sheet_name)

        column_count = len(cursor.description)
        last_column_letter = get_column_letter(column_count)
        print(f"Column count: {column_count}, Last column letter: {last_column_letter}")

        if paste_format == '最終行積立て':
            last_row = len(worksheet.col_values(1)) + 1
            if last_row > worksheet.row_count:
                additional_rows = last_row - worksheet.row_count
                worksheet.add_rows(additional_rows)
            worksheet.update(f'A{last_row}', converted_data)
        elif paste_format == '全張替え':
            clear_range = f'A2:{last_column_letter}{worksheet.row_count}'
            print(f"Clearing range: {clear_range}")
            worksheet.batch_clear([clear_range])
            data_row_count = len(converted_data)
            if data_row_count > worksheet.row_count - 1:
                additional_rows = data_row_count - (worksheet.row_count - 1)
                worksheet.add_rows(additional_rows)
            worksheet.update('A2', converted_data)

        print(f"Data has been transferred to {sheet_name} sheet in {save_path_id} with {paste_format} method.")
    except Exception as e:
        print(f"Error during query execution or spreadsheet writing: {e}")
        raise
    finally:
        cursor.close()

#テスト実行
def setup_test_environment(test_execution, output_to_spreadsheet, save_path_id, csv_file_name, spreadsheet_id, json_keyfile_path):
    if str(test_execution).lower() == 'true':
        if output_to_spreadsheet == 'CSV':
            # CSVの場合のテスト環境設定
            test_folder = os.path.join(save_path_id, 'test')
            if not os.path.exists(test_folder):
                os.makedirs(test_folder)
            save_path_id = test_folder
        elif output_to_spreadsheet == 'スプシ':
            # スプレッドシートの場合のテスト環境設定
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
            gc = gspread.authorize(credentials)
            
            # 保存先PATH/IDからスプレッドシートIDを取得
            spreadsheet_id = save_path_id
            
            spreadsheet = gc.open_by_key(save_path_id)
            print('Spreadsheet ID:', spreadsheet.id)
            print('Spreadsheet Title:', spreadsheet.title)
            
            test_sheet_name = f"{csv_file_name}_test"
            try:
                worksheet = spreadsheet.worksheet(test_sheet_name)
                print(worksheet)
            except gspread.exceptions.WorksheetNotFound:
                try:
                    source_sheet = spreadsheet.worksheet(csv_file_name)
                    worksheet = spreadsheet.add_worksheet(title=test_sheet_name, rows=source_sheet.row_count, cols=source_sheet.col_count)
                    header_row = source_sheet.row_values(1)
                    worksheet.insert_row(header_row, index=1)
                except gspread.exceptions.WorksheetNotFound:
                    raise ValueError(f"The source sheet '{csv_file_name}' does not exist in the spreadsheet.")
            csv_file_name = test_sheet_name

    return save_path_id, csv_file_name