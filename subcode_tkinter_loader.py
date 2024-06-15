# spreadsheet_loader.py の更新版
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import configparser

def load_sql_list_from_spreadsheet():
    """config.iniから設定を読み込み、指定されたスプレッドシートからSQLファイルリストを読み込みます。"""
    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # config.iniから情報を取得
    json_keyfile_path = config['Credentials']['json_keyfile_path']
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    sheet_name = config['Spreadsheet']['main_sheet']

    # Google Sheets APIへの認証
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    # スプレッドシートとシートを選択
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # スプレッドシートからのデータ読み込み処理
    data = sheet.get_all_values()

    # ヘッダ行から「個別リスト」、「SQLファイル名」、「CSVファイル呼称」の列番号を取得
    header = data[0]
    target_index = header.index('個別リスト')
    sql_file_name_index = header.index('sqlファイル名')
    csv_file_name_index = header.index('CSVファイル呼称')

    # チェックボックスがONのレコードのSQLファイル名とCSVファイル呼称を取得
    records = {
        row[csv_file_name_index]: row[sql_file_name_index]
        for row in data[1:]
        if row[target_index].lower() == 'true'
    }

    return records

def get_sql_file_name(selected_option):
    """指定されたプルダウン選択肢に対応するSQLファイル名から.sql拡張子を除去します。

    Args:
        selected_option (str): プルダウンで選択されたオプションのテキスト。

    Returns:
        str: .sql拡張子を除去したファイル名。該当する項目がなければNoneを返します。
    """
    # スプレッドシートからデータを再度読み込む
    records = load_sql_list_from_spreadsheet()
    sql_file_name = records.get(selected_option)
    
    if sql_file_name:
        # .sql拡張子を除去して返す
        return sql_file_name.replace('.sql', '')
    else:
        # 該当する項目がない場合はNoneを返す
        return None

#csvファイルの保存（個別）
import tkinter as tk
from tkinter import messagebox, filedialog
import os
import csv
import datetime

#SQL文のシート名を呼び出し
def load_sheet_from_spreadsheet(sheet_name):
    # configparserの設定
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    
    # config.iniからスプレッドシートIDと認証情報を取得
    spreadsheet_id = config['Spreadsheet']['spreadsheet_id']
    json_keyfile_path = config['Credentials']['json_keyfile_path']

    # Google Sheets APIへの認証
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    try:
        # スプレッドシートとシートを選択
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        print(f"Loaded sheet: {sheet_name}")
        return sheet
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet not found: {sheet_name}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
#選択シートの条件取得
def get_filtered_data_from_sheet(sheet):
    try:
        header_row = sheet.row_values(1)
        # 大文字小文字を無視して比較し、不要なスペースをトリムする
        cleaned_header_row = [h.strip().lower() for h in header_row]
        print("Cleaned Header Row:", cleaned_header_row)  # クリーンなヘッダ行を出力

        if len(cleaned_header_row) != len(set(cleaned_header_row)):
            raise ValueError("ヘッダ行に重複する項目があります。")

        records = sheet.get_all_records()

        filtered_data = []
        for record in records:
            if record['絞込'] == 'TRUE':
                data = {
                    'db_item': record['DB項目'],
                    'table_name': record['TABLE_NAME'],
                    'data_item': record['DATA_ITEM'],
                    'input_type': record['入力方式'],
                    'options': [option.split(' ') for option in record['選択項目'].split('\n') if option.strip()]  # オプションを設定値と名称のペアのリストに変換
                }
                filtered_data.append(data)
                #print("Filtered data item:", data)  # フィルタリングされたデータを出力

        return filtered_data
    except Exception as e:
        print("Exception in get_filtered_data_from_sheet:", e)  # デバッグ用
        return []