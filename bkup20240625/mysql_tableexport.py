import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import os
import configparser
import traceback  # トレースバック用
from my_logging import setup_department_logger  # ログ設定用
import slack_notify  # Slack通知用

LOGGER = setup_department_logger('googlesheet_export')  # ロガーの設定

# 設定ファイルのパス
config_file = 'config.ini'

# 設定ファイルの読み込み
config = configparser.ConfigParser()
config.read(config_file, encoding='utf-8')

# CSVファイルの基本パスの取得
csv_base_path = config['Paths']['csv_base_path']

# 認証情報の取得
json_keyfile_path = config['Credentials']['json_keyfile_path']

# スプレッドシートの情報の取得
spreadsheet_id = config['Spreadsheet']['spreadsheet_id']

# 認証情報とスコープの設定
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
gc = gspread.authorize(credentials)

# スプレッドシートの選択
spreadsheet = gc.open_by_key(spreadsheet_id)

# CSVファイルとシート名のマッピングを取得してループ
csv_to_sheet_mapping = config['CSV']['csv_file_paths'].split(',')

for mapping in csv_to_sheet_mapping:
    csv_file_name, sheet_name = mapping.strip().split('=')
    csv_file_path = os.path.join(csv_base_path, csv_file_name)

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file_obj:
            reader = csv.reader(file_obj)
            next(reader)  # ヘッダ行をスキップ
            data_list = list(reader)

        worksheet = spreadsheet.worksheet(sheet_name)

        if data_list:
            # データの更新範囲をヘッダの次の行から開始する
            worksheet.update('A2', data_list)

    except Exception as e:
        error_message = traceback.format_exc()
        LOGGER.error(f"エラーが発生しました: {error_message} (ファイル: {csv_file_name}, シート: {sheet_name})")
        slack_notify.send_slack_error_message(e, config=config)
