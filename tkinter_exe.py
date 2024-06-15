import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
from subcode_tkinter_loader import (
    load_sql_list_from_spreadsheet, get_sql_file_name,
    load_sheet_from_spreadsheet, get_filtered_data_from_sheet
)
from main_tkinter import execute_sql_file
import configparser
from datetime import datetime, timedelta, date

# configparserの設定
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# スプレッドシートのシート名を定義
SHEET_NAME = config['Spreadsheet']['main_sheet']

def load_sql_list():
    sql_files_dict = load_sql_list_from_spreadsheet()
    sql_file_combo['values'] = list(sql_files_dict.keys())
    show_message("SQLファイルリストを読み込みました。")

input_fields = {}
input_fields_types = {}
options_dict = {}

def create_dynamic_input_fields(master, data):
    global input_fields, input_fields_types, options_dict
    input_fields.clear()
    input_fields_types.clear()
    options_dict.clear()

    if not data:
        show_message("指定されている項目がありません", is_error=True)
        return

    row_index = 0
    total_height = 0  # 全体の高さを計算

    for item in data:
        label_text = item['db_item']
        label = ttk.Label(master, text=label_text)
        label.grid(row=row_index, column=0, pady=5, sticky='e')

        if item['input_type'] == 'FA':
            input_field = ttk.Entry(master)
            input_field.grid(row=row_index, column=1, sticky='ew', padx=5)
            input_fields[item['db_item']] = input_field
            input_fields_types[item['db_item']] = 'FA'
            total_height += 30  # 項目の高さを加算

        elif item['input_type'] == 'プルダウン':
            options = ['-'] + [option[1] for option in item['options']]
            input_field = ttk.Combobox(master, values=options, state="readonly")
            input_field.current(0)
            input_field.grid(row=row_index, column=1, sticky='ew', padx=5)
            input_fields[item['db_item']] = input_field
            input_fields_types[item['db_item']] = 'プルダウン'
            options_dict[item['db_item']] = item['options']
            total_height += 30  # 項目の高さを加算

        elif item['input_type'] == 'ラジオボタン':
            var = tk.StringVar(master, value="")
            radio_frame = ttk.Frame(master)
            radio_frame.grid(row=row_index, column=1, sticky='ew', padx=5)

            radio_buttons = []
            for i, option in enumerate(item['options']):
                radio_button = ttk.Radiobutton(radio_frame, text=option[1], variable=var, value=option[1])
                radio_button.grid(row=i // 5, column=i % 5, padx=5, pady=5)
                radio_buttons.append(radio_button)

            uncheck_var = tk.BooleanVar(value=False)
            uncheck_checkbox = ttk.Checkbutton(radio_frame, text="選択を外す", variable=uncheck_var, command=lambda: deselect_radio_buttons(var, radio_buttons, uncheck_var))
            uncheck_checkbox.grid(row=(len(item['options']) + 1) // 5, column=(len(item['options']) + 1) % 5, padx=5, pady=5)

            def deselect_radio_buttons(var, radio_buttons, uncheck_var):
                if uncheck_var.get():
                    var.set("")
                    for radio_button in radio_buttons:
                        radio_button.deselect()

            input_fields[item['db_item']] = var
            input_fields_types[item['db_item']] = 'ラジオボタン'
            options_dict[item['db_item']] = item['options']
            total_height += 60  # 項目の高さを加算

        elif item['input_type'] == 'チェックボックス':
            checkbox_frame = ttk.Frame(master)
            checkbox_frame.grid(row=row_index, column=1, sticky='ew', padx=5)
            var_dict = {}
            for i, option in enumerate(item['options']):
                var = tk.BooleanVar()
                checkbox = ttk.Checkbutton(checkbox_frame, text=option[1], variable=var)
                checkbox.grid(row=i // 5, column=i % 5, padx=5, pady=5)
                var_dict[option[0]] = var
            input_fields[item['db_item']] = var_dict
            input_fields_types[item['db_item']] = 'チェックボックス'
            total_height += 60  # 項目の高さを加算

        elif item['input_type'] == 'Date':
            date_frame = ttk.Frame(master)
            date_frame.grid(row=row_index, column=1, sticky='ew', padx=5)

            # 開始日のフィールドとラベルを追加
            start_date_label = ttk.Label(date_frame, text="開始日")
            start_date_label.grid(row=0, column=0, pady=5)

            start_date_field = DateEntry(date_frame, date_pattern='yyyy/MM/dd')
            start_date_field.grid(row=0, column=1, sticky='ew', padx=5, pady=5)

            # 終了日のフィールドとラベルを追加
            end_date_label = ttk.Label(date_frame, text="終了日")
            end_date_label.grid(row=1, column=0, pady=5)

            end_date_field = DateEntry(date_frame, date_pattern='yyyy/MM/dd')
            end_date_field.grid(row=1, column=1, sticky='ew', padx=5, pady=5)

            # 開始日のリンク
            def set_start_date_today():
                start_date_field.set_date(date.today())

            def set_start_date_yesterday():
                yesterday = date.today() - timedelta(days=1)
                start_date_field.set_date(yesterday)

            def clear_start_date():
                start_date_field.delete(0, 'end')

            today_link_start = ttk.Label(date_frame, text="本日", foreground="blue", cursor="hand2")
            today_link_start.grid(row=0, column=2, padx=5)
            today_link_start.bind("<Button-1>", lambda e: set_start_date_today())

            yesterday_link_start = ttk.Label(date_frame, text="昨日", foreground="blue", cursor="hand2")
            yesterday_link_start.grid(row=0, column=3, padx=5)
            yesterday_link_start.bind("<Button-1>", lambda e: set_start_date_yesterday())

            clear_link_start = ttk.Label(date_frame, text="クリア", foreground="blue", cursor="hand2")
            clear_link_start.grid(row=0, column=4, padx=5)
            clear_link_start.bind("<Button-1>", lambda e: clear_start_date())

            # 終了日のリンク
            def set_end_date_today():
                end_date_field.set_date(date.today())

            def set_end_date_yesterday():
                yesterday = date.today() - timedelta(days=1)
                end_date_field.set_date(yesterday)

            def clear_end_date():
                end_date_field.delete(0, 'end')

            today_link_end = ttk.Label(date_frame, text="本日", foreground="blue", cursor="hand2")
            today_link_end.grid(row=1, column=2, padx=5)
            today_link_end.bind("<Button-1>", lambda e: set_end_date_today())

            yesterday_link_end = ttk.Label(date_frame, text="昨日", foreground="blue", cursor="hand2")
            yesterday_link_end.grid(row=1, column=3, padx=5)
            yesterday_link_end.bind("<Button-1>", lambda e: set_end_date_yesterday())

            clear_link_end = ttk.Label(date_frame, text="クリア", foreground="blue", cursor="hand2")
            clear_link_end.grid(row=1, column=4, padx=5)
            clear_link_end.bind("<Button-1>", lambda e: clear_end_date())

            input_fields[item['db_item']] = {'start_date_field': start_date_field, 'end_date_field': end_date_field}
            input_fields_types[item['db_item']] = 'Date'
            total_height += 80  # 日付型項目の高さを加算

        row_index += 1

    master.update_idletasks()  # レイアウト更新
    new_height = total_height + 200  # 全体の高さを設定
    root.geometry(f"{root.winfo_width()}x{new_height}")  # ウィンドウの高さを更新

def get_input_values():
    values = {}
    for db_item, field_info in input_fields.items():
        if input_fields_types[db_item] == 'Date':
            start_date_field, end_date_field = field_info['start_date_field'], field_info['end_date_field']
            values[db_item] = {
                'start_date': start_date_field.get(),
                'end_date': end_date_field.get()
            }

        elif isinstance(field_info, ttk.Combobox):
            selected_option_text = field_info.get()
            options_list = options_dict[db_item]
            selected_option_value = next((option[0] for option in options_list if option[1] == selected_option_text), None)
            values[db_item] = selected_option_value

        elif isinstance(field_info, ttk.Entry):
            values[db_item] = field_info.get()

        elif isinstance(field_info, tk.StringVar):
            selected_option = field_info.get()
            options_list = options_dict[db_item]
            selected_option_value = next((option[0] for option in options_list if option[1] == selected_option), None)
            values[db_item] = selected_option_value

        elif isinstance(field_info, dict) and input_fields_types[db_item] == 'チェックボックス':
            selected_options = [key for key, var in field_info.items() if var.get()]
            values[db_item] = selected_options

    return values

def on_csv_execute():
    global input_fields_types
    include_header = not header_check_var.get()

    try:
        selected_option = sql_file_combo.get()
        sql_file_name = get_sql_file_name(selected_option)
        if sql_file_name:
            sheet_name = sql_file_name.replace('.sql', '')
            load_sheet_from_spreadsheet(sheet_name)
            selected_sheet = load_sheet_from_spreadsheet(sheet_name)
            get_filtered_data_from_sheet(selected_sheet)
            full_sql_file_name = sql_file_name + ".sql"

            input_values = get_input_values()

            if execute_sql_file(full_sql_file_name, input_values, input_fields_types, 'download', include_header, root):
                show_message(f"{sheet_name} のSQLファイル実行に成功しました。")
            else:
                show_message("SQLファイルの実行に失敗しました。", is_error=True)
        else:
            show_message("指定された項目に対応するSQLファイルが見つかりませんでした。", is_error=True)
    except Exception as e:
        show_message(f"エラーが発生しました: {e}", is_error=True)

def on_copy_to_clipboard():
    global input_fields_types
    include_header = not header_check_var.get()

    try:
        selected_option = sql_file_combo.get()
        sql_file_name = get_sql_file_name(selected_option)
        if sql_file_name:
            sheet_name = sql_file_name.replace('.sql', '')
            load_sheet_from_spreadsheet(sheet_name)
            selected_sheet = load_sheet_from_spreadsheet(sheet_name)
            get_filtered_data_from_sheet(selected_sheet)
            full_sql_file_name = sql_file_name + ".sql"
            input_values = get_input_values()

            if execute_sql_file(full_sql_file_name, input_values, input_fields_types, 'copy', include_header, root):
                show_message(f"{sheet_name} のSQLファイル実行に成功しました。")
            else:
                show_message("SQLファイルの実行に失敗しました。", is_error=True)
        else:
            show_message("指定された項目に対応するSQLファイルが見つかりませんでした。", is_error=True)
    except Exception as e:
        show_message(f"エラーが発生しました: {e}", is_error=True)

def on_filter():
    for widget in input_frame.winfo_children():
        widget.destroy()

    selected_option = sql_file_combo.get()
    print("Selected option:", selected_option)  # デバッグ用
    sql_file_name = get_sql_file_name(selected_option)
    print("SQL file name:", sql_file_name)  # デバッグ用

    sheet = load_sheet_from_spreadsheet(sql_file_name)
    print("Loaded sheet:", sheet)  # デバッグ用
    data = get_filtered_data_from_sheet(sheet)
    print("Filtered data:", data)  # デバッグ用

    if data:
        create_dynamic_input_fields(input_frame, data)
    else:
        show_message("指定されている項目がありません", is_error=True)

def show_message(message, is_error=False):
    style = 'Error.TLabel' if is_error else 'Info.TLabel'
    message_label.config(text=message, style=style)

def center_window(root):
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width // 2) - (width // 2)
    y = (screen_height // 3) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

# GUIのセットアップ
root = tk.Tk()
root.title("CSV絞込ダウンロードツール")

# スタイルの設定
style = ttk.Style()
style.configure('Error.TLabel', foreground='red')
style.configure('Info.TLabel', foreground='black')

# メインフレーム
main_frame = ttk.Frame(root)
main_frame.pack(fill=tk.BOTH, expand=True)

# SQLファイル選択コンボボックスを囲むLabelFrameを作成
selection_frame = ttk.LabelFrame(main_frame, text="選択対象シート")
selection_frame.grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky='ew')

# SQLファイル選択コンボボックス
sql_file_combo = ttk.Combobox(selection_frame, state="readonly")
sql_file_combo.grid(row=0, column=0, padx=20, pady=10)

# 絞込ボタンの設置
filter_btn = ttk.Button(selection_frame, text="絞込", command=on_filter)
filter_btn.grid(row=0, column=1, padx=10, pady=10)

# 入力フィールドを含むフレームを囲むLabelFrameを作成
fields_container_frame = ttk.LabelFrame(main_frame, text="入力フィールド")
fields_container_frame.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky='nsew')

# 入力フィールドを含むフレーム
input_frame = ttk.Frame(fields_container_frame)
input_frame.pack(fill=tk.BOTH, expand=True)

main_frame.grid_rowconfigure(1, weight=1)
main_frame.grid_columnconfigure(0, weight=1)
main_frame.grid_columnconfigure(1, weight=1)

# チェックボックス用の変数を定義
header_check_var = tk.BooleanVar(value=False)

# ヘッダ行を含めないチェックボックスを追加
header_checkbox = ttk.Checkbutton(main_frame, text="ヘッダ行を含めない", variable=header_check_var)
header_checkbox.grid(row=2, column=0, padx=10, pady=10)

# CSV実行ボタン
execute_btn = ttk.Button(main_frame, text="CSVダウンロード", command=on_csv_execute)
execute_btn.grid(row=3, column=0, padx=10, pady=10)

# クリップボードにコピーするボタン
copy_btn = ttk.Button(main_frame, text="クリップボードにコピー", command=on_copy_to_clipboard)
copy_btn.grid(row=3, column=1, padx=10, pady=10)

# メッセージ表示用のLabel
message_label = ttk.Label(main_frame, text="", style="Info.TLabel")
message_label.grid(row=4, column=0, columnspan=2, sticky='w', padx=10, pady=10)

# GUI表示時にSQLファイルリストを自動的に読み込む
load_sql_list()

# ウィンドウの最小サイズを設定
root.minsize(800, 700)

# ウィンドウを画面中央に配置
center_window(root)

root.mainloop()
