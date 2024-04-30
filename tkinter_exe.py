import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
from subcode_tkinter_loader import load_sql_list_from_spreadsheet, get_sql_file_name,load_sheet_from_spreadsheet,get_filtered_data_from_sheet
from main_tkinter import execute_sql_file
import configparser
import datetime

# configparserの設定
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# スプレッドシートのシート名を定義
SHEET_NAME = config['Spreadsheet']['main_sheet']

def load_sql_list():
    # スプレッドシートからSQLファイルリストを読み込み
    sql_files_dict = load_sql_list_from_spreadsheet()  # 辞書型でSQLファイルリストを受け取る
    sql_file_combo['values'] = list(sql_files_dict.keys())  # キー(表示用テキスト)をプルダウンに設定
    show_message("SQLファイルリストを読み込みました。")  # GUIにメッセージを表示

input_fields = {}  # db_itemをキーとして、入力フィールドの参照を保持する辞書
input_fields_types = {}  # db_itemをキーとして、入力フィールドの参照を保持する辞書

options_dict = {}  # db_itemをキーとして、オプションリストを保存する辞書

def create_dynamic_input_fields(master, data):
    global input_fields, input_fields_types, options_dict
    input_fields.clear()
    input_fields_types.clear()
    options_dict.clear()

    row_index = 0
    for item in data:
        label_text = item['db_item']
        label = tk.Label(master, text=label_text)
        label.grid(row=row_index, column=0)

        if item['input_type'] == 'FA':
            input_field = tk.Entry(master)
            input_field.grid(row=row_index, column=1, sticky='ew')
            input_fields[item['db_item']] = input_field
            input_fields_types[item['db_item']] = 'FA'

        elif item['input_type'] == 'プルダウン':
            options = ['-'] + [option[1] for option in item['options']]
            input_field = ttk.Combobox(master, values=options, state="readonly")
            input_field.current(0)
            input_field.grid(row=row_index, column=1, sticky='ew')
            input_fields[item['db_item']] = input_field
            input_fields_types[item['db_item']] = 'プルダウン'
            options_dict[item['db_item']] = item['options']

        elif item['input_type'] == 'ラジオボタン':
            print(f"Processing radio button: {item['db_item']}")
            var = tk.StringVar(master, value="")  # 初期値を空文字列に設定
            radio_frame = tk.Frame(master)
            radio_frame.grid(row=row_index, column=1, sticky='ew')

            # ラジオボタンを作成
            radio_buttons = []
            for i, option in enumerate(item['options']):
                radio_button = tk.Radiobutton(radio_frame, text=option[1], variable=var, value=option[1], command=lambda v=var: on_radio_button_select(v, item['db_item']))  # 修正
                radio_button.grid(row=i // 5, column=i % 5)
                radio_buttons.append(radio_button)

            # 「選択を外す」チェックボックスを追加
            uncheck_var = tk.BooleanVar(value=False)  # デフォルトでチェックを外さない状態にする
            uncheck_checkbox = tk.Checkbutton(radio_frame, text="選択を外す", variable=uncheck_var, command=lambda v=var, r=radio_buttons, uv=uncheck_var: deselect_radio_buttons(v, r, uv, item['db_item']))  # 修正
            uncheck_checkbox.grid(row=(len(item['options']) + 1) // 5, column=(len(item['options']) + 1) % 5)

            # ラジオボタンの選択を解除する関数
            def deselect_radio_buttons(var, radio_buttons, uncheck_var, db_item): 
                if uncheck_var.get():
                    var.set("")
                    for radio_button in radio_buttons:
                        radio_button.deselect()
                else:
                    if var.get() == "":
                        uncheck_checkbox.deselect()

            # ラジオボタンを選択したときの処理
            def on_radio_button_select(var, db_item): 
                uncheck_var.set(False)

            input_fields[item['db_item']] = var
            input_fields_types[item['db_item']] = 'ラジオボタン'
            options_dict[item['db_item']] = item['options']

        elif item['input_type'] == 'チェックボックス':
            checkbox_frame = tk.Frame(master)
            checkbox_frame.grid(row=row_index, column=1, sticky='ew')
            var_dict = {}
            for i, option in enumerate(item['options']):
                var = tk.BooleanVar()
                checkbox = tk.Checkbutton(checkbox_frame, text=option[1], variable=var)
                checkbox.grid(row=i // 5, column=i % 5)
                var_dict[option[0]] = var
            input_fields[item['db_item']] = var_dict
            input_fields_types[item['db_item']] = 'チェックボックス'

        elif item['input_type'] == 'Date':
            start_date_field = DateEntry(master, date_pattern='yyyy/MM/dd')
            start_date_field.grid(row=row_index, column=1, sticky='ew')
            row_index += 1
            end_date_label = tk.Label(master, text=item['db_item'] + "（終了日）")
            end_date_label.grid(row=row_index, column=0)
            end_date_field = DateEntry(master, date_pattern='yyyy/MM/dd')
            end_date_field.grid(row=row_index, column=1, sticky='ew')
            input_fields[item['db_item']] = {'start_date_field': start_date_field, 'end_date_field': end_date_field}
            input_fields_types[item['db_item']] = 'Date'

        row_index += 1

def get_input_values():
    values = {}
    for db_item, field_info in input_fields.items():
        print(f"Processing {db_item}: type(field_info) = {type(field_info)}")

        if input_fields_types[db_item] == 'Date':
            start_date_field, end_date_field = field_info['start_date_field'], field_info['end_date_field']
            values[db_item] = {
                'start_date': start_date_field.get(),
                'end_date': end_date_field.get()
            }
            print(f"  Date field processed: start_date = {values[db_item]['start_date']}, end_date = {values[db_item]['end_date']}")

        elif isinstance(field_info, ttk.Combobox):
            selected_option_text = field_info.get()
            options_list = options_dict[db_item]
            selected_option_value = next((option[0] for option in options_list if option[1] == selected_option_text), None)
            values[db_item] = selected_option_value
            print(f"  Combobox selected: {selected_option_text}, value set to: {selected_option_value}")

        elif isinstance(field_info, tk.Entry):
            values[db_item] = field_info.get()
            print(f"  Entry field processed: {values[db_item]}")

        elif isinstance(field_info, tk.StringVar):
            selected_option = field_info.get()
            options_list = options_dict[db_item]
            selected_option_value = next((option[0] for option in options_list if option[1] == selected_option), None)
            values[db_item] = selected_option_value
            print(f"  Radio button selected: {selected_option}, value set to: {selected_option_value}")

        elif isinstance(field_info, dict) and input_fields_types[db_item] == 'チェックボックス':
            selected_options = [key for key, var in field_info.items() if var.get()]
            values[db_item] = selected_options
            print(f"  Checkboxes processed: {selected_options}, value set to: {values[db_item]}")

    print(f"Final return values: {values}")
    return values


#csvファイルダウンロードの実行
def on_csv_execute():
    global input_fields_types
    include_header = not header_check_var.get()  # ヘッダ行を含めるかどうかの選択

    try:
        selected_option = sql_file_combo.get()  # プルダウンで選択されたオプションを取得
        sql_file_name = get_sql_file_name(selected_option)  # 選択されたオプションに基づいてSQLファイル名を取得
        if sql_file_name:
            sheet_name = sql_file_name.replace('.sql', '')  # .sqlを除外してシート名を取得
            load_sheet_from_spreadsheet(sheet_name) #シートを検索
            selected_sheet = load_sheet_from_spreadsheet(sheet_name)
            get_filtered_data_from_sheet(selected_sheet) #絞込項目の辞書を取得
            full_sql_file_name = sql_file_name+".sql"
            print('full_sql_file_name:'+full_sql_file_name)

            # 入力値を辞書形式で取得
            input_values = get_input_values()
            # 取得した値をコンソールに出力
            print('input_values:',input_values)

            # SQLファイルを実行
            # execute_sql_file 関数の呼び出しに include_header を追加
            if execute_sql_file(full_sql_file_name, input_values, input_fields_types, 'download', include_header, root):
                show_message(f"{sheet_name} のSQLファイル実行に成功しました。")
            else:
                show_message("SQLファイルの実行に失敗しました。", is_error=True)
        else:
            show_message("指定された項目に対応するSQLファイルが見つかりませんでした。", is_error=True)
    except Exception as e:
        show_message(f"エラーが発生しました: {e}", is_error=True)

#クリップボードにコピーの実行
def on_copy_to_clipboard():
    global input_fields_types
    include_header = not header_check_var.get()  # ヘッダ行を含めるかどうかの選択

    try:
        selected_option = sql_file_combo.get()  # プルダウンで選択されたオプションを取得
        sql_file_name = get_sql_file_name(selected_option)  # 選択されたオプションに基づいてSQLファイル名を取得
        if sql_file_name:
            sheet_name = sql_file_name.replace('.sql', '')  # .sqlを除外してシート名を取得
            load_sheet_from_spreadsheet(sheet_name) #シートを検索
            selected_sheet = load_sheet_from_spreadsheet(sheet_name)
            get_filtered_data_from_sheet(selected_sheet) #絞込項目の辞書を取得
            full_sql_file_name = sql_file_name+".sql"
            print('full_sql_file_name:'+full_sql_file_name)

            # 入力値を辞書形式で取得
            input_values = get_input_values()
            # 取得した値をコンソールに出力
            print(input_values)

            # SQLファイルを実行
            # execute_sql_file 関数の呼び出しに include_header を追加
            if execute_sql_file(full_sql_file_name, input_values, input_fields_types, 'copy', include_header, root):
                show_message(f"{sheet_name} のSQLファイル実行に成功しました。")
            else:
                show_message("SQLファイルの実行に失敗しました。", is_error=True)
        else:
            show_message("指定された項目に対応するSQLファイルが見つかりませんでした。", is_error=True)
    except Exception as e:
        show_message(f"エラーが発生しました: {e}", is_error=True)

def on_filter():
    # 前の入力フィールドをクリア
    for widget in input_frame.winfo_children():
        widget.destroy()
    # 選択されたSQLファイル名を取得
    selected_option = sql_file_combo.get()
    sql_file_name = get_sql_file_name(selected_option)
    # スプレッドシートから条件に合うデータを取得
    sheet = load_sheet_from_spreadsheet(sql_file_name)
    data = get_filtered_data_from_sheet(sheet)
    # 入力フィールドを動的に生成
    create_dynamic_input_fields(input_frame, data)

def show_message(message, is_error=False):
    # 既存のメッセージをクリア
    message_label.config(text="")
    # エラーの場合は文字色を赤に、それ以外の場合は黒に設定
    message_label.config(fg="red" if is_error else "black")
    # 新しいメッセージを表示
    message_label.config(text=message)

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

# メインフレーム
main_frame = tk.Frame(root)
main_frame.pack(fill=tk.BOTH, expand=True)

# SQLファイル選択コンボボックス
sql_file_combo = ttk.Combobox(main_frame, state="readonly")
sql_file_combo.grid(row=0, column=0, padx=20, pady=10)  # プルダウンリストの位置を調整
# コンボボックスの選択イベントバインドは削除または修正する必要があるかもしれません

# 絞込ボタンの設置
filter_btn = ttk.Button(main_frame, text="絞込", command=on_filter)
filter_btn.grid(row=0, column=1, padx=10, pady=10)

# 入力フィールドを含むフレーム
input_frame = tk.Frame(main_frame)
input_frame.grid(row=1, column=0, columnspan=2, sticky='nsew', padx=10, pady=10)

# チェックボックス用の変数を定義（既にある場合は再定義不要）
header_check_var = tk.BooleanVar(value=False)

# ヘッダ行を含めないチェックボックスを追加
header_checkbox = ttk.Checkbutton(main_frame, text="ヘッダ行を含めない", variable=header_check_var)
header_checkbox.grid(row=2, column=0, padx=10, pady=10)  # 適切な位置に配置

# CSV実行ボタン
execute_btn = ttk.Button(main_frame, text="CSVダウンロード", command=on_csv_execute)
execute_btn.grid(row=3, column=0, padx=10, pady=10)

# クリップボードにコピーするボタン
copy_btn = ttk.Button(main_frame, text="クリップボードにコピー", command=on_copy_to_clipboard)
copy_btn.grid(row=3, column=1, padx=10, pady=10)

# メッセージ表示用のLabel
message_label = tk.Label(main_frame, text="", fg="gray")
message_label.grid(row=4, column=0, columnspan=2, sticky='w', padx=10, pady=10)

# GUI表示時にSQLファイルリストを自動的に読み込む
load_sql_list()

# ウィンドウの最小サイズを設定（必要に応じて調整）
root.minsize(600, 600)  

# ウィンドウを画面中央に配置
center_window(root)

root.mainloop()