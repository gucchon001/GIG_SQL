import streamlit as st

# Streamlitのアプリケーション設定は最初に行う必要があります
st.set_page_config(
    page_title="塾ステ CSVダウンロードツール ストミンくん β版",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 他のインポートは `st.set_page_config()` の後に配置
import subprocess
import configparser
import threading
from csv_download import csv_download  # 関数名を変更
from subcode_streamlit_loader import load_sql_list_from_spreadsheet
try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('main')
except ImportError:
    # フォールバック：旧構造
    from my_logging import setup_department_logger
    LOGGER = setup_department_logger('main')

# サイドバーのタイトルを小さくするためのCSSスタイル
sidebar_header = """
<style>
.sidebar .markdown-text-container h3 {
    font-size: 12px;
    margin: 0;
    padding: 0;
    line-height: 1.2;
    text-align: left;
}
.stRadio > div {
    flex-direction: column;
}
</style>
<div style="display: flex; align-items: flex-start;">
    <h3>塾ステ CSVダウンロードツール<br>ストミンくん β版</h3>
</div>
"""
st.sidebar.markdown(sidebar_header, unsafe_allow_html=True)

# セッションステートの初期化
if 'batch_status' not in st.session_state:
    st.session_state.batch_status = "未実行"  # "未実行", "実行中", "完了", "エラー"
    st.session_state.batch_output = ""

# 親階層の選択肢
parent_options = ["CSVダウンロード"]
selected_parent = st.sidebar.radio("メインメニュー", parent_options, index=0, key="parent_radio")

# バッチファイル実行関数
def run_batch_file(batch_file_path, table_name=None):
    try:
        if table_name:
            LOGGER.info(f"バッチファイルを実行開始: {batch_file_path} テーブル: {table_name}")
            # テーブル名を引数としてバッチファイルを実行
            result = subprocess.run(
                [batch_file_path, table_name],
                check=True,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'  # 正しいエンコーディングを設定
            )
        else:
            LOGGER.info(f"バッチファイルを実行開始: {batch_file_path}")
            # バッチファイルを実行
            result = subprocess.run(
                batch_file_path,
                check=True,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'  # 正しいエンコーディングを設定
            )
        LOGGER.info(f"バッチファイルの出力: {result.stdout}")
        st.session_state.batch_status = "完了"
        st.session_state.batch_output = result.stdout
        # 成功通知を表示
        st.toast("データソースの更新が完了しました。", icon="✅")
    except subprocess.CalledProcessError as e:
        LOGGER.error(f"バッチファイル実行中にエラーが発生しました: {e.stderr}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = e.stderr
        # エラー通知を表示
        st.toast("データソースの更新中にエラーが発生しました。", icon="❌")
    except Exception as e:
        LOGGER.error(f"最新更新ボタンの実行中にエラーが発生しました: {e}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = str(e)
        # エラー通知を表示
        st.toast("予期せぬエラーが発生しました。", icon="⚠️")

# 「CSVダウンロード」が選択された場合の処理
if selected_parent == "CSVダウンロード":
    # データ更新ボタンとバッチ実行ステータスの表示をラジオボタンの上に移動
    st.sidebar.markdown("---")
    
    # データ更新ボタンの追加（全件）
    if st.sidebar.button("データ更新（全件）"):
        if st.session_state.batch_status == "実行中":
            st.sidebar.warning("現在データ更新が実行中です。")
        else:
            # セッションステートを更新
            st.session_state.batch_status = "実行中"
            st.session_state.batch_output = ""
            
            # 新構造の設定管理を使用
            try:
                from src.core.config.settings import AppConfig
                app_config = AppConfig.from_config_file('config.ini')
                batch_file_path = app_config.batch.create_datasets
            except ImportError:
                # フォールバック：旧構造
                config = configparser.ConfigParser()
                config.read('config.ini', encoding='utf-8')
                batch_file_path = config['batch_exe']['create_datasets']
            
            # バッチファイルの存在確認
            if not subprocess.os.path.exists(batch_file_path):
                st.session_state.batch_status = "エラー"
                st.session_state.batch_output = f"バッチファイルが見つかりません: {batch_file_path}"
                LOGGER.error(f"バッチファイルが見つかりません: {batch_file_path}")
                # エラー通知を表示
                st.toast(f"バッチファイルが見つかりません: {batch_file_path}", icon="❌")
            else:
                # バッチファイルをバックグラウンドで実行
                thread = threading.Thread(target=run_batch_file, args=(batch_file_path,), daemon=True)
                thread.start()
                LOGGER.info("バッチファイルをバックグラウンドで実行しました。")
                # 実行開始通知を表示
                st.toast("データソースの更新を開始しました。", icon="⏳")

    # データ更新ボタンの追加（選択テーブル）
    if st.sidebar.button("データ更新（個別※まだ使えない）"):
        if st.session_state.batch_status == "実行中":
            st.sidebar.warning("現在データ更新が実行中です。")
        else:
            selected_table = st.session_state.get('selected_child', None)  # 選択されたテーブル名を取得
            if selected_table:
                st.session_state.batch_status = "実行中"
                st.session_state.batch_output = ""
                
                # 新構造の設定管理を使用
                try:
                    from src.core.config.settings import AppConfig
                    app_config = AppConfig.from_config_file('config.ini')
                    batch_file_path = app_config.batch.create_datasets_individual
                except ImportError:
                    # フォールバック：旧構造
                    config = configparser.ConfigParser()
                    config.read('config.ini', encoding='utf-8')
                    batch_file_path = config['batch_exe']['create_datasets_individual']
                
                # バッチファイルの存在確認
                if not subprocess.os.path.exists(batch_file_path):
                    st.session_state.batch_status = "エラー"
                    st.session_state.batch_output = f"バッチファイルが見つかりません: {batch_file_path}"
                    LOGGER.error(f"バッチファイルが見つかりません: {batch_file_path}")
                    # エラー通知を表示
                    st.toast(f"バッチファイルが見つかりません: {batch_file_path}", icon="❌")
                else:
                    # バッチファイルをバックグラウンドで実行し、選択されたテーブル名を渡す
                    thread = threading.Thread(target=run_batch_file, args=(batch_file_path, selected_table), daemon=True)
                    thread.start()
                    LOGGER.info(f"バッチファイルをバックグラウンドで実行しました。 テーブル: {selected_table}")
                    # 実行開始通知を表示
                    st.toast(f"データソースの更新を開始しました（テーブル: {selected_table}）。", icon="⏳")
            else:
                st.sidebar.error("テーブルが選択されていません。")

    # 親階層の選択肢の後に子階層のラジオボタンを配置
    # SQLファイル一覧の取得
    if 'sql_files_dict' not in st.session_state:
        try:
            st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()
            LOGGER.info("SQLファイル一覧を正常に読み込みました。")
        except Exception as e:
            LOGGER.error(f"SQLファイル一覧の読み込みに失敗しました: {e}")
            st.session_state['sql_files_dict'] = {}
            st.error("SQLファイル一覧の読み込みに失敗しました")
            st.error("「リスト再読み込み」ボタンを試してください")
            st.error(f"エラー詳細: {e}")
    
    sql_files_dict = st.session_state['sql_files_dict']
    
    if not sql_files_dict:
        LOGGER.warning("SQLファイル辞書が空です。")
        st.warning("SQLファイル一覧が空です。「リスト再読み込み」ボタンを試してください。")
        sql_file_display_names = []
    else:
        sql_file_display_names = list(sql_files_dict.keys())

    # 子階層の選択肢
    child_options = sql_file_display_names if sql_file_display_names else ["サンプル1", "サンプル2"]
    selected_child = st.sidebar.radio("サブメニュー", child_options, key="child_radio")

    # バッチ実行ステータスの表示
    st.sidebar.markdown("---")
    if st.session_state.batch_status == "実行中":
        st.sidebar.info("データソースの更新を実行中です。しばらくお待ちください。")
    elif st.session_state.batch_status == "完了":
        st.sidebar.success("データソースの更新が完了しました。")
        if st.session_state.batch_output:
            st.sidebar.text_area("バッチファイルの出力", st.session_state.batch_output, height=200)
    elif st.session_state.batch_status == "エラー":
        st.sidebar.error("データソースの更新中にエラーが発生しました。")
        if st.session_state.batch_output:
            st.sidebar.text_area("エラー詳細", st.session_state.batch_output, height=200)

    # リスト再読み込みボタンを追加（CSVダウンロードが選択されているときのみ）
    st.sidebar.markdown("---")
    if st.sidebar.button("リスト再読み込み"):
        try:
            # キャッシュをクリア
            if hasattr(load_sql_list_from_spreadsheet, 'clear'):
                load_sql_list_from_spreadsheet.clear()
            # セッションステートからも削除
            if 'sql_files_dict' in st.session_state:
                del st.session_state['sql_files_dict']
            st.session_state['sql_files_dict'] = load_sql_list_from_spreadsheet()
            st.rerun()
        except Exception as e:
            st.sidebar.error("リストの再読み込み中にエラーが発生しました。")
            st.sidebar.write(f"エラー詳細: {e}")
            LOGGER.error(f"リスト再読み込み中にエラーが発生しました: {e}")

    # フィルタキャッシュクリアボタンを追加
    if st.sidebar.button("フィルタ設定リセット"):
        try:
            # すべてのキャッシュをクリア
            from subcode_streamlit_loader import load_sheet_from_spreadsheet
            if hasattr(load_sheet_from_spreadsheet, 'clear'):
                load_sheet_from_spreadsheet.clear()
            if hasattr(load_sql_list_from_spreadsheet, 'clear'):
                load_sql_list_from_spreadsheet.clear()
            # フィルタ関連のセッションステートをクリア
            keys_to_clear = ['input_fields', 'input_fields_types', 'options_dict', 'last_selected_table']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.sidebar.success("フィルタ設定をリセットしました。")
            st.rerun()
        except Exception as e:
            st.sidebar.error("フィルタ設定のリセット中にエラーが発生しました。")
            st.sidebar.write(f"エラー詳細: {e}")
            LOGGER.error(f"フィルタ設定リセット中にエラーが発生しました: {e}")

    # 選択に応じてCSVファイルを表示
    try:
        LOGGER.info(f"Calling csv_download function with {selected_child}")
        csv_download(selected_child)
        LOGGER.info("CSV download function call completed")
    except Exception as e:
        st.error(f"CSVダウンロード中にエラーが発生しました: {e}")
        LOGGER.error(f"CSVダウンロード中にエラーが発生しました: {e}")

# ダッシュボード機能は削除されました
