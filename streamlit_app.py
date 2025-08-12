import streamlit as st

# Streamlitのアプリケーション設定は最初に行う必要があります
st.set_page_config(
    page_title="塾ステ CSVダウンロードツール ストミンくん v2.0",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 他のインポートは `st.set_page_config()` の後に配置
import subprocess
import configparser
import threading
import os
import time
import re

# API制限対策：キャッシュクリア機能
@st.cache_data(ttl=300)  # 5分間キャッシュ
def get_spreadsheet_data_cached():
    """スプレッドシートデータを5分間キャッシュして取得"""
    return None
from core.streamlit.csv_download import csv_download  # 関数名を変更
from core.streamlit.subcode_streamlit_loader import load_sql_list_from_spreadsheet
try:
    # 新構造のログ管理を優先使用
    from src.core.logging.logger import get_logger
    LOGGER = get_logger('streamlit')
except ImportError:
    # フォールバック：旧構造
    from core.config.my_logging import setup_department_logger
    LOGGER = setup_department_logger('streamlit', app_type='streamlit')

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
    <h3>塾ステ CSVダウンロードツール<br>ストミンくん v2.0</h3>
</div>
"""
st.sidebar.markdown(sidebar_header, unsafe_allow_html=True)

# セッションステートの初期化
if 'batch_status' not in st.session_state:
    st.session_state.batch_status = "未実行"  # "未実行", "実行中", "完了", "エラー"
    st.session_state.batch_output = ""

# 🔔 完了・エラー通知の表示のみ（プログレスバーなし）

# run_batch_file関数の定義
def run_batch_file(script_file_path, table_name=None):
    """バッチファイルを実行する関数"""
    try:
        # 実行開始時間を記録（毎回更新）
        st.session_state.batch_start_time = time.time()
        st.session_state.batch_status = "実行中"
            
        # PowerShellスクリプトを実行
        if table_name:
            LOGGER.info(f"PowerShellスクリプトを実行開始: {script_file_path} テーブル: {table_name}")
            result = subprocess.run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", script_file_path, "-TableName", table_name],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=1800  # 30分でタイムアウト
            )
        else:
            LOGGER.info(f"PowerShellスクリプトを実行開始: {script_file_path}")
            result = subprocess.run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", script_file_path],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=1800  # 30分でタイムアウト
            )
        
        LOGGER.info(f"バッチファイルの出力: {result.stdout}")
        
        # 実行結果から詳細情報を抽出
        import re
        from datetime import datetime
        
        # 完了時刻を設定
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.batch_completion_time = completion_time
        
        # レコード数を抽出（出力から「XXX レコード処理完了」を検索）
        record_match = re.search(r'(\d+) レコード処理完了', result.stdout)
        if record_match:
            st.session_state.batch_records_count = int(record_match.group(1))
        else:
            # 代替パターンを試す
            record_match = re.search(r'(\d+) レコード', result.stdout)
            if record_match:
                st.session_state.batch_records_count = int(record_match.group(1))
            else:
                st.session_state.batch_records_count = "不明"
        
        # 処理されたファイル名を抽出
        file_match = re.search(r'処理完了: (\w+)', result.stdout)
        if file_match:
            st.session_state.batch_processed_table = file_match.group(1)
        
        st.session_state.batch_status = "完了"
        st.session_state.batch_output = result.stdout
        st.session_state.show_success_toast = True  # 成功トーストフラグ
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
        
        LOGGER.info(f"処理完了 - 時刻: {completion_time}, レコード数: {st.session_state.get('batch_records_count', '不明')}")
    except subprocess.CalledProcessError as e:
        LOGGER.error(f"バッチファイル実行中にエラーが発生しました: {e.stderr}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = e.stderr
        st.session_state.show_error_toast = True  # エラートーストフラグ
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
    except Exception as e:
        LOGGER.error(f"最新更新ボタンの実行中にエラーが発生しました: {e}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = str(e)
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
        
        # エラー通知を表示
        st.toast("予期せぬエラーが発生しました。", icon="⚠️")

# pending_batch_executionフラグをチェックしてバッチ実行を開始
if 'pending_batch_execution' in st.session_state:
    selected_table = st.session_state.pending_batch_execution
    del st.session_state.pending_batch_execution  # フラグをクリア
    
    # 現在選択中のテーブル名を保存
    st.session_state.current_selected_table = selected_table
    
    # 実行開始時刻を記録
    st.session_state.batch_start_time = time.time()
    st.session_state.batch_status = "実行中"
    
    # バッチファイル実行を開始
    # 新構造の設定管理を使用
    try:
        from src.core.config.settings import AppConfig
        app_config = AppConfig.from_config_file('config/settings.ini')
        batch_file_path = app_config.batch.create_datasets_individual
    except ImportError:
        # フォールバック：旧構造
        import configparser
        config = configparser.ConfigParser()
        config.read('config/settings.ini', encoding='utf-8')
        batch_file_path = config['batch_exe']['create_datasets_individual']
    
    # スクリプトファイルの存在確認（相対パス対応）
    script_full_path = os.path.abspath(batch_file_path)
    if not os.path.exists(script_full_path):
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = f"スクリプトファイルが見つかりません: {script_full_path}"
        LOGGER.error(f"スクリプトファイルが見つかりません: {script_full_path}")
        # エラー通知を表示
        st.toast(f"スクリプトファイルが見つかりません: {batch_file_path}", icon="❌")
    else:
        # スクリプトファイルをバックグラウンドで実行し、選択されたテーブル名を渡す
        thread = threading.Thread(target=run_batch_file, args=(script_full_path, selected_table), daemon=True)
        thread.start()
        LOGGER.info(f"バッチファイルをバックグラウンドで実行しました。 テーブル: {selected_table}")
        # 実行開始通知を表示
        st.toast(f"データソースの更新を開始しました（テーブル: {selected_table}）。", icon="⏳")



# バックグラウンド実行状態のチェック（UI表示なし）
def check_background_completion():
    """バックグラウンド処理の完了をチェック"""
    try:
        import os
        import re
        log_file = 'logs/datasets.log'
        if not os.path.exists(log_file):
            return False
            
        # ファイルの最後の5KB程度を読み取り（より多く読む）
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(0, 2)  # ファイル末尾に移動
            file_size = f.tell()
            read_size = min(5120, file_size)
            f.seek(max(0, file_size - read_size))
            tail_content = f.read()
            
        # より厳密な完了判定：完全なメッセージのみ検知
        final_completion_indicators = [
            '🎉 全ての処理が正常に完了しました - SUCCESS',  # 最終完了メッセージ
            '全ての処理が正常に完了しました',  # 日本語版
            'All processes completed successfully',  # 英語版
        ]
        
        # ログファイルの最終更新時間をチェック
        import os, time
        current_time = time.time()
        file_mtime = os.path.getmtime(log_file)
        time_since_update = current_time - file_mtime
        
        # 最終完了メッセージがある場合、2分以上更新されていない場合は完了とみなす
        # 最終完了メッセージがない場合、5分以上更新されていない場合は完了とみなす
        has_final_message = any(indicator in tail_content for indicator in final_completion_indicators)
        timeout_threshold = 120 if has_final_message else 300  # 2分 or 5分
        
        if time_since_update > timeout_threshold:
            LOGGER.info(f"ログファイルが{timeout_threshold//60}分以上更新されていないため完了とみなします: {time_since_update:.1f}秒前")
            return True
        
        # 正常な日本語での成功パターンのみ（文字化け対応は削除）
        success_patterns = [
            r'処理完了',
            r'Parquet.*正常.*保存.*ました',
            r'(\d+).*レコード.*処理完了',
            r'正常に保存されました',
            r'Parquetファイルが正常に保存されました',
            r'処理完了.*->',
            r'ログシートに書き込みました.*成功',
            r'\d+.*レコード',
            r'Success',
            r'success',
            r'完了',
        ]
        
        # エラーパターン（正常な日本語と英語のみ）
        error_patterns = [
            r'MySQL Connection not available',
            r'OperationalError',
            r'Connection.*failed',
            r'Timeout',
            r'Error',
            r'エラーが発生しました',
            r'エラー',
            r'失敗',
            r'Exception',
            r'Traceback',
        ]
        
        # 最終完了メッセージをチェック - ただし最後の行にある場合のみ有効
        lines = tail_content.strip().split('\n')
        last_few_lines = lines[-3:]  # 最後の3行をチェック
        
        for indicator in final_completion_indicators:
            # 最後の数行のいずれかに完了メッセージがあるかチェック
            found_in_recent_lines = any(indicator in line for line in last_few_lines)
            
            if found_in_recent_lines:
                # バッチ開始時刻が設定されている場合、その後のメッセージのみ有効
                if hasattr(st.session_state, 'batch_start_time') and st.session_state.batch_start_time:
                    # ログファイルの最終更新時刻がバッチ開始時刻より後かチェック
                    if file_mtime > st.session_state.batch_start_time:
                        LOGGER.info(f"最終完了メッセージを最後の行で検知（バッチ開始後）: {indicator}")
                        return True
                    else:
                        LOGGER.info(f"完了メッセージはバッチ開始前のものです: {indicator}")
                        continue
                else:
                    LOGGER.info(f"最終完了メッセージを最後の行で検知: {indicator}")
                    return True
        
        # エラーパターンをチェック（エラーがあれば完了とみなす）
        for pattern in error_patterns:
            if re.search(pattern, tail_content):
                LOGGER.warning(f"エラーパターンを検知: {pattern}")
                st.session_state.batch_status = "エラー"
                st.session_state.batch_output = f"エラーが検出されました: {pattern}"
                return True
        
        # デバッグ情報を詳細に記録
        LOGGER.info(f"ログ末尾の内容（最後の500文字）: {tail_content[-500:]}")
        return False
    except Exception as e:
        LOGGER.warning(f"完了チェックエラー: {e}")
        return False

# 実行中の場合はバックグラウンドで処理完了をチェック
if st.session_state.batch_status == "実行中":
    if check_background_completion():
        st.session_state.batch_status = "完了"
        st.session_state.batch_completion_time = time.strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.show_success_toast = True
        st.rerun()
    else:
        # バックグラウンド実行中のステータス表示（ページ保持）
        st.info("🔄 バックグラウンドでデータ更新処理を実行中です...")
        
        # 実行時間を計算して表示
        if 'batch_start_time' in st.session_state:
            elapsed_time = time.time() - st.session_state.batch_start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            st.info(f"⏱️ 実行時間: {minutes}分{seconds}秒")
        
        # 30秒間隔で自動チェック
        import datetime
        if 'last_auto_check' not in st.session_state:
            st.session_state.last_auto_check = time.time()
        
        current_time = time.time()
        if current_time - st.session_state.last_auto_check > 30:  # 30秒間隔
            st.session_state.last_auto_check = current_time
            st.rerun()
        
        # 手動更新ボタンを提供
        if st.button("🔄 状況確認", key="manual_check_button"):
            # 手動チェック時にデバッグ情報も表示
            completion_result = check_background_completion()
            st.info(f"完了チェック結果: {completion_result}")
            
            # ログファイルの最終更新時間も表示
            import os
            log_file = 'logs/datasets.log'
            if os.path.exists(log_file):
                import datetime
                mtime = os.path.getmtime(log_file)
                last_modified = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                st.info(f"ログファイル最終更新: {last_modified}")
            
            st.rerun()
        
        # バッチ状態のリセットボタンを追加
        if st.button("🔧 状態リセット", key="reset_batch_status", help="処理が正常に完了したのに状態が変わらない場合に使用"):
            st.session_state.batch_status = "待機"
            if 'batch_start_time' in st.session_state:
                del st.session_state.batch_start_time
            if 'current_selected_table' in st.session_state:
                del st.session_state.current_selected_table
            st.success("バッチ状態をリセットしました")
            st.rerun()
        
        # 現在のページ情報を表示
        if 'current_selected_table' in st.session_state:
            st.info(f"📊 更新対象: {st.session_state.current_selected_table}")
        
        # ページ遷移を無効化（更新中はトップページに戻らない）
        st.markdown("""
        <style>
        .main > div {
            padding-top: 0rem;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # 30秒間隔でのみ自動チェック（UI固定なし）
        if 'last_auto_check' not in st.session_state:
            st.session_state.last_auto_check = time.time()
        
        current_time = time.time()
        if current_time - st.session_state.last_auto_check >= 30:
            st.session_state.last_auto_check = current_time
            # 自動でチェックのみ実行、即座にrerun()
            st.rerun()

# ポップアップ通知（完了・エラー時のみ）
elif st.session_state.batch_status == "エラー":
    # エラー状態でも最上部に表示
    st.error("❌ データソースの更新中にエラーが発生しました。")
    if st.session_state.batch_output:
        st.text_area("エラー詳細", st.session_state.batch_output, height=200)
    st.markdown("---")

elif st.session_state.batch_status == "タイムアウト":
    # タイムアウト状態の表示
    st.warning("⏰ 処理がタイムアウトしました。")
    st.info("💡 ページを手動で更新（F5）して最新の状態を確認してください。")
    
    # 状態をリセットするボタン
    if st.button("🔄 状態をリセット"):
        st.session_state.batch_status = "未実行"
        # ファイル監視関連の状態もクリア
        for key in ['initial_file_size', 'initial_file_mtime', 'last_change_time', 'latest_file_size', 'latest_file_mtime', 'batch_start_time']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    st.markdown("---")

# 親階層の選択肢
parent_options = ["CSVダウンロード"]
selected_parent = st.sidebar.radio("メインメニュー", parent_options, index=0, key="parent_radio")

# スクリプト実行関数（PowerShell対応）
def run_batch_file(script_file_path, table_name=None):
    try:
        # ファイル拡張子でPowerShellかバッチファイルかを判定
        if script_file_path.endswith('.ps1'):
            # PowerShellスクリプト実行
            if table_name:
                LOGGER.info(f"PowerShellスクリプトを実行開始: {script_file_path} テーブル: {table_name}")
                result = subprocess.run(
                   ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", script_file_path, "-TableName", table_name, "-Mode", "test"],
                   check=True,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE,
                   text=True,
                   encoding='cp932'  # Shift-JIS エンコーディング対応
               )
            else:
                LOGGER.info(f"PowerShellスクリプトを実行開始: {script_file_path}")
                result = subprocess.run(
                    ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", script_file_path],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='cp932'  # Shift-JIS エンコーディング対応
                )
        else:
            # バッチファイル実行（従来の処理）
            if table_name:
                LOGGER.info(f"バッチファイルを実行開始: {script_file_path} テーブル: {table_name}")
                result = subprocess.run(
                    [script_file_path, table_name],
                    check=True,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8'
                )
            else:
                LOGGER.info(f"バッチファイルを実行開始: {script_file_path}")
                result = subprocess.run(
                    script_file_path,
                    check=True,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8'
                )
        LOGGER.info(f"バッチファイルの出力: {result.stdout}")
        
        # 実行結果から詳細情報を抽出
        import re
        from datetime import datetime
        
        # 完了時刻を設定
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.batch_completion_time = completion_time
        
        # レコード数を抽出（出力から「XXX レコード処理完了」を検索）
        record_match = re.search(r'(\d+) レコード処理完了', result.stdout)
        if record_match:
            st.session_state.batch_records_count = int(record_match.group(1))
        else:
            # 代替パターンを試す
            record_match = re.search(r'(\d+) レコード', result.stdout)
            if record_match:
                st.session_state.batch_records_count = int(record_match.group(1))
            else:
                st.session_state.batch_records_count = "不明"
        
        # 処理されたファイル名を抽出
        file_match = re.search(r'処理完了: (\w+)', result.stdout)
        if file_match:
            st.session_state.batch_processed_table = file_match.group(1)
        
        st.session_state.batch_status = "完了"
        st.session_state.batch_output = result.stdout
        st.session_state.show_success_toast = True  # 成功トーストフラグ
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
        
        LOGGER.info(f"処理完了 - 時刻: {completion_time}, レコード数: {st.session_state.get('batch_records_count', '不明')}")
    except subprocess.CalledProcessError as e:
        LOGGER.error(f"バッチファイル実行中にエラーが発生しました: {e.stderr}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = e.stderr
        st.session_state.show_error_toast = True  # エラートーストフラグ
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
    except Exception as e:
        LOGGER.error(f"最新更新ボタンの実行中にエラーが発生しました: {e}")
        st.session_state.batch_status = "エラー"
        st.session_state.batch_output = str(e)
        
        # バックグラウンドスレッドからはst.rerun()が使えないため削除
        
        # エラー通知を表示
        st.toast("予期せぬエラーが発生しました。", icon="⚠️")

# 「CSVダウンロード」が選択された場合の処理
if selected_parent == "CSVダウンロード":
    # データ更新ボタンとバッチ実行ステータスの表示をラジオボタンの上に移動
    st.sidebar.markdown("---")
    
    # データ更新ボタンの追加（全件）
    update_all_disabled = st.session_state.batch_status == "実行中"
    if st.sidebar.button("データ更新（全件）", disabled=update_all_disabled):
        if st.session_state.batch_status == "実行中":
            st.sidebar.warning("現在データ更新が実行中です。")
        else:
            # セッションステートを更新
            st.session_state.batch_status = "実行中"
            st.session_state.batch_output = ""
            
            # 新構造の設定管理を使用
            try:
                from src.core.config.settings import AppConfig
                app_config = AppConfig.from_config_file('config/settings.ini')
                batch_file_path = app_config.batch.create_datasets
            except ImportError:
                # フォールバック：旧構造
                config = configparser.ConfigParser()
                config.read('config/settings.ini', encoding='utf-8')
                batch_file_path = config['batch_exe']['create_datasets']
            
            # スクリプトファイルの存在確認（相対パス対応）
            script_full_path = os.path.abspath(batch_file_path)
            if not os.path.exists(script_full_path):
                st.session_state.batch_status = "エラー"
                st.session_state.batch_output = f"スクリプトファイルが見つかりません: {script_full_path}"
                LOGGER.error(f"スクリプトファイルが見つかりません: {script_full_path}")
                # エラー通知を表示
                st.toast(f"スクリプトファイルが見つかりません: {batch_file_path}", icon="❌")
            else:
                # スクリプトファイルをバックグラウンドで実行
                thread = threading.Thread(target=run_batch_file, args=(script_full_path,), daemon=True)
                thread.start()
                LOGGER.info("バッチファイルをバックグラウンドで実行しました。")
                # 実行開始通知を表示
                st.toast("データソースの更新を開始しました。", icon="⏳")
    


    # データ更新ボタンの追加（選択テーブル）
    update_individual_disabled = st.session_state.batch_status == "実行中"
    
    # ボタンのキーを指定して重複クリックを防ぐ
    if st.sidebar.button("データ更新（個別）", disabled=update_individual_disabled, key="update_individual_btn"):
        LOGGER.info("🔘 「データ更新（個別）」ボタンがクリックされました！")
        if st.session_state.batch_status == "実行中":
            LOGGER.warning("既に実行中のため、処理をスキップします")
            st.sidebar.warning("現在データ更新が実行中です。")
        else:
            LOGGER.info("新しい処理を開始します")
            # 選択されたテーブル表示名を取得
            selected_display_name = st.session_state.get('child_radio', None)
            LOGGER.info(f"選択されたテーブル表示名: {selected_display_name}")
            
            # 個別実行シートから正確なメインテーブル名を取得
            selected_table = None
            if selected_display_name and 'sql_files_dict' in st.session_state:
                sql_file_name = st.session_state['sql_files_dict'].get(selected_display_name)
                LOGGER.info(f"SQLファイル名: {sql_file_name}")
                
                if sql_file_name:
                    # 個別実行シートからメインテーブル名を取得
                    try:
                        from core.streamlit.subcode_streamlit_loader import load_sheet_from_spreadsheet
                        import configparser
                        
                        config = configparser.ConfigParser()
                        config.read('config/settings.ini', encoding='utf-8')
                        sheet_name = config['Spreadsheet']['eachdata_sheet']
                        
                        # シートからデータを取得
                        sheet = load_sheet_from_spreadsheet(sheet_name)
                        if sheet:
                            data = sheet.get_all_values()
                            if data:
                                header = data[0]
                                # ヘッダーからインデックスを取得
                                try:
                                    sql_file_index = header.index('sqlファイル名')
                                    main_table_index = header.index('メインテーブル')
                                    
                                    # 対応するメインテーブル名を検索
                                    for row in data[1:]:
                                        if len(row) > max(sql_file_index, main_table_index):
                                            if row[sql_file_index] == sql_file_name:
                                                selected_table = row[main_table_index]
                                                LOGGER.info(f"個別実行シートから取得したメインテーブル名: {selected_table}")
                                                break
                                    
                                    if not selected_table:
                                        LOGGER.warning(f"SQLファイル '{sql_file_name}' に対応するメインテーブル名が見つかりませんでした。")
                                        
                                except ValueError as e:
                                    LOGGER.error(f"必要なヘッダー（sqlファイル名またはメインテーブル）が見つかりません: {e}")
                            else:
                                LOGGER.error("個別実行シートにデータがありません。")
                        else:
                            LOGGER.error("個別実行シートの読み込みに失敗しました。")
                            
                    except Exception as e:
                        LOGGER.error(f"個別実行シートからメインテーブル名を取得中にエラーが発生: {e}")
                        # フォールバック：SQLファイル名から推定
                        base_name = sql_file_name.replace('.sql', '')
                        if '_' in base_name:
                            selected_table = base_name.split('_')[-1]
                        else:
                            selected_table = base_name
                        LOGGER.info(f"フォールバック: SQLファイル名から推定したメインテーブル名: {selected_table}")
            
            if selected_table:
                LOGGER.info(f"メインテーブル名が取得できました: {selected_table}")
                LOGGER.info("セッション状態を「実行中」に変更します")
                st.session_state.batch_status = "実行中"
                st.session_state.batch_output = ""
                # バッチ開始時刻をリセット（他の状態もクリア）
                import time
                st.session_state.batch_start_time = time.time()
                
                # 古い状態をクリア
                for key in ['batch_completion_time', 'batch_records_count', 'batch_file_size', 'batch_processed_table']:
                    if key in st.session_state:
                        del st.session_state[key]
                        
                LOGGER.info(f"現在のbatch_status: {st.session_state.batch_status}")
                
                # 初期ファイル情報を記録（進捗計算のため）
                try:
                    config = configparser.ConfigParser()
                    config.read('config/settings.ini', encoding='utf-8')
                    nas_base_path = config['Paths']['csv_base_path']
                    sql_file_name = st.session_state['sql_files_dict'].get(selected_display_name, '')
                    base_name = sql_file_name.replace('.sql', '')
                    target_file = f"{nas_base_path}/{base_name}.parquet"
                    
                    if os.path.exists(target_file):
                        stat = os.stat(target_file)
                        st.session_state.initial_file_size = stat.st_size
                        st.session_state.initial_file_mtime = stat.st_mtime
                except Exception as e:
                    LOGGER.warning(f"初期ファイル情報記録エラー: {e}")
                
                # バッチ実行開始フラグを設定（即座にプログレスバーを表示するため）
                st.session_state.pending_batch_execution = selected_table
                
                # **即座にUIを更新してプログレスバーを表示**
                st.rerun()
            else:
                LOGGER.error("メインテーブル名が取得できませんでした")
                if not selected_display_name:
                    LOGGER.error("selected_display_name が None です")
                    st.sidebar.error("テーブルが選択されていません。左サイドバーのサブメニューからテーブルを選択してください。")
                else:
                    LOGGER.error(f"SQLファイル名: {st.session_state['sql_files_dict'].get(selected_display_name, 'NotFound')}")
                    st.sidebar.error(f"選択されたテーブル '{selected_display_name}' のメインテーブル名を取得できませんでした。")

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

    # 学習データ収集モードの処理
    if st.session_state.batch_status == "学習準備":
        if st.session_state.learning_current_index < len(st.session_state.learning_tables):
            current_table = st.session_state.learning_tables[st.session_state.learning_current_index]
            st.sidebar.info(f"📊 学習データ収集中... ({st.session_state.learning_current_index + 1}/{len(st.session_state.learning_tables)})")
            st.sidebar.info(f"現在処理中: {current_table}")
            
            # 状態を実行中に変更
            st.session_state.batch_status = "実行中"
            st.session_state.batch_start_time = time.time()
            st.session_state.learning_current_table = current_table  # 現在処理中のテーブルを記録
            
            # 個別実行ロジックを実行
            selected_display_name = current_table
            if selected_display_name and 'sql_files_dict' in st.session_state:
                sql_file_name = st.session_state['sql_files_dict'].get(selected_display_name)
                if sql_file_name:
                    try:
                        from core.streamlit.subcode_streamlit_loader import load_sheet_from_spreadsheet
                        import configparser
                        
                        config = configparser.ConfigParser()
                        config.read('config/settings.ini', encoding='utf-8')
                        sheet_name = config['Spreadsheet']['eachdata_sheet']
                        
                        sheet = load_sheet_from_spreadsheet(sheet_name)
                        if sheet:
                            data = sheet.get_all_values()
                            if data:
                                header = data[0]
                                try:
                                    sql_file_index = header.index('sqlファイル名')
                                    main_table_index = header.index('メインテーブル')
                                    
                                    selected_table = None
                                    for row in data[1:]:
                                        if len(row) > max(sql_file_index, main_table_index):
                                            if row[sql_file_index] == sql_file_name:
                                                selected_table = row[main_table_index]
                                                break
                                    
                                    if selected_table:
                                        # 個別実行を開始
                                        try:
                                            from src.core.config.settings import AppConfig
                                            app_config = AppConfig.from_config_file('config/settings.ini')
                                            batch_file_path = app_config.batch.create_datasets_individual
                                        except ImportError:
                                            import configparser
                                            config = configparser.ConfigParser()
                                            config.read('config/settings.ini', encoding='utf-8')
                                            batch_file_path = config['batch_exe']['create_datasets_individual']
                                        
                                        script_full_path = os.path.abspath(batch_file_path)
                                        if os.path.exists(script_full_path):
                                            thread = threading.Thread(target=run_batch_file, args=(script_full_path, selected_table), daemon=True)
                                            thread.start()
                                            LOGGER.info(f"学習データ収集: {current_table}({selected_table})の処理を開始")
                                        else:
                                            LOGGER.error(f"スクリプトファイルが見つかりません: {script_full_path}")
                                            # 次のテーブルに進む
                                            st.session_state.learning_current_index += 1
                                            if st.session_state.learning_current_index >= len(st.session_state.learning_tables):
                                                st.session_state.batch_status = "完了"
                                                st.session_state.learning_mode = False
                                            else:
                                                st.session_state.batch_status = "学習準備"
                                                st.rerun()
                                    else:
                                        LOGGER.error(f"テーブル {current_table} のメインテーブル名が見つかりません")
                                        # 次のテーブルに進む
                                        st.session_state.learning_current_index += 1
                                        if st.session_state.learning_current_index >= len(st.session_state.learning_tables):
                                            st.session_state.batch_status = "完了"
                                            st.session_state.learning_mode = False
                                        else:
                                            st.session_state.batch_status = "学習準備"
                                            st.rerun()
                                        
                                except ValueError:
                                    LOGGER.error("学習データ収集: 必要なヘッダーが見つかりません")
                                    # 次のテーブルに進む
                                    st.session_state.learning_current_index += 1
                                    if st.session_state.learning_current_index >= len(st.session_state.learning_tables):
                                        st.session_state.batch_status = "完了"
                                        st.session_state.learning_mode = False
                                    else:
                                        st.session_state.batch_status = "学習準備"
                                        st.rerun()
                    except Exception as e:
                        LOGGER.error(f"学習データ収集エラー: {e}")
                        # 次のテーブルに進む
                        st.session_state.learning_current_index += 1
                        if st.session_state.learning_current_index >= len(st.session_state.learning_tables):
                            st.session_state.batch_status = "完了"
                            st.session_state.learning_mode = False
                        else:
                            st.session_state.batch_status = "学習準備"
                            st.rerun()
                else:
                    LOGGER.error(f"SQLファイル名が見つかりません: {current_table}")
                    # 次のテーブルに進む
                    st.session_state.learning_current_index += 1
                    if st.session_state.learning_current_index >= len(st.session_state.learning_tables):
                        st.session_state.batch_status = "完了"
                        st.session_state.learning_mode = False
                    else:
                        st.session_state.batch_status = "学習準備"
                        st.rerun()
            else:
                LOGGER.error("sql_files_dict が見つかりません")
                # 学習モードを終了
                st.session_state.batch_status = "完了"
                st.session_state.learning_mode = False
        else:
            # 全ての学習が完了
            st.session_state.batch_status = "完了"
            st.session_state.learning_mode = False

    # サイドバーに簡潔な状況表示
    st.sidebar.markdown("---")
    if st.session_state.batch_status == "実行中":
        if st.session_state.get('learning_mode', False):
            current_index = st.session_state.get('learning_current_index', 0)
            total_tables = len(st.session_state.get('learning_tables', []))
            current_table = st.session_state.get('learning_current_table', '不明')
            st.sidebar.info(f"📊 学習中... ({current_index + 1}/{total_tables})")
            st.sidebar.info(f"処理中: {current_table}")
        else:
            st.sidebar.info("🔄 更新中...")
    elif st.session_state.batch_status == "完了":
        # 学習モードかどうかをチェック
        if st.session_state.get('learning_mode', False):
            # 次のテーブルがあるかチェック
            current_index = st.session_state.get('learning_current_index', 0)
            total_tables = len(st.session_state.get('learning_tables', []))
            
            if current_index + 1 < total_tables:
                # まだ処理するテーブルがある - 次のテーブルに進む
                st.session_state.learning_current_index += 1
                st.session_state.batch_status = "学習準備"
                next_table = st.session_state.learning_tables[st.session_state.learning_current_index]
                st.sidebar.info(f"📊 学習継続中... ({st.session_state.learning_current_index + 1}/{total_tables})")
                st.sidebar.info(f"次: {next_table}")
                st.rerun()  # 次のテーブル処理を開始
            else:
                # すべてのテーブル処理完了
                st.sidebar.success("📊 全テーブルの学習データ収集が完了しました！")
                st.sidebar.info("✅ 次回からより正確な進捗予想が可能になります")
                # 学習データの分析結果を表示
                try:
                    performance_data = analyze_historical_performance()
                    if performance_data:
                        st.sidebar.info(f"📈 収集データ数: {len(performance_data)} 件")
                        recent_data = performance_data[-5:]
                        avg_time = sum(p['estimated_time'] for p in recent_data) / len(recent_data)
                        st.sidebar.info(f"⏱️ 平均処理時間: {avg_time:.1f}秒")
                except:
                    pass
                # 学習モードフラグをリセット
                st.session_state.learning_mode = False
        else:
            st.sidebar.success("🎉 データソースの更新が完了しました！")
        
        # 共通の完了情報表示
        if 'batch_completion_time' in st.session_state:
            st.sidebar.info(f"⏰ 完了時刻: {st.session_state.batch_completion_time}")
        if 'batch_records_count' in st.session_state:
            st.sidebar.info(f"📊 更新レコード数: {st.session_state.batch_records_count:,} 件")
        # 成功トースト表示（一度だけ）- 学習モード継続中は表示しない
        if st.session_state.get('show_success_toast', False):
            learning_mode = st.session_state.get('learning_mode', False)
            current_index = st.session_state.get('learning_current_index', 0)
            total_tables = len(st.session_state.get('learning_tables', []))
            
            # 学習モードで次のテーブルがある場合はトーストを表示しない
            if learning_mode and current_index < total_tables:
                pass  # 学習継続中はトーストなし
            elif learning_mode:
                st.toast("📊 全テーブルの学習データ収集が完了しました！今後の予想精度が向上します。", icon="📈")
            else:
                st.toast("✅ データ更新が正常に完了しました！", icon="🎉")
            st.session_state.show_success_toast = False

        if st.session_state.batch_output:
            st.sidebar.text_area("バッチファイルの出力", st.session_state.batch_output, height=200)
    elif st.session_state.batch_status == "エラー":
        st.sidebar.error("❌ データソースの更新中にエラーが発生しました。")
        # エラートースト表示（一度だけ）
        if st.session_state.get('show_error_toast', False):
            st.toast("❌ データ更新中にエラーが発生しました。", icon="🚨")
            st.session_state.show_error_toast = False
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

    # バッチ実行状態をサイドバーに表示
    current_status = st.session_state.get('batch_status', '待機')
    if current_status == "実行中":
        st.sidebar.info(f"🔄 バックグラウンド実行中")
        if 'batch_start_time' in st.session_state:
            elapsed = time.time() - st.session_state.batch_start_time
            st.sidebar.info(f"⏱️ 実行時間: {int(elapsed//60)}分{int(elapsed%60)}秒")
    elif current_status == "完了":
        st.sidebar.success("✅ 更新完了")
    elif current_status == "エラー":
        st.sidebar.error("❌ エラー発生")
    
    # API制限解除ボタンを追加
    if st.sidebar.button("🔧 APIキャッシュクリア", help="Google Sheets API制限エラー時に使用"):
        try:
            st.cache_data.clear()
            st.cache_resource.clear()
            st.sidebar.success("APIキャッシュをクリアしました。5分待ってから再試行してください。")
            LOGGER.info("APIキャッシュがクリアされました")
        except Exception as e:
            st.sidebar.error(f"キャッシュクリア中にエラーが発生しました: {e}")
            LOGGER.error(f"キャッシュクリア中にエラーが発生しました: {e}")

    # フィルタキャッシュクリアボタンを追加
    if st.sidebar.button("フィルタ設定リセット"):
        try:
            # すべてのキャッシュをクリア
            from core.streamlit.subcode_streamlit_loader import load_sheet_from_spreadsheet
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
        
        # 最新更新時間の表示
        try:
            from src.utils.data_processing import get_parquet_file_last_modified
            import configparser
            
            # 設定ファイル読み込み
            config = configparser.ConfigParser()
            config.read('config/settings.ini', encoding='utf-8')
            nas_base_path = config['Paths']['csv_base_path']
            
            # 選択されたテーブルのParquetファイルパス
            sql_file_name = st.session_state['sql_files_dict'].get(selected_child, '').replace('.sql', '')
            parquet_file_path = f"{nas_base_path}/{sql_file_name}.parquet"
            
            # 最新更新時間を取得
            last_modified = get_parquet_file_last_modified(parquet_file_path)
            
            if last_modified:
                # サイドバーに最新更新時間を表示
                st.sidebar.markdown("---")
                st.sidebar.markdown("### 📅 データ情報")
                st.sidebar.info(f"**最終更新**: {last_modified}")
                
                # データの新鮮度を色で表現
                from datetime import datetime, timedelta
                try:
                    last_modified_dt = datetime.strptime(last_modified, "%Y-%m-%d %H:%M:%S")
                    now = datetime.now()
                    time_diff = now - last_modified_dt
                    
                    if time_diff < timedelta(hours=1):
                        freshness_color = "🟢"
                        freshness_text = "データは最新です"
                    elif time_diff < timedelta(hours=24):
                        freshness_color = "🟡"
                        freshness_text = "データは比較的新しいです"
                    else:
                        freshness_color = "🔴"
                        freshness_text = "データが古い可能性があります"
                    
                    st.sidebar.markdown(f"{freshness_color} {freshness_text}")
                    
                except ValueError:
                    pass  # 日時パースエラーは無視
            else:
                st.sidebar.markdown("---")
                st.sidebar.warning("⚠️ ファイルの更新時間を取得できませんでした")
                
        except Exception as e:
            LOGGER.warning(f"最新更新時間の取得に失敗: {e}")
        
        csv_download(selected_child)
        LOGGER.info("CSV download function call completed")
    except Exception as e:
        st.error(f"CSVダウンロード中にエラーが発生しました: {e}")
        LOGGER.error(f"CSVダウンロード中にエラーが発生しました: {e}")

# ダッシュボード機能は削除されました
