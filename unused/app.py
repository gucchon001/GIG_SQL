"""
ストミンくん - Streamlit WebUI

塾ステ CSVダウンロードツールのWebインターフェース
"""
import streamlit as st
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Streamlit設定は最初に行う
st.set_page_config(
    page_title="塾ステ CSVダウンロードツール ストミンくん β版",
    page_icon=":bar_chart:",
    layout="wide"
)

import subprocess
import configparser
import threading
from src.core.config.settings import AppConfig
from src.core.logging.logger import get_logger
from src.streamlit_system.ui.components import StreamlitUI
from src.streamlit_system.data_sources.csv_downloader import CSVDownloader
from src.streamlit_system.data_sources.sql_loader import SQLLoader

# 新構造の統合モジュールを使用
from src.streamlit_system.ui.session_manager import (
    initialize_session_state, create_dynamic_input_fields, on_sql_file_change
)
from src.streamlit_system.ui.styles import load_css, apply_sidebar_styles
from src.utils.data_processing import load_and_filter_parquet, format_dates

# 元の本番環境で動作していた構造に戻す
from csv_download import csv_download

# ロガーの設定
logger = get_logger(__name__)


class StreamlitApp:
    """Streamlit アプリケーションクラス"""
    
    def __init__(self):
        """アプリケーションを初期化"""
        try:
            self.config = AppConfig.from_config_file("config.ini")
            self.ui = StreamlitUI()
            self.csv_downloader = CSVDownloader(self.config)
            self.sql_loader = SQLLoader(self.config)
            self._initialize_session_state()
            
        except Exception as e:
            logger.error(f"アプリケーション初期化エラー: {e}")
            st.error("アプリケーションの初期化に失敗しました")
    
    def _initialize_session_state(self) -> None:
        """セッション状態を初期化（新構造の統合モジュール使用）"""
        # 統合されたセッション管理を使用
        initialize_session_state()
        
        # SQLファイルリスト用の追加初期化
        if 'sql_files_dict' not in st.session_state:
            try:
                # 本番環境で動作していた関数を使用
                from subcode_streamlit_loader import load_sql_list_from_spreadsheet
                sql_files_dict = load_sql_list_from_spreadsheet()
                st.session_state.sql_files_dict = sql_files_dict
                logger.info(f"SQLファイル辞書読み込み成功: {len(sql_files_dict)}個")
            except Exception as e:
                logger.error(f"SQLファイル辞書読み込みエラー: {e}")
                st.session_state.sql_files_dict = {}
        
        if 'selected_child' not in st.session_state:
            st.session_state.selected_child = None
    
    def run(self) -> None:
        """メインアプリケーションを実行"""
        try:
            # サイドバーUI
            self.ui.render_sidebar()
            
            # メインメニューの選択
            selected_parent = st.sidebar.radio(
                "メインメニュー",
                ["CSVダウンロード"],
                index=0,
                key="parent_radio"
            )
            
            if selected_parent == "CSVダウンロード":
                self._render_sidebar_menu()
                self._render_csv_download_page()
                
        except Exception as e:
            logger.error(f"アプリケーション実行エラー: {e}")
            st.error("アプリケーションの実行中にエラーが発生しました")
    
    def _render_csv_download_page(self) -> None:
        """CSVダウンロードページをレンダリング"""
        st.title("📊 塾ステ CSVダウンロードツール")
        st.markdown("---")
        
        # データ作成セクション
        with st.container():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.subheader("🔄 データ作成")
                st.write("Parquetファイルを生成して最新データでダッシュボードを更新します")
            
            with col2:
                if st.button("データ作成実行", type="primary", key="create_data"):
                    self._execute_data_creation()
        
        st.markdown("---")
        
        # SQLクエリ選択セクション
        self._render_sql_selection()
        
        # SQLクエリ選択に基づくCSV表示
        if st.session_state.get('selected_child'):
            st.markdown("---")
            try:
                logger.info(f"Calling csv_download function with {st.session_state.selected_child}")
                csv_download(st.session_state.selected_child)
                logger.info("CSV download function call completed")
            except Exception as e:
                st.error(f"CSVダウンロード中にエラーが発生しました: {e}")
                logger.error(f"CSVダウンロード中にエラーが発生しました: {e}")
        
        # データ表示セクション
        if st.session_state.selected_data is not None:
            self._render_data_display()
    
    def _render_sidebar_menu(self) -> None:
        """旧構造のサイドバーメニューをレンダリング"""
        st.sidebar.markdown("---")

        # データ更新ボタン（全件）
        if st.sidebar.button("データ更新（全件）"):
            if st.session_state.batch_status == "実行中":
                st.sidebar.warning("現在データ更新が実行中です。")
            else:
                st.session_state.batch_status = "実行中"
                st.session_state.batch_output = ""
                
                # config.ini を読み込む
                import configparser
                config = configparser.ConfigParser()
                config.read('config.ini', encoding='utf-8')
                batch_file_path = config['batch_exe']['create_datasets']
                
                if not os.path.exists(batch_file_path):
                    st.session_state.batch_status = "エラー"
                    st.session_state.batch_output = f"バッチファイルが見つかりません: {batch_file_path}"
                    logger.error(f"バッチファイルが見つかりません: {batch_file_path}")
                    st.toast(f"バッチファイルが見つかりません: {batch_file_path}", icon="❌")
                else:
                    import threading
                    thread = threading.Thread(target=self._run_batch_file, args=(batch_file_path,), daemon=True)
                    thread.start()
                    logger.info("バッチファイルをバックグラウンドで実行しました。")
                    st.toast("データソースの更新を開始しました。", icon="⏳")

        # データ更新ボタン（個別）
        if st.sidebar.button("データ更新（個別）"):
            if st.session_state.batch_status == "実行中":
                st.sidebar.warning("現在データ更新が実行中です。")
            else:
                selected_table = st.session_state.get('selected_child', None)
                if selected_table:
                    st.session_state.batch_status = "実行中"
                    st.session_state.batch_output = ""
                    
                    import configparser
                    config = configparser.ConfigParser()
                    config.read('config.ini', encoding='utf-8')
                    batch_file_path = config['batch_exe']['create_datasets_individual']
                    
                    if not os.path.exists(batch_file_path):
                        st.session_state.batch_status = "エラー"
                        st.session_state.batch_output = f"バッチファイルが見つかりません: {batch_file_path}"
                        logger.error(f"バッチファイルが見つかりません: {batch_file_path}")
                        st.toast(f"バッチファイルが見つかりません: {batch_file_path}", icon="❌")
                    else:
                        import threading
                        thread = threading.Thread(target=self._run_batch_file, args=(batch_file_path, selected_table), daemon=True)
                        thread.start()
                        logger.info(f"バッチファイルをバックグラウンドで実行しました。 テーブル: {selected_table}")
                        st.toast(f"データソースの更新を開始しました（テーブル: {selected_table}）。", icon="⏳")
                else:
                    st.sidebar.error("テーブルが選択されていません。")

        # SQLファイル一覧の取得と表示
        sql_files_dict = st.session_state.sql_files_dict
        
        if sql_files_dict and len(sql_files_dict) > 0:
            sql_file_display_names = list(sql_files_dict.keys())
            logger.debug(f"SQLファイル表示名一覧: {sql_file_display_names}")
            
            selected_child = st.sidebar.radio("サブメニュー", sql_file_display_names, key="child_radio")
            st.session_state.selected_child = selected_child
            
            # 選択されたファイルの情報をログ出力
            if selected_child in sql_files_dict:
                sql_file_name = sql_files_dict[selected_child]
                logger.info(f"選択されたテーブル: {selected_child} -> SQLファイル: {sql_file_name}")
        else:
            st.sidebar.warning("SQLファイル一覧の読み込みに失敗しました")
            st.sidebar.info("「リスト再読み込み」ボタンを試してください")
            # デフォルト値は設定しない
            st.session_state.selected_child = None

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

        # リスト再読み込みボタン
        st.sidebar.markdown("---")
        if st.sidebar.button("リスト再読み込み"):
            try:
                # 本番環境で動作していた関数を使用
                from subcode_streamlit_loader import load_sql_list_from_spreadsheet
                
                # キャッシュクリア（関数にclearメソッドがある場合）
                if hasattr(load_sql_list_from_spreadsheet, 'clear'):
                    load_sql_list_from_spreadsheet.clear()
                
                # SQLファイル辞書を再読み込み
                sql_files_dict = load_sql_list_from_spreadsheet()
                st.session_state.sql_files_dict = sql_files_dict
                
                logger.info(f"SQLファイル辞書再読み込み成功: {len(sql_files_dict)}個")
                st.sidebar.success(f"リスト再読み込み完了: {len(sql_files_dict)}個のテーブル")
                st.rerun()
                
            except Exception as e:
                st.sidebar.error("リストの再読み込み中にエラーが発生しました。")
                st.sidebar.write(f"エラー詳細: {e}")
                logger.error(f"リスト再読み込み中にエラーが発生しました: {e}")

    def _run_batch_file(self, batch_file_path: str, table_name: str = None) -> None:
        """バッチファイルを実行するヘルパー関数"""
        try:
            import subprocess
            if table_name:
                logger.info(f"バッチファイルを実行開始: {batch_file_path} テーブル: {table_name}")
                result = subprocess.run(
                    [batch_file_path, table_name],
                    check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8'
                )
            else:
                logger.info(f"バッチファイルを実行開始: {batch_file_path}")
                result = subprocess.run(
                    batch_file_path,
                    check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8'
                )
            logger.info(f"バッチファイルの出力: {result.stdout}")
            st.session_state.batch_status = "完了"
            st.session_state.batch_output = result.stdout
            st.toast("データソースの更新が完了しました。", icon="✅")
        except subprocess.CalledProcessError as e:
            logger.error(f"バッチファイル実行中にエラーが発生しました: {e.stderr}")
            st.session_state.batch_status = "エラー"
            st.session_state.batch_output = e.stderr
            st.toast("データソースの更新中にエラーが発生しました。", icon="❌")
        except Exception as e:
            logger.error(f"最新更新ボタンの実行中にエラーが発生しました: {e}")
            st.session_state.batch_status = "エラー"
            st.session_state.batch_output = str(e)
            st.toast("予期せぬエラーが発生しました。", icon="⚠️")
    
    def _execute_data_creation(self) -> None:
        """データ作成を実行"""
        try:
            with st.spinner("データ作成中..."):
                st.session_state.batch_status = "実行中"
                
                # データ作成処理を実行
                success = self._run_data_creation_process()
                
                if success:
                    st.session_state.batch_status = "完了"
                    st.success("データ作成が完了しました")
                else:
                    st.session_state.batch_status = "エラー"
                    st.error("データ作成中にエラーが発生しました")
                    
        except Exception as e:
            logger.error(f"データ作成実行エラー: {e}")
            st.session_state.batch_status = "エラー"
            st.error("データ作成の実行に失敗しました")
    
    def _run_data_creation_process(self) -> bool:
        """データ作成プロセスを実行"""
        try:
            # TODO: データ作成ロジックを実装
            # 現在は旧システムのバッチファイル実行を呼び出し
            result = subprocess.run(
                ["python", "run_create_datesets.py"],
                capture_output=True,
                text=True,
                timeout=300  # 5分のタイムアウト
            )
            
            if result.returncode == 0:
                logger.info("データ作成プロセスが正常に完了")
                return True
            else:
                logger.error(f"データ作成プロセスエラー: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("データ作成プロセスがタイムアウト")
            return False
        except Exception as e:
            logger.error(f"データ作成プロセス実行エラー: {e}")
            return False
    
    def _render_sql_selection(self) -> None:
        """SQLクエリ選択UIをレンダリング"""
        st.subheader("📋 SQLクエリ選択")
        
        try:
            # SQLファイルリストを取得
            sql_files = self.sql_loader.get_sql_file_list()
            
            if sql_files:
                # セレクトボックスでSQLファイルを選択
                selected_file = st.selectbox(
                    "実行するSQLクエリを選択してください",
                    options=sql_files,
                    key="sql_file_selector"
                )
                
                col1, col2 = st.columns([1, 4])
                
                with col1:
                    if st.button("データ取得", type="secondary"):
                        self._load_selected_data(selected_file)
                
                with col2:
                    if st.button("CSV ダウンロード", type="primary"):
                        self._download_csv(selected_file)
            else:
                st.warning("利用可能なSQLファイルがありません")
                
        except Exception as e:
            logger.error(f"SQL選択UI表示エラー: {e}")
            st.error("SQLファイルリストの取得に失敗しました")
    
    def _load_selected_data(self, sql_file: str) -> None:
        """選択されたSQLでデータを取得"""
        try:
            with st.spinner("データを取得中..."):
                data = self.sql_loader.execute_sql_file(sql_file)
                if data is not None and not data.empty:
                    st.session_state.selected_data = data
                    st.success(f"{len(data)} 行のデータを取得しました")
                else:
                    st.warning("データが見つかりませんでした")
                    
        except Exception as e:
            logger.error(f"データ取得エラー: {e}")
            st.error("データの取得に失敗しました")
    
    def _download_csv(self, sql_file: str) -> None:
        """CSVダウンロードを実行"""
        try:
            with st.spinner("CSVを生成中..."):
                csv_data = self.csv_downloader.generate_csv(sql_file)
                if csv_data:
                    st.download_button(
                        label="📥 CSVファイルをダウンロード",
                        data=csv_data,
                        file_name=f"{sql_file.replace('.sql', '')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("CSVの生成に失敗しました")
                    
        except Exception as e:
            logger.error(f"CSVダウンロードエラー: {e}")
            st.error("CSVダウンロードに失敗しました")
    
    def _render_data_display(self) -> None:
        """データ表示UIをレンダリング"""
        data = st.session_state.selected_data
        
        st.subheader("📊 データ表示")
        
        # データ統計
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("行数", len(data))
        
        with col2:
            st.metric("列数", len(data.columns))
        
        with col3:
            st.metric("メモリ使用量", f"{data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # データテーブル表示
        st.dataframe(
            data,
            use_container_width=True,
            height=400
        )
        
        # データ型情報
        with st.expander("📋 データ型情報"):
            dtype_info = pd.DataFrame({
                'カラム名': data.columns,
                'データ型': [str(dtype) for dtype in data.dtypes],
                'Non-Null数': [data[col].count() for col in data.columns],
                'Null数': [data[col].isnull().sum() for col in data.columns]
            })
            st.dataframe(dtype_info, use_container_width=True)


def main():
    """メイン関数"""
    app = StreamlitApp()
    app.run()


if __name__ == "__main__":
    main()