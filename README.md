# GIG_SQL
更新日：2024/08/02
1. 夜間バッチ処理
・1日1回（夜間）にCSVファイルの生成・スプレッドシートへの書き込みを行う
・トモノカイ共有PC（Windows10 Home）のタスクスケジューラで、run.bat を起動し、main.pyを実行
・ADMIN_CSV_DLツール_v1.0 - 実行対象ファイル：実行シート（https://docs.google.com/spreadsheets/d/1HqdbZ5owG2YIOP8M5kc7DOjk34BneEFYkXsJWreJU7Q/edit?pli=1&gid=2146321019#gid=2146321019 ）の”実行対象”列にチェックがついている”SQLファイル名”にあるSQLを呼び出す
・H列～P列の条件を、SQL文に修正を加える
・修正を加えたSQL文を実行し、GIG_DBにSSHトンネルからアクセスしてデータを抽出し、CSV or スプシに保存
・関連ファイル
　main.py　実行ファイル
　common_exe_functions.py　メインコード
　ssh_connection.py　SSHの接続
　database_connection.py　MYSQLの接続
　subcode_loader.py　サブコード（SQLの修正やCSV処理等）
　※main_test.py 手動実行用（実行シートのテスト実行列にチェックがついているファイルを実行 / デバッグ・エラー時の手動実行で利用）

2. CSVダウンロードツール　ストミンくん
・PythonWEBアプリ フレームワーク'Streamlit'を使って、トモノカイローカルネットワーク内で、各テーブルのCSVダウンロードを絞込して行う
・トモノカイ共有PC（Windows10 Home）で、streamiltを常時立ち上げる
・30分に1回、Parquetファイルの生成をして、それをstreamlitで呼び出してWebブラウザ上で表示（パフォーマンスの問題でSQLを毎回実行するのではなく、ローカルファイルを呼び出している）
　※ADMIN_CSV_DLツール_v1.0 にあるシート名の各シートがテーブルとなっていて、そこで型指定を行っている
・ADMIN_CSV_DLツール_v1.0 - 実行対象ファイル：個別実行シート（https://docs.google.com/spreadsheets/d/1HqdbZ5owG2YIOP8M5kc7DOjk34BneEFYkXsJWreJU7Q/edit?pli=1&gid=261132441#gid=261132441 ）の”個別リスト”列にチェックがついている”SQLファイル名”にあるSQLを呼び出す
・H列～J列の条件を、SQL文に修正を加える
・Webブラウザ上で呼び出されたファイル名がサイドバーに表示され、各テーブルごとに画面切り替えを行い、テーブル表示・絞込・CSVダウンロードを行える
・ADMIN_CSV_DLツール_v1.0 にあるシート名の各シートのB列にチェックを入れると絞込検索のフィールドが表示され、C列：入力方式／D列：選択項目の表示を行う
・修正を加えたSQL文を実行し、GIG_DBにSSHトンネルからアクセスしてデータを抽出し、ParquetファイルをローカルPCに保存
・関連ファイル
　- Parquetファイルのデータ生成
　　run_create_datesets.py　実行ファイル
　　common_create_datasets.py　メインコード
　　db_utils.py　SSHとDB接続
　　subcode_loader　サブコード（SQLの修正やCSV処理等）
　　subcode_streamlit_loader.py　streamlit用サブコード（Parquetファイル生成・streamlitの調整）
　・Streamlitの実行
　　streamlit_app.py　実行ファイル
　　csv_download.py　CSVファイルの生成処理
　　subcode_streamlit_loader.py　streamlit用サブコード
　　utils.py　Streamlit用サブコードで主に画面制御系

4. BIツール（開発中）
 ダッシュボード
