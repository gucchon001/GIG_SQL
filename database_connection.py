import mysql.connector
import traceback

def create_database_connection(db_config, local_bind_port):
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port=local_bind_port,
            user=db_config['user'],
            passwd=db_config['password'],
            database=db_config['database'],
            auth_plugin='mysql_native_password',  # 認証プラグインを指定
            connect_timeout=100,  # 接続タイムアウトを100秒に設定
            use_pure=True
        )
        print('データベースに接続しました。')
        return conn
    except mysql.connector.Error as err:
        print(f"データベース接続エラー: {err}")
        return None
    except Exception as e:
        print(f"予期しないエラー: {e}")
        traceback.print_exc()
        return None