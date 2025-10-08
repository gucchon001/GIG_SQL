"""
エラー通知のテストスクリプト
"""
import sys
import os
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.streamlit_system.utils.error_notifier import (
    notify_critical_error,
    notify_data_update_error,
    notify_batch_execution_error,
    notify_warning
)

def test_notifications():
    """各種通知をテスト"""
    
    print("=" * 60)
    print("エラー通知テスト開始")
    print("=" * 60)
    
    # テスト1: 警告通知
    print("\n【テスト1】警告通知")
    print("-" * 60)
    result = notify_warning(
        "これはテスト警告です。システムは正常に動作しています。",
        context={"テストタイプ": "警告通知テスト"}
    )
    print(f"結果: {'✅ 成功' if result else '❌ 失敗'}")
    
    # テスト2: データ更新エラー通知
    print("\n【テスト2】データ更新エラー通知")
    print("-" * 60)
    try:
        # 意図的にエラーを発生
        raise ValueError("テーブル更新に失敗しました（テスト）")
    except Exception as e:
        result = notify_data_update_error("test_table", e)
        print(f"結果: {'✅ 成功' if result else '❌ 失敗'}")
    
    # テスト3: バッチ実行エラー通知
    print("\n【テスト3】バッチ実行エラー通知")
    print("-" * 60)
    try:
        # 意図的にエラーを発生
        raise RuntimeError("バッチ実行が失敗しました（テスト）")
    except Exception as e:
        result = notify_batch_execution_error("データ更新（全件）テスト", e)
        print(f"結果: {'✅ 成功' if result else '❌ 失敗'}")
    
    # テスト4: 重大なエラー通知
    print("\n【テスト4】重大なエラー通知")
    print("-" * 60)
    try:
        # 意図的にエラーを発生
        raise ConnectionError("データベース接続に失敗しました（テスト）")
    except Exception as e:
        result = notify_critical_error(
            e,
            additional_info={
                "処理": "データ取得",
                "テーブル": "companies",
                "試行回数": "3回"
            }
        )
        print(f"結果: {'✅ 成功' if result else '❌ 失敗'}")
    
    print("\n" + "=" * 60)
    print("エラー通知テスト完了")
    print("=" * 60)
    print("\n💡 Slackチャンネルを確認して、4件の通知が届いているか確認してください。")

if __name__ == '__main__':
    test_notifications()
