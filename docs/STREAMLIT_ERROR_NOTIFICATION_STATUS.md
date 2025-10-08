# 📊 Streamlitアプリのエラー通知状況

## 📋 現在の状況

### ✅ **実装済み**

1. **エラー通知モジュール**
   - ファイル: `src/streamlit_system/utils/error_notifier.py`
   - 機能:
     - `notify_critical_error()`: 重大なエラー通知
     - `notify_data_update_error()`: データ更新エラー通知
     - `notify_batch_execution_error()`: バッチ実行エラー通知
     - `notify_warning()`: 警告通知

2. **基本的なログ記録**
   - Streamlitアプリは`logs/streamlit.log`にエラーを記録
   - ログレベル: DEBUG, INFO, WARNING, ERROR

### ⏳ **部分的実装**

**Streamlitアプリ本体へ の統合**:
- インポート部分は追加済み
- 個別のエラーハンドリング箇所への組み込みは**手動で必要**

---

## 🔧 手動でSlack通知を追加する方法

Streamlitアプリのエラー箇所に以下のコードを追加：

### パターン1: バッチ実行エラー
```python
except subprocess.CalledProcessError as e:
    LOGGER.error(f"バッチファイル実行中にエラーが発生しました: {e.stderr}")
    st.session_state.batch_status = "エラー"
    st.session_state.batch_output = e.stderr
    
    # Slack通知を追加 ⬇️
    if ERROR_NOTIFICATION_AVAILABLE:
        try:
            notify_batch_execution_error("データ更新（全件）", e)
        except:
            pass
```

### パターン2: 一般的なエラー
```python
except Exception as e:
    LOGGER.error(f"エラーが発生しました: {e}")
    
    # Slack通知を追加 ⬇️
    if ERROR_NOTIFICATION_AVAILABLE:
        try:
            notify_critical_error(e, {"処理": "データ表示"})
        except:
            pass
```

### パターン3: データ更新エラー
```python
except Exception as e:
    LOGGER.error(f"テーブル更新エラー: {e}")
    
    # Slack通知を追加 ⬇️
    if ERROR_NOTIFICATION_AVAILABLE:
        try:
            notify_data_update_error(table_name, e)
        except:
            pass
```

---

## 🎯 推奨される実装箇所

### 優先度：高 🔴

1. **データ更新（全件）エラー**
   - ファイル: `streamlit_app.py`
   - 行: 約150行目、520行目
   - エラータイプ: `subprocess.CalledProcessError`

2. **データ更新（個別）エラー**
   - ファイル: `streamlit_app.py`
   - 行: 約680行目
   - エラータイプ: 各種Exception

3. **データベース接続エラー**
   - ファイル: `core/streamlit/csv_download.py`
   - 関数: `csv_download()`

### 優先度：中 🟡

4. **ファイル読み込みエラー**
   - ファイル: `core/streamlit/subcode_streamlit_loader.py`
   - 関数: `load_and_filter_parquet()`

5. **Google Sheets APIエラー**
   - ファイル: `core/streamlit/subcode_streamlit_loader.py`
   - 関数: `load_sql_list_from_spreadsheet()`

---

## 🚀 クイックスタート: Streamlitエラー通知を有効化

### ステップ1: テスト送信
```powershell
# 通知モジュールのテスト
python -c "from src.streamlit_system.utils.error_notifier import notify_warning; notify_warning('Streamlit通知テスト')"
```

### ステップ2: Streamlitアプリを起動
```powershell
.\scripts\powershell\run_streamlit_new.ps1
```

### ステップ3: エラーログを監視
```powershell
# Streamlitログのエラーを監視
Get-Content logs\streamlit.log -Wait | Select-String "ERROR"
```

---

## 📊 現在の通知カバレッジ

| コンポーネント | エラー通知 | 状態 |
|---|---|---|
| **create_datasets.ps1** | ✅ | 完全実装 |
| **common_exe_functions.py** | ✅ | 完全実装 |
| **Streamlitアプリ** | ⚠️ | 部分的（モジュールのみ） |
| **CSV Download** | ❌ | 未実装 |
| **Google Sheets Loader** | ❌ | 未実装 |

---

## 💡 代替案: ログベースの監視

Streamlitアプリに直接通知を組み込まない場合でも、ログ監視で対応可能：

### 方法1: 定期的なログ監視スクリプト
```powershell
# タスクスケジューラで1時間ごとに実行
# Streamlitログのエラーをチェック
$errors = Get-Content logs\streamlit.log | Select-String "ERROR" -Context 2
if ($errors) {
    python scripts\python\notify_error.py
}
```

### 方法2: リアルタイム監視
```powershell
# PowerShellでリアルタイムエラー監視
Get-Content logs\streamlit.log -Wait | Where-Object {$_ -match "ERROR"} | ForEach-Object {
    Write-Host "⚠️ エラー検出: $_" -ForegroundColor Red
    # 必要に応じてSlack通知
}
```

---

## 🔍 エラー通知のベストプラクティス

### 通知すべきエラー ✅
- データベース接続エラー
- バッチ実行失敗
- ファイル読み込みエラー
- Google Sheets APIエラー
- システムクラッシュ

### 通知不要なエラー ❌
- ユーザー入力の検証エラー
- 軽微なWarning
- キャッシュミス
- 一時的なネットワーク遅延（リトライで解決）

---

## 📞 今後の改善提案

### 短期（1週間以内）
1. ✅ エラー通知モジュール作成 - **完了**
2. ⏳ 重要なエラー箇所への組み込み - **手動作業が必要**
3. ⏳ テスト送信で動作確認

### 中期（1ヶ月以内）
1. 全エラー箇所への組み込み完了
2. エラー頻度の統計取得
3. 通知の最適化（重複排除、頻度制限）

### 長期（3ヶ月以内）
1. エラーダッシュボードの作成
2. 自動復旧機能の追加
3. 予測的エラー検知

---

## 📚 関連ドキュメント

- **エラー通知モジュール**: `src/streamlit_system/utils/error_notifier.py`
- **Slack通知ガイド**: `docs/SLACK_NOTIFICATION_GUIDE.md`
- **エラー対応ガイド**: `docs/ERROR_HANDLING_GUIDE.md`

---

**最終更新**: 2025年10月8日
**ステータス**: エラー通知モジュール実装完了、Streamlitアプリへの統合は部分的

