# 旧構造ファイル バックアップ

このディレクトリには、新構造への移行前の旧ファイルがバックアップとして保管されています。

## 移行完了後のバックアップファイル

### 主要ファイル
- `subcode_loader.py` (1092行) - 新構造の複数モジュールに分散移行済み
- `subcode_streamlit_loader.py` (460行) - 新構造に統合済み  
- `utils.py` (317行) - 新構造に統合済み
- `streamlit_app.py` (192行) - 新構造アプリに統合済み

### 移行先マッピング

| 旧ファイル | 新構造移行先 |
|-----------|-------------|
| `subcode_loader.py` | `src/core/google_api/`, `src/batch_system/sql_utils.py`, `src/utils/export_utils.py` |
| `subcode_streamlit_loader.py` | `src/streamlit_system/ui/`, `src/utils/data_processing.py` |
| `utils.py` | `src/streamlit_system/ui/display_utils.py`, `src/utils/data_processing.py` |
| `streamlit_app.py` | `src/streamlit_system/app.py` |

## 注意事項

- これらのファイルは参照用として保管しています
- 新構造で全機能が動作確認済みです
- 必要に応じて特定機能の参照に使用してください
- 削除前に最終確認のために一時的に保管されています

## 作成日時
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}