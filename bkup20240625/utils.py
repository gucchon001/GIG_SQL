import pandas as pd
from io import StringIO

def truncate_text(text, max_length=35):
    if len(str(text)) > max_length:
        return str(text)[:max_length] + "..."
    return text

def apply_styles(df, selected_rows):
    # ヘッダ行とデータ行の文字数を制限し、省略表示にする
    df.columns = [truncate_text(col, 20) for col in df.columns]  # ヘッダ行は20文字まで
    df = df.applymap(lambda x: truncate_text(x, 35) if isinstance(x, str) else x)  # データ行は35文字まで
    
    # スタイル設定
    def highlight_header(s):
        return ['background-color: lightgrey' for _ in s]

    def white_background(val):
        return 'background-color: white'
    
    styled_df = df.head(selected_rows).style.apply(highlight_header, axis=0).applymap(white_background, subset=pd.IndexSlice[:, :])
    
    return styled_df

def load_and_prepare_data(csv_data, selected_rows):
    df = pd.read_csv(StringIO(csv_data), encoding='cp932')
    df = df.head(selected_rows)  # 選択された行数だけ抽出
    styled_df = df.style.set_properties(**{'text-align': 'left'}).set_table_styles([dict(selector='th', props=[('text-align', 'center')])])
    return styled_df