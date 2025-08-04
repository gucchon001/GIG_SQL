import pandas as pd

class DataSource:
    def __init__(self, file_path, date_columns=None, datetime_as_date=False, file_type='parquet'):
        self.file_path = file_path
        self.date_columns = date_columns or []
        self.datetime_as_date = datetime_as_date  # DatetimeをDateとして扱うかどうか
        self.df = None
        self.load_data(file_type)

    def load_data(self, file_type='parquet'):
        if file_type == 'parquet':
            self.df = pd.read_parquet(self.file_path)

        # 日付列の型変換とオプションに応じた処理
        for col in self.date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                if self.datetime_as_date:
                    # Datetime型をDate型に変換
                    self.df[col] = self.df[col].dt.date
        
        return self.df

    def get_column_values(self, column_name):
        if self.df is None:
            raise ValueError("Data not loaded. There might be an issue with the data file.")
        return self.df[column_name].unique().tolist()

    def get_dataframe(self):
        if self.df is None:
            raise ValueError("Data not loaded. There might be an issue with the data file.")
        return self.df
