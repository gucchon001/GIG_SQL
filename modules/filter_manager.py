import pandas as pd
import logging

# ロギングの設定は通常、アプリケーションのエントリーポイントまたは設定ファイルで一度だけ設定されます。
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class FilterManager:
    def __init__(self):
        self.filters = {}
        logging.debug("FilterManager initialized with empty filters.")

    def add_filter(self, key, value):
        self.filters[key] = value
        logging.debug(f"Filter added: {key} = {value}")

    def get_filters(self):
        logging.debug(f"Current filters: {self.filters}")
        return self.filters

    def apply_filters(self, df, filters):
        logging.debug("Applying filters...")
        for column, value in filters.items():
            if isinstance(value, tuple) and len(value) == 2:
                start_date, end_date = pd.to_datetime(value[0]), pd.to_datetime(value[1])
                # 終了日の23:59:59まで含めるように調整
                end_date = end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                logging.debug(f"Filtering {column} from {start_date} to {end_date}")
                before_filter = df.shape[0]
                df = df[(df[column] >= start_date) & (df[column] <= end_date)]
                after_filter = df.shape[0]
                logging.debug(f"Filtered {column}: {before_filter} -> {after_filter}")
            else:
                logging.debug(f"Filtering {column} for exact match of {value}")
                before_filter = df.shape[0]
                df = df[df[column] == value]
                after_filter = df.shape[0]
                logging.debug(f"Filtered {column}: {before_filter} -> {after_filter}")
        return df
