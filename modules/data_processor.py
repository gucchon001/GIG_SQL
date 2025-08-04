import logging
import pandas as pd
from modules.period_manager import PeriodManager

class DataProcessor:
    def __init__(self, data_source, config_manager):
        self.data_source = data_source
        self.config_manager = config_manager
        self.period = None
        logging.debug("DataProcessor initialized with configuration.")

    def process_data(self, filters, period):
        logging.debug("Starting data processing...")
        df = self.data_source.get_dataframe()
        logging.debug(f"Initial dataframe loaded with {len(df)} rows.")
        filtered_df = self.apply_filters(df, filters)
        logging.debug(f"Dataframe filtered down to {len(filtered_df)} rows after applying filters.")
        self.period = period
        return self.aggregate_data(filtered_df, period)

    def apply_filters(self, df, filters):
        logging.debug("Applying filters...")
        original_count = len(df)
        for column, value in filters.items():
            if isinstance(value, tuple) and len(value) == 2:
                start_date, end_date = pd.to_datetime(value[0]), pd.to_datetime(value[1])
                end_date = end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                df = df[(df[column] >= start_date) & (df[column] <= end_date)]
            else:
                df = df[df[column] == value]
            logging.debug(f"Filtering on {column} reduced data to {len(df)} rows.")
        logging.debug(f"Applied all filters. Rows reduced from {original_count} to {len(df)}.")
        return df

    class DataProcessor:
        def aggregate_data(self, df, period):
            logging.debug(f"Starting aggregation for period: {period}")
            date_column = self.config_manager.date_column
            group_column = self.config_manager.group_column
            logging.debug(f"Date column: {date_column}, Group column: {group_column}")

            logging.debug(f"DataFrame shape before grouping: {df.shape}")
            logging.debug(f"DataFrame columns: {df.columns}")
            logging.debug(f"Date column dtype: {df[date_column].dtype}")
            logging.debug(f"Sample of date column: {df[date_column].head()}")

            grouping_func = PeriodManager.get_grouping_function(period, date_column)
            logging.debug(f"Grouping function: {grouping_func}")

            try:
                grouped = df.groupby([grouping_func, group_column])
                logging.debug("Successfully grouped data")
            except Exception as e:
                logging.error(f"Error during grouping: {str(e)}")
                raise

    def get_date_range(self, df, period, date_column):
        freq = PeriodManager.get_frequency(period)
        if period == '時間別':
            return PeriodManager.get_full_hour_range(df, date_column)
        elif period == '日別':
            return pd.date_range(df[date_column].min().date(), df[date_column].max().date(), freq=freq)
        elif period == '週別':
            return pd.date_range(df[date_column].min().to_period('W').start_time, 
                                 df[date_column].max().to_period('W').end_time, freq=freq)
        elif period == '月別':
            return pd.date_range(df[date_column].min().to_period('M').start_time, 
                                 df[date_column].max().to_period('M').end_time, freq=freq)
        
    def aggregate_data(self, df, period):
        logging.debug(f"Starting aggregation for period: {period}")
        date_column = self.config_manager.date_column
        group_column = self.config_manager.group_column
        
        logging.debug(f"Date column: {date_column}, Group column: {group_column}")
        logging.debug(f"DataFrame shape: {df.shape}")
        logging.debug(f"DataFrame columns: {df.columns}")
        logging.debug(f"Date column dtype: {df[date_column].dtype}")
        logging.debug(f"Sample of date column: {df[date_column].head()}")
        logging.debug(f"Unique values in group column: {df[group_column].unique()}")

        grouping_func = PeriodManager.get_grouping_function(period, date_column)
        logging.debug(f"Grouping function: {grouping_func}")

        try:
            grouped = df.groupby([grouping_func(df), group_column])
            logging.debug("Successfully grouped data")
            logging.debug(f"Number of groups: {len(grouped)}")
        except Exception as e:
            logging.error(f"Error during grouping: {str(e)}")
            raise

        aggregated_data = {}
        for measure in self.config_manager.measures:
            logging.debug(f"Processing measure: {measure}")
            measure_definition = self.config_manager.get_measure_definition(measure)
            aggregated = grouped.apply(measure_definition.calculate).reset_index()
            aggregated.columns = [date_column, group_column, measure]
            
            logging.debug(f"Aggregated data for {measure} before filling missing periods:")
            logging.debug(aggregated.head())
            logging.debug(f"Shape of aggregated data: {aggregated.shape}")

            date_range = PeriodManager.get_date_range(df[date_column].min(), df[date_column].max(), period)
            aggregated = self.fill_missing_periods(aggregated, date_range, group_column, measure, period)
            
            logging.debug(f"Aggregated data for {measure} after filling missing periods:")
            logging.debug(aggregated.head())
            logging.debug(f"Shape of aggregated data: {aggregated.shape}")

            aggregated_data[measure] = aggregated

        logging.debug("Aggregation completed")
        return aggregated_data

    def fill_missing_periods(self, df, date_range, group_column, measure, period):
        logging.debug(f"Processing data for {measure} without filling missing periods")
        logging.debug(f"Original shape: {df.shape}")
        
        date_column = self.config_manager.date_column
        
        # 日付列をフォーマット
        df[date_column] = df[date_column].apply(lambda x: PeriodManager.format_date(x, period))
        
        logging.debug(f"Shape after processing: {df.shape}")
        logging.debug(f"Sample data after processing:")
        logging.debug(df.head())
        
        return df