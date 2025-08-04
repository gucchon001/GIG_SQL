import pandas as pd
import logging

class PreviousYearDataVisualizer:
    def __init__(self, df, config, selected_period):
        self.df = df
        self.config = config
        self.selected_period = selected_period

    def filter_and_aggregate_previous_year_data(self, start_date, end_date):
        previous_start_date, previous_end_date = self.get_previous_year_dates(start_date, end_date)
        logging.debug(f"Previous year date range: {previous_start_date} to {previous_end_date}")
        
        date_column = self.config.date_column
        group_column = self.config.group_column
        
        mask = (self.df[date_column] >= previous_start_date) & (self.df[date_column] <= previous_end_date)
        previous_year_df = self.df.loc[mask]
        
        logging.debug(f"Filtered previous year data:\n{previous_year_df.head()}")  # フィルタされたデータを表示
        
        if not previous_year_df.empty:
            measure_definition = self.config.get_measure_definition()
            
            # 日付列をdatetime型に変換
            previous_year_df[date_column] = pd.to_datetime(previous_year_df[date_column])
            
            aggregated_df = previous_year_df.groupby([previous_year_df[date_column].dt.date, group_column]).apply(measure_definition.calculate).reset_index()
            aggregated_df.columns = [date_column, group_column, self.config.active_measure]
            
            aggregated_df[date_column] = pd.to_datetime(aggregated_df[date_column]) + pd.DateOffset(years=1)
            
            logging.debug(f"Aggregated previous year data:\n{aggregated_df}")  # 集計後のデータを表示
            
            return aggregated_df
        else:
            logging.warning("No previous year data found")
            return pd.DataFrame(columns=[date_column, group_column, self.config.active_measure])

    def get_previous_year_dates(self, start_date, end_date):
        previous_start_date = start_date - pd.DateOffset(years=1)
        previous_end_date = end_date - pd.DateOffset(years=1)
        return previous_start_date, previous_end_date
