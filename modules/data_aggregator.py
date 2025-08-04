import logging
from modules.period_manager import PeriodManager
class DataAggregator:
    def aggregate_data(self, df, date_column, group_column, measure_definition, period):
        logging.debug("Starting data aggregation...")
        freq = PeriodManager.get_frequency(period)
        logging.debug(f"Aggregating data with frequency: {freq}")

        grouping_func = PeriodManager.get_grouping_function(period, date_column)
        grouped = df.groupby([grouping_func, group_column])
        
        aggregated = grouped.apply(measure_definition.calculate).reset_index()
        aggregated.columns = [date_column, group_column, measure_definition.get_name()]
        logging.debug(f"Data aggregated: \n{aggregated.head()}")
        return aggregated