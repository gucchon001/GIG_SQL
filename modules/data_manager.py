from datetime import timedelta

class DataManager:
    def __init__(self, components, config):
        self.components = components
        self.config = config

    def process_data(self, filters, period):
        return self.components['data_processor'].process_data(filters, period)

    def get_previous_year_data(self, filters, period):
        previous_year_filters = filters.copy()
        start_date, end_date = filters[self.config['date_column']]
        previous_year_filters[self.config['date_column']] = (
            start_date - timedelta(days=365),
            end_date - timedelta(days=365)
        )
        return self.components['data_processor'].process_data(previous_year_filters, period)