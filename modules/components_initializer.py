from modules.measure_registry import MeasureRegistry
from modules.data_source import DataSource
from modules.filter_definition import FilterManager, FilterRegistry
from modules.date_range_selector import DateRangeSelector
from modules.data_processor import DataProcessor
from modules.config_manager import ConfigManager

class ComponentsInitializer:
    def __init__(self, config):
        self.config = config

    def initialize_components(self):
        measure_registry = MeasureRegistry()
        config_manager = ConfigManager(
            date_column=self.config['date_column'],
            measures=self.config['measures'],
            group_column={self.config['group_column']['name']: self.config['group_column']['display_name']},
            table_name=self.config['table_name'],
            measure_registry=measure_registry
        )
        data_source = DataSource(self.config['data_source']['file_path'], 
                                 date_columns=self.config['data_source']['date_columns'])
        filter_manager = FilterManager()
        for filter_def in FilterRegistry.get_all_filters():
            filter_manager.add_filter(filter_def)
        date_range_selector = DateRangeSelector()
        data_processor = DataProcessor(data_source, config_manager)

        return {
            'measure_registry': measure_registry,
            'config_manager': config_manager,
            'data_source': data_source,
            'filter_manager': filter_manager,
            'date_range_selector': date_range_selector,
            'data_processor': data_processor
        }