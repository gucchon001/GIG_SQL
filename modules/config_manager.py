# config_manager.py
from modules.measure_registry import MeasureRegistry

class ConfigManager:
    def __init__(self, date_column, measures, group_column, table_name, measure_registry):
        self.date_column = date_column
        self.measures = measures
        self.active_measure = measures[0] if measures else None
        self.group_column, self.group_column_display = next(iter(group_column.items()))
        self.table_name = table_name
        self.measure_registry = measure_registry

    def get_measure_definition(self, measure_name=None):
        if measure_name is None:
            measure_name = self.active_measure
        return self.measure_registry.get_measure(measure_name)

    def set_active_measure(self, measure_name):
        if measure_name in self.measures:
            self.active_measure = measure_name
        else:
            raise ValueError(f"Measure '{measure_name}' not in available measures: {self.measures}")

    def to_dict(self):
        return {
            'date_column': self.date_column,
            'measures': self.measures,
            'active_measure': self.active_measure,
            'group_column': self.group_column,
            'group_column_display': self.group_column_display,
            'table_name': self.table_name
        }

    @classmethod
    def from_dict(cls, config_dict, measure_registry):
        return cls(
            date_column=config_dict['date_column'],
            measures=config_dict['measures'],
            group_column={config_dict['group_column']: config_dict['group_column_display']},
            table_name=config_dict['table_name'],
            measure_registry=measure_registry
        )