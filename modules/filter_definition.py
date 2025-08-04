from typing import List, Dict, Any, Optional
from enum import Enum

class FilterType(Enum):
    RADIO = "radio"
    DROPDOWN = "dropdown"
    CHECKBOX = "checkbox"

class FilterDefinition:
    def __init__(self, name: str, column: str, display_name: str, options: List[str] = None, master_table: str = None, option_column: str = None):
        self.name = name
        self.column = column
        self.display_name = display_name
        self.options = options
        self.master_table = master_table
        self.option_column = option_column

class FilterManager:
    def __init__(self):
        self.filters: Dict[str, FilterDefinition] = {}
        self.selected_values: Dict[str, Any] = {}

    def add_filter(self, filter_def: FilterDefinition):
        self.filters[filter_def.name] = filter_def

    def get_filter(self, name: str) -> FilterDefinition:
        return self.filters.get(name)

    def set_selected_value(self, name: str, value: Any):
        self.selected_values[name] = value

    def get_selected_value(self, name: str) -> Any:
        return self.selected_values.get(name)

class FilterRegistry:
    @staticmethod
    def get_site_type_filter():
        return FilterDefinition(
            name="site_type",
            column="開始列",
            display_name="サイト種別",
            options=["ステーション", "キャリア"]
        )

    @staticmethod
    def get_all_filters():
        return [
            FilterRegistry.get_site_type_filter(),
            # 他のフィルターがあればここに追加
        ]