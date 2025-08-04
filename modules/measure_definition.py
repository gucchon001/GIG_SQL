from abc import ABC, abstractmethod

class MeasureDefinition(ABC):
    name = ""
    display_name = ""
    data_type = None
    format_string = "{}"
    previous_year_column_name = ""

    @abstractmethod
    def calculate(self, df):
        pass

    def format_value(self, value):
        return self.format_string.format(self.data_type(value))

    def get_name(self):
        return self.name

    def get_display_name(self):
        return self.display_name if self.display_name else self.name

    def get_previous_year_column_name(self):
        return self.previous_year_column_name if self.previous_year_column_name else f"{self.get_display_name()}（前年）"

class RecordCountMeasure(MeasureDefinition):
    name = "応募数"
    display_name = "応募数"
    data_type = int
    format_string = "{:,.0f}"
    previous_year_column_name = "応募数（前年）"

    def calculate(self, df):
        return df.shape[0]

class AdoptionCountMeasure(MeasureDefinition):
    name = "採用数"
    display_name = "採用数"
    data_type = int
    format_string = "{:,.0f}"
    previous_year_column_name = "採用数（前年）"

    def calculate(self, df):
        return df[df['採用ステータス'] == "採用"].shape[0]

class AdoptionRateMeasure(MeasureDefinition):
    name = "採用率"
    display_name = "採用率"
    data_type = float
    format_string = "{:.1f}%"
    previous_year_column_name = "採用率（前年）"

    def calculate(self, df):
        adoption_count = df[df['採用ステータス'] == "採用"].shape[0]
        total_count = df.shape[0]
        return (adoption_count / total_count) * 100 if total_count > 0 else 0