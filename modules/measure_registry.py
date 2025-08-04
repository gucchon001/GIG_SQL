# modules/measure_registry.py

from modules.measure_definition import RecordCountMeasure, AdoptionCountMeasure, AdoptionRateMeasure

class MeasureRegistry:
    def __init__(self):
        self.measures = {}
        self.register_default_measures()

    def register_measure(self, measure_class):
        self.measures[measure_class.name] = measure_class()

    def get_measure(self, name):
        measure = self.measures.get(name)
        if measure is None:
            raise ValueError(f"Measure '{name}' not found in registry")
        return measure

    def register_default_measures(self):
        self.register_measure(RecordCountMeasure)
        self.register_measure(AdoptionCountMeasure)
        self.register_measure(AdoptionRateMeasure)

default_registry = MeasureRegistry()