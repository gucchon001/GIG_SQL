# date_range_selector.py
import streamlit as st
from modules.period_manager import PeriodManager
class DateRangeSelector:
    @staticmethod
    def get_default_dates(period):
        return PeriodManager.get_default_dates(period)

    def select_date_range(self, period):
        default_start, default_end = self.get_default_dates(period)
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("開始日", value=default_start)
        with col2:
            end_date = st.date_input("終了日", value=default_end)
        return start_date, end_date