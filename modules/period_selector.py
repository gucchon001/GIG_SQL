import streamlit as st
from modules.period_manager import PeriodManager

class PeriodSelector:
    def __init__(self):
        self.periods = PeriodManager.PERIODS
        self.selected_period = '日別'  # デフォルト値

    def select_period(self):
        cols = st.columns(4)
        for col, period in zip(cols, self.periods):
            with col:
                if st.button(period):
                    self.selected_period = period
        return self.selected_period

    def get_selected_period(self):
        return self.selected_period