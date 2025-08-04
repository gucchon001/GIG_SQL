import streamlit as st
import pandas as pd
from modules.period_manager import PeriodManager
import logging

logger = logging.getLogger(__name__)

class DashboardUI:
    def __init__(self, dashboard):
        self.dashboard = dashboard
        self.config = dashboard.config
        self.components = dashboard.components
        logger.debug("DashboardUI initialized")

    def render_main_menu(self):
        logger.debug("Rendering main menu")
        st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)

        col1, col2 = st.columns([2, 3])
        with col1:
            selected_period = st.radio(
                "集計期間",
                self.config['period_options'],
                index=self.config['period_options'].index(self.config['default_period'])
            )
        logger.debug("Selected period: %s", selected_period)

        with col2:
            start_date, end_date = PeriodManager.get_default_dates(selected_period)
            start_date = st.date_input("開始日", value=start_date)
            end_date = st.date_input("終了日", value=end_date)
        logger.debug("Date range: %s to %s", start_date, end_date)

        if 'site_type' in self.components['filter_manager'].filters:
            site_type_filter = self.components['filter_manager'].filters['site_type']
            selected_site_type = st.radio("サイト種別", site_type_filter.options)
            self.components['filter_manager'].set_selected_value('site_type', selected_site_type)
        logger.debug("Selected site type: %s", selected_site_type if 'selected_site_type' in locals() else "Not set")

        st.markdown("### 前年データ")
        show_previous_year = {}
        cols = st.columns(len(self.config['measures']))
        for i, measure in enumerate(self.config['measures']):
            with cols[i]:
                show_previous_year[measure] = st.checkbox(
                    f"{measure}（前年）",
                    value=False
                )
        logger.debug("Show previous year: %s", show_previous_year)

        filters = {self.config['date_column']: (start_date, end_date)}
        for filter_def in self.components['filter_manager'].filters.values():
            filter_value = self.components['filter_manager'].get_selected_value(filter_def.name)
            if filter_value:
                filters[filter_def.column] = filter_value
        logger.debug("Filters: %s", filters)

        logger.debug("Returning from render_main_menu")
        return filters, selected_period, show_previous_year

    def render_data_display(self, current_data, previous_data):
        logger.debug("Rendering data display")
        st.subheader(self.config['table_name'])

        if current_data is None or all(df.empty for df in current_data.values()):
            logger.warning("No data to display")
            st.write("表示するデータがありません")
            return

        logger.debug("Preparing data for display")
        combined_data = self.prepare_display_data(current_data, previous_data)
        
        logger.debug("Displaying data table")
        st.dataframe(combined_data)
        
        logger.debug("Data display rendered")

    def prepare_display_data(self, current_data, previous_data):
        logger.debug("Preparing display data")
        date_column = self.config['date_column']
        group_column = self.config['group_column']['name']
        
        # 最初の指標のデータフレームをベースとして使用
        base_df = current_data[self.config['measures'][0]]
        combined_data = base_df[[date_column, group_column]].copy()

        for measure in self.config['measures']:
            logger.debug(f"Processing measure: {measure}")
            measure_df = current_data[measure]
            combined_data[measure] = measure_df[measure]

            if previous_data is not None and measure in previous_data:
                previous_year_column = f"{measure}（前年）"
                combined_data[previous_year_column] = previous_data[measure][measure]

        logger.debug("Pivoting data")
        pivoted_data = combined_data.pivot(index=date_column, columns=group_column)
        pivoted_data.columns = [f"{col[1]}_{col[0]}" for col in pivoted_data.columns]
        pivoted_data = pivoted_data.reset_index()

        logger.debug("Display data preparation completed")
        logger.debug(f"Final data shape: {pivoted_data.shape}")
        logger.debug(f"Final data sample:")
        logger.debug(pivoted_data.head())
        
        return pivoted_data