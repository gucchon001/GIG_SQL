import streamlit as st
from modules.dashboard_ui import DashboardUI
from modules.data_manager import DataManager
from modules.components_initializer import ComponentsInitializer
import logging

logger = logging.getLogger(__name__)

class Dashboard:
    def __init__(self, config):
        logger.debug("Initializing Dashboard with config: %s", config)
        self.config = config
        self.components = ComponentsInitializer(config).initialize_components()
        self.ui = DashboardUI(self)
        self.data_manager = DataManager(self.components, self.config)

    def run(self):
        logger.debug("Running Dashboard")
        st.title(self.config['title'])
        try:
            logger.debug("Calling render_main_menu")
            filters, period, show_previous_year = self.ui.render_main_menu()
            logger.debug("Filters: %s, Period: %s, Show Previous Year: %s", filters, period, show_previous_year)
            
            # Add this line
            self.data_manager.period = period
            
        except AttributeError as e:
            logger.error("AttributeError in render_main_menu: %s", e)
            st.error("An error occurred while rendering the main menu. Please check the logs for more details.")
            return
        except Exception as e:
            logger.error("Unexpected error in render_main_menu: %s", e)
            st.error("An unexpected error occurred. Please check the logs for more details.")
            return

        current_data = self.data_manager.process_data(filters, period)
        previous_data = self.data_manager.get_previous_year_data(filters, period) if any(show_previous_year.values()) else None
        self.ui.render_data_display(current_data, previous_data)