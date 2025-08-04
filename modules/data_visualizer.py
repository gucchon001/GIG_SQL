import streamlit as st
import altair as alt
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class DataVisualizer:
    def __init__(self, config_manager):
        self.config_manager = config_manager

    def display_data(self, current_data, previous_year_data=None):
        logging.debug("Displaying data...")
        if current_data.empty:
            st.write("表示するデータがありません")
            return
        merged_data = self.merge_data(current_data, previous_year_data)
        self.display_table(merged_data)
        self.display_chart(merged_data)

    def merge_data(self, current_data, previous_year_data):
        logging.debug("Starting data merge...")
        date_column = self.config_manager.date_column
        group_column = self.config_manager.group_column
        measure_name = self.config_manager.active_measure

        current_data = current_data.rename(columns={measure_name: f'現在{measure_name}'})
        if previous_year_data is not None and not previous_year_data.empty:
            previous_year_data = previous_year_data.rename(columns={measure_name: f'前年{measure_name}'})
            logging.debug("Current data keys for merge: \n" + str(current_data[[date_column, group_column]].drop_duplicates()))
            logging.debug("Previous year data keys for merge: \n" + str(previous_year_data[[date_column, group_column]].drop_duplicates()))

            merged_data = pd.merge(current_data, previous_year_data, on=[date_column, group_column], how='left')
            merged_data[f'前年{measure_name}'] = merged_data[f'前年{measure_name}'].fillna(0)
            merged_data['前年比'] = ((merged_data[f'現在{measure_name}'] - merged_data[f'前年{measure_name}']) /
                                    merged_data[f'前年{measure_name}'].replace(0, 1) * 100).fillna(0)
            logging.debug(f"Merged data: \n{merged_data}")
        else:
            merged_data = current_data
            merged_data[f'前年{measure_name}'] = 0
            merged_data['前年比'] = 100
        return merged_data


    def display_table(self, data):
        st.write(f"{self.config_manager.table_name}の集計結果:")
        st.table(data.set_index([self.config_manager.date_column, self.config_manager.group_column]))

    def display_chart(self, data):
        chart_data = data.melt(
            id_vars=[self.config_manager.date_column, self.config_manager.group_column],
            value_vars=[f'現在{self.config_manager.active_measure}', f'前年{self.config_manager.active_measure}'],
            var_name='年', value_name=self.config_manager.active_measure
        )

        chart = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X(f'{self.config_manager.date_column}:T', axis=alt.Axis(title='日付')),
            y=alt.Y(f'{self.config_manager.active_measure}:Q', title=self.config_manager.active_measure),
            color='年:N',
            column=self.config_manager.group_column,
            tooltip=[f'{self.config_manager.date_column}:T', f'{self.config_manager.active_measure}:Q', '年:N', self.config_manager.group_column]
        ).properties(
            title=f'{self.config_manager.table_name}の集計結果'
        ).interactive()

        st.altair_chart(chart, use_container_width=True)