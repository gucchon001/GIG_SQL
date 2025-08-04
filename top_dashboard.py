import streamlit as st
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

from modules.data_source import DataSource
from modules.config_manager import ConfigManager
#from modules.data_aggregator import DataAggregator
from modules.measure_registry import MeasureRegistry
from modules.filter_definition import FilterManager, FilterRegistry, FilterType
from modules.date_range_selector import DateRangeSelector
from modules.data_processor import DataProcessor

class Dashboard:
    def __init__(self):
        # メジャーレジストリとコンフィグマネージャーの初期化
        self.measure_registry = MeasureRegistry()
        self.config_manager = ConfigManager(
            date_column='応募日',
            measures=['応募数', '採用数', '採用率'],
            group_column={'開始列': 'サイト種別'},
            table_name='応募者数分析',
            measure_registry=self.measure_registry
        )

        # データファイルのパスを設定
        current_dir = os.getcwd()
        file_path = os.path.join(current_dir, 'data_Parquet', '15_job_candidate_fact.parquet')
        logging.debug(f"Attempting to load data from: {file_path}")
        
        # データソースの初期化
        date_columns = ['応募日']
        self.data_source = DataSource(file_path, date_columns=date_columns)
        
        # データ集計器の初期化
        #self.data_aggregator = DataAggregator()

        # 前年データ表示フラグの初期化
        self.show_previous_year = {
            "応募数": False,
            "採用数": False,
            "採用率": False
        }

        # フィルターマネージャーの初期化
        self.filter_manager = FilterManager()
        for filter_def in FilterRegistry.get_all_filters():
            self.filter_manager.add_filter(filter_def)

        # 日付範囲選択機能の初期化
        self.date_range_selector = DateRangeSelector()

        # データプロセッサーの初期化（data_sourceとconfig_managerの後に初期化）
        self.data_processor = DataProcessor(self.data_source, self.config_manager)

        # 期間オプションの設定
        self.period_options = ['時間別', '日別', '週別', '月別']
        self.selected_period = '日別'  # デフォルト値

    def run(self):
        st.title("データ分析ダッシュボード")
        self.setup_main_menu()
        self.process_and_display_data()

    def setup_main_menu(self):
    # ラジオボタンを横並びにするためのCSS
        st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)

        col1, col2 = st.columns([2, 3])
        with col1:
            self.selected_period = st.radio(
                "集計期間",
                ('時間別', '日別', '週別', '月別'),
                index=self.period_options.index(self.selected_period)
            )

        with col2:
            self.start_date, self.end_date = self.date_range_selector.select_date_range(self.selected_period)

        if 'site_type' in self.filter_manager.filters:
            site_type_filter = self.filter_manager.filters['site_type']
            selected_site_type = st.radio("サイト種別", site_type_filter.options)
            self.filter_manager.set_selected_value('site_type', selected_site_type)

        st.markdown("### 前年データ")
        cols = st.columns(len(self.measure_registry.measures))  # メジャーの数に基づいて列を生成
        for i, measure in enumerate(self.measure_registry.measures.values()):
            with cols[i]:
                self.show_previous_year[measure.name] = st.checkbox(f"{measure.get_display_name()}（前年）", value=self.show_previous_year.get(measure.name, False))


    def process_and_display_data(self):
        filters = {self.config_manager.date_column: (self.start_date, self.end_date)}
        for filter_def in self.filter_manager.filters.values():
            filter_value = self.filter_manager.get_selected_value(filter_def.name)
            if filter_value:
                filters[filter_def.column] = filter_value

        current_year_data = self.data_processor.process_data(filters, self.selected_period)
        
        if any(self.show_previous_year.values()):
            previous_year_filters = filters.copy()
            previous_year_filters[self.config_manager.date_column] = (
                self.start_date - timedelta(days=365),
                self.end_date - timedelta(days=365)
            )
            previous_year_data = self.data_processor.process_data(previous_year_filters, self.selected_period)
        else:
            previous_year_data = None

        self.display_table(current_year_data, previous_year_data)

    def display_table(self, current_year_data, previous_year_data):
        st.subheader(self.config_manager.table_name)

        base_columns = [self.config_manager.date_column, self.config_manager.group_column]
        first_measure = self.config_manager.measures[0]
        combined_data = current_year_data[first_measure][base_columns].copy()

        for measure in self.config_manager.measures:
            measure_definition = self.config_manager.get_measure_definition(measure)
            display_name = measure_definition.get_display_name()
            combined_data[display_name] = current_year_data[measure][measure]
            combined_data[display_name] = combined_data[display_name].apply(measure_definition.format_value)

            if self.show_previous_year[measure] and previous_year_data is not None:
                previous_year_column = f"{measure}\n（前年）"
                combined_data[previous_year_column] = previous_year_data[measure][measure]
                combined_data[previous_year_column] = combined_data[previous_year_column].apply(measure_definition.format_value)

        # 日付列のフォーマットを調整
        combined_data[self.config_manager.date_column] = pd.to_datetime(combined_data[self.config_manager.date_column])
        if self.selected_period == '日別':
            combined_data[self.config_manager.date_column] = combined_data[self.config_manager.date_column].apply(lambda x: x.strftime('%m/%d(%a)'))
        elif self.selected_period == '時間別':
            combined_data[self.config_manager.date_column] = combined_data[self.config_manager.date_column].dt.strftime('%H:00')
        elif self.selected_period == '週別':
            combined_data[self.config_manager.date_column] = combined_data[self.config_manager.date_column].dt.to_period('W').apply(lambda r: r.start_time).dt.strftime('%Y-%m-%d')
        elif self.selected_period == '月別':
            combined_data[self.config_manager.date_column] = combined_data[self.config_manager.date_column].dt.to_period('M').apply(lambda r: r.start_time).dt.strftime('%Y-%m')

        # グループ列でピボット
        pivoted_data = combined_data.pivot(index=self.config_manager.date_column, columns=self.config_manager.group_column)

        # メジャー名のみを表示するためにカラム名を再設定
        # カラム名はタプルの形で('メジャー名', 'グループ名')となっているため、メジャー名のみを取り出す
        pivoted_data.columns = [col[0] for col in pivoted_data.columns]

        # データフレームを転置して表示
        transposed_data = pivoted_data.transpose()  # 縦横転置を実行
        st.dataframe(transposed_data)  # 転置したデータフレームを表示

if __name__ == '__main__':
    dashboard = Dashboard()
    dashboard.run()