from datetime import datetime, timedelta
import pandas as pd
import logging

class PeriodManager:
    # 利用可能な期間のリスト
    PERIODS = ['時間別', '日別', '週別', '月別']
    # 各期間に対応するpandasの頻度文字列
    PERIOD_MAPPING = {'時間別': 'H', '日別': 'D', '週別': 'W', '月別': 'M'}

    @staticmethod
    def get_default_dates(period):
        # 指定された期間に基づいてデフォルトの日付範囲を返す
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        if period == '時間別':
            return today, today  # 時間別の場合、今日の日付を開始と終了に
        elif period == '日別':
            return yesterday - timedelta(days=6), yesterday  # 日別の場合、過去7日間
        elif period == '週別':
            # 週別の場合、過去12週間（直近の週の日曜日から）
            return yesterday - timedelta(days=yesterday.weekday(), weeks=12), yesterday - timedelta(days=yesterday.weekday())
        elif period == '月別':
            # 月別の場合、過去12ヶ月（直近の月の1日から）
            return (yesterday.replace(day=1) - pd.DateOffset(months=11)), yesterday.replace(day=1) - timedelta(days=1)

    @staticmethod
    def get_grouping_function(period, date_column):
        logging.debug(f"Getting grouping function for period: {period}, date_column: {date_column}")
        freq = PeriodManager.PERIOD_MAPPING[period]
        logging.debug(f"Frequency: {freq}")

        def hourly_grouping(df):
            return df[date_column].dt.floor('H')

        def daily_grouping(df):
            return df[date_column].dt.date

        def weekly_grouping(df):
            return df[date_column].dt.to_period('W').apply(lambda r: r.start_time)

        def monthly_grouping(df):
            return df[date_column].dt.to_period('M').apply(lambda r: r.start_time)

        if freq == 'H':
            logging.debug("Returning hourly grouping function")
            return hourly_grouping
        elif freq == 'D':
            logging.debug("Returning daily grouping function")
            return daily_grouping
        elif freq == 'W':
            logging.debug("Returning weekly grouping function")
            return weekly_grouping
        elif freq == 'M':
            logging.debug("Returning monthly grouping function")
            return monthly_grouping
        else:
            logging.error(f"Invalid frequency: {freq}")
            raise ValueError(f"Invalid frequency: {freq}")


    @staticmethod
    def get_frequency(period):
        # 指定された期間に対応するpandasの頻度文字列を返す
        return PeriodManager.PERIOD_MAPPING[period]
    
    @staticmethod
    def get_full_hour_range(df, date_column):
        # データフレーム内の日付範囲に基づいて、全ての時間帯のリストを生成
        start_date = df[date_column].min().normalize()  # 最小日付の0時
        end_date = df[date_column].max().normalize() + pd.Timedelta(days=1)  # 最大日付の翌日の0時
        return pd.date_range(start=start_date, end=end_date, freq='H')[:-1]  # 1時間おきの日付時刻リスト（最後の要素を除く）
    
    DATE_FORMATS = {
        '時間別': '%H:00',
        '日別': '%m/%d(%a)',
        '週別': '%Y-%m-%d',
        '月別': '%Y-%m'
    }

    @staticmethod
    def format_date(date, period):
        if period == '日別':
            # 日付のみを取得し、曜日を付加
            return date.strftime('%m/%d') + f"({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][date.weekday()]})"
        return date.strftime(PeriodManager.DATE_FORMATS[period])

    @staticmethod
    def get_date_range(start_date, end_date, period):
        # 指定された期間と日付範囲に基づいて、日付のリストを生成
        freq = PeriodManager.PERIOD_MAPPING[period]
        return pd.date_range(start=start_date, end=end_date, freq=freq)