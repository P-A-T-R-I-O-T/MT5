from datetime import datetime
import pandas as pd
import pathlib
import MetaTrader5 as mt5
from pathlib import Path

class HistoryManager:
    TIMEFRAME_MAPPING = {
        'M1': mt5.TIMEFRAME_M1,
        'M5': mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'H1': mt5.TIMEFRAME_H1,
        'D1': mt5.TIMEFRAME_D1
    }

    @classmethod
    def download_and_save_history(cls, mt5_conn, history_settings: dict):
        symbols = history_settings.get('symbols', [])
        timeframe_str = history_settings.get('timeframe', 'M1')
        start_date_str = history_settings.get('start_date', '')
        end_date_str = history_settings.get('end_date', '')

        timeframe = cls.TIMEFRAME_MAPPING.get(timeframe_str, mt5.TIMEFRAME_M1)

        # Используем JsonManager для парсинга и конвертации дат
        json_manager = mt5_conn.json_manager
        try:
            start_date = json_manager.parse_utc_date_string(start_date_str)
            end_date = json_manager.parse_utc_date_string(end_date_str)
        except ValueError as e:
            print(f"Ошибка в формате даты: {e}")
            return

        if start_date >= end_date:
            print("Ошибка: начальная дата должна быть раньше конечной")
            return

        print(f"Исходная дата начала: {start_date_str} → UTC: {start_date.isoformat()}")
        print(f"Исходная дата начала: {end_date_str} → UTC: {end_date.isoformat()}")

        # Собираем все данные в один DataFrame
        all_data = []

        for symbol in symbols:
            print(f"\nЗагрузка исторических данных для {symbol}...")
            df = mt5_conn.get_historical_data(symbol, timeframe, start_date, end_date)
            if df is not None:
                df['symbol'] = symbol
                all_data.append(df)

        # Объединяем все данные
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df.sort_values('time', inplace=True)
            cls._save_by_days(combined_df, output_dir="F:/MT5-soft/history")
        else:
            print("Нет данных для сохранения")



    @staticmethod
    def save_to_csv(df: pd.DataFrame, symbol: str, timeframe: str):
        """Сохраняет DataFrame в CSV‑файл."""
        history_dir = pathlib.Path('F:/MT5-soft/history')
        symbol_dir = history_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        filename = symbol_dir / f"{symbol}_{timeframe}.csv"
        df.to_csv(filename, index=False)
        print(f"Данные сохранены в: {filename}")


    @staticmethod
    def save_daily_candles(df: pd.DataFrame, date_str: str, output_dir: str = "history"):
        """
        Сохраняет свечи за день в Parquet‑файл.
        :param df: DataFrame с колонками ['time', 'symbol', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        :param date_str: дата в формате 'YYYY-MM-DD'
        :param output_dir: путь к директории для сохранения
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / f"{date_str}.parquet"

        # Приводим типы для оптимизации Parquet
        df['symbol'] = df['symbol'].astype('string')
        df['time'] = pd.to_datetime(df['time'])

        try:
            if file_path.exists():
                # Читаем существующий файл, объединяем с новыми данными, удаляем дубликаты по time+symbol
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.drop_duplicates(subset=['time', 'symbol'], keep='last', inplace=True)
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df.to_parquet(file_path, engine='pyarrow', index=False)

            print(f"Данные за {date_str} сохранены в {file_path}")
        except Exception as e:
            print(f"Ошибка при сохранении данных за {date_str}: {e}")


    @staticmethod
    def _save_by_days(df: pd.DataFrame, output_dir: str = "history"):
        """
        Разбивает DataFrame по дням и сохраняет каждый день в отдельный Parquet‑файл.
        :param df: объединённый DataFrame со всеми данными и колонкой 'symbol'
        :param output_dir: путь к директории для сохранения
        """
        # Создаём колонку с датой (без времени) для группировки
        df['date'] = df['time'].dt.date

        # Группируем по датам
        grouped = df.groupby('date')

        for date_obj, day_df in grouped:
            date_str = date_obj.strftime('%Y-%m-%d')
            # Удаляем служебную колонку 'date' перед сохранением
            day_df_clean = day_df.drop('date', axis=1).copy()
            # Сохраняем данные за конкретный день
            HistoryManager.save_daily_candles(day_df_clean, date_str, output_dir)
