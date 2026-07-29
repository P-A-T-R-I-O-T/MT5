# src/utils/history_manager.py

import pandas as pd
import MetaTrader5 as mt5
from pathlib import Path
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

class HistoryManager:
    TIMEFRAME_MAPPING = {
        'M1': mt5.TIMEFRAME_M1,
        'M5': mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'H1': mt5.TIMEFRAME_H1,
        'D1': mt5.TIMEFRAME_D1
    }

    @classmethod
    def get_history_with_archiving(cls, mt5_conn, history_settings: dict):
        """
        Основной метод для подготовки данных:
        1. Проверка файла real.csv
        2. Архивация пропущенных дней
        3. Догрузка данных текущего дня
        """
        print("[DEBUG] get_history_with_archiving вызван")
        # Получаем настройки
        symbol = history_settings.get('symbols', ['GBPUSD'])[0]  # Берём первый символ
        timeframe_str = history_settings.get('timeframe', 'M1')
        start_date_str = history_settings.get('start_date', '')
        
        # Парсим start_date как UTC
        json_manager = mt5_conn.json_manager
        try:
            start_date = json_manager.parse_utc_date_string(start_date_str)
        except ValueError as e:
            print(f"Ошибка в формате даты: {e}")
            return
        
        timeframe = cls.TIMEFRAME_MAPPING.get(timeframe_str, mt5.TIMEFRAME_M1)
        
        # Путь к файлам
        base_dir = Path('F:/MT5-soft/history')
        output_file = base_dir / 'real.csv'
        
        # Проверяем и подготавливаем real.csv
        is_valid = cls.check_or_create_output_file(output_file)
        
        if not is_valid:
            print("[WARN] Файл real.csv признан битым. Удаляю и создаю заново.")
            try:
                os.remove(output_file)
            except OSError:
                pass
            cls.check_or_create_output_file(output_file)
            last_time = None
        else:
            last_time = cls.get_last_recorded_time(output_file)
        
        # Если нет валидных данных - начинаем с start_date
        if last_time is None:
            print(f"[INFO] Нет валидных данных. Начинаем с {start_date_str}")
            cls.prepare_real_csv_for_current_day(output_file)
            # Загружаем историю с start_date до сегодня
            cls.download_historical_from_start(mt5_conn, symbol, timeframe, start_date, base_dir)
            return
        
        # ВСЕГДА загружаем историю от start_date до вчера
        start_date_for_archive = start_date.date()  # Всегда от start_date!
        today_date = datetime.now(timezone.utc).date()
        yesterday_date = today_date - timedelta(days=1)  # До вчерашнего дня

        if start_date_for_archive <= yesterday_date:
            print(f"[INFO] Загрузка исторических данных с {start_date_for_archive} по {yesterday_date}.")
            cls.download_and_archive_missing_days(
                mt5_conn, symbol, timeframe, 
                start_date_for_archive, yesterday_date, base_dir
            )
        
        # После архивации готовим real.csv для текущего дня
        cls.prepare_real_csv_for_current_day(output_file)
        
        # Догружаем бары текущего дня
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        utc_now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        if today_start < utc_now:
            df_today = cls.get_latest_closed_bars(
                mt5_conn, symbol, timeframe, today_start, utc_now
            )
            if df_today is not None and not df_today.empty:
                cls.save_bars_to_csv(df_today, output_file)
                print(f"[INFO] Догружены бары текущего дня в real.csv: {len(df_today)} шт.")

        print(f"[DEBUG] last_time = {last_time}")
        print(f"[DEBUG] start_date = {start_date}")
        print(f"[DEBUG] start_date_for_archive = {start_date_for_archive}")
        print(f"[DEBUG] today_date = {today_date}")

    @staticmethod
    def check_or_create_output_file(filename: Path) -> bool:
        """Проверяет существование и целостность файла real.csv"""
        required_columns = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume', 'symbol']
        
        if not filename.exists():
            # Создаём директорию, если её нет
            filename.parent.mkdir(parents=True, exist_ok=True)
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                f.write(','.join(required_columns) + '\n')
            return True
        
        try:
            df = pd.read_csv(filename, nrows=0)
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                print(f"Файл {filename} имеет неверную структуру (отсутствуют колонки: {missing_cols}).")
                return False
            return True
        except Exception as e:
            print(f"Ошибка чтения файла {filename}: {e}")
            return False

    @staticmethod
    def get_last_recorded_time(filename: Path) -> Optional[datetime]:
        """Возвращает время последней записанной минуты в UTC"""
        if not filename.exists():
            return None
        
        try:
            df = pd.read_csv(filename)
            if df.empty:
                return None
            
            if 'time' not in df.columns:
                return None
            
            # Парсим время в формате DD-MM-YYYY HH:MM:SS
            df['time'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', utc=True)
            last_time = df['time'].iloc[-1]
            last_minute = last_time.replace(second=0, microsecond=0)
            return last_minute
        except Exception:
            return None

    @staticmethod
    def get_latest_closed_bars(mt5_conn, symbol: str, timeframe: int, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Получает закрытые бары в указанном диапазоне"""
        rates = mt5_conn.copy_rates_range(
            symbol,
            timeframe,
            start_time.replace(second=0, microsecond=0),
            end_time.replace(second=0, microsecond=0)
        )
        
        if rates is None:
            return None
        
        df = pd.DataFrame(rates)
        if df.empty:
            return None
        
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        cols_order = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        df = df[cols_order]
        df['symbol'] = symbol
        
        return df

    @staticmethod
    def save_bars_to_csv(df: pd.DataFrame, filename: Path) -> bool:
        """Сохраняет бары в CSV с форматом DD-MM-YYYY"""
        if df.empty:
            return False
        
        # Конвертируем время в строку формата DD-MM-YYYY HH:MM:SS
        df_display = df.copy()
        df_display['time'] = df_display['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
        
        file_exists = filename.exists()
        df_display.to_csv(
            filename,
            mode='a',
            index=False,
            header=not file_exists
        )
        return True

    @staticmethod
    def download_and_archive_missing_days(mt5_conn, symbol: str, timeframe: int, 
                                         start_date, end_date, base_dir: Path):
        """Скачивает и архивирует пропущенные дни в формат AVRO"""
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() >= 5:  # Выходные
                current_date += timedelta(days=1)
                continue
            
            date_str = current_date.strftime('%d-%m-%Y')
            start_time = datetime.combine(current_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_time = datetime.combine(current_date, datetime.max.time()).replace(tzinfo=timezone.utc)
            
            print(f"[DOWNLOAD] Загрузка данных за {date_str}...")
            df_day = HistoryManager.get_latest_closed_bars(mt5_conn, symbol, timeframe, start_time, end_time)
            
            if df_day is not None and not df_day.empty:
                HistoryManager.save_daily_data_avro(df_day, current_date, base_dir)
            
            current_date += timedelta(days=1)

    @staticmethod
    def save_daily_data_avro(df: pd.DataFrame, date: datetime, base_dir: Path):
        """Сохраняет дневные данные в AVRO файл с форматом DD-MM-YYYY"""
        date_str = date.strftime('%d-%m-%Y')
        file_path = base_dir / f"{date_str}.avro"
        
        # Конвертируем время в строку для AVRO
        df_display = df.copy()
        df_display['time'] = df_display['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
        
        try:
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, df_display], ignore_index=True)
                combined_df.drop_duplicates(subset=['time', 'symbol'], keep='last', inplace=True)
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df_display.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"[ARCHIVE] Данные за {date_str} сохранены в {file_path}")
        except Exception as e:
            print(f"Ошибка при сохранении данных за {date_str}: {e}")

    @staticmethod
    def download_historical_from_start(mt5_conn, symbol: str, timeframe: int, start_date: datetime, base_dir: Path):
        """Скачивает всю историю от start_date до сегодня в AVRO"""
        utc_now = datetime.now(timezone.utc).date()
        start_date_only = start_date.date()
        
        current_date = start_date_only
        while current_date <= utc_now:
            if current_date.weekday() >= 5:  # Выходные
                current_date += timedelta(days=1)
                continue
            
            date_str = current_date.strftime('%d-%m-%Y')
            start_time = datetime.combine(current_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_time = datetime.combine(current_date, datetime.max.time()).replace(tzinfo=timezone.utc)
            
            print(f"[DOWNLOAD] Загрузка данных за {date_str}...")
            df_day = HistoryManager.get_latest_closed_bars(mt5_conn, symbol, timeframe, start_time, end_time)
            
            if df_day is not None and not df_day.empty:
                HistoryManager.save_daily_data_avro(df_day, current_date, base_dir)
            
            current_date += timedelta(days=1)

    @staticmethod
    def prepare_real_csv_for_current_day(filename: Path):
        """Подготавливает real.csv для текущего дня"""
        required_columns = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume', 'symbol']
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            f.write(','.join(required_columns) + '\n')
        print(f"[INFO] Файл {filename} подготовлен для текущего дня.")

    @staticmethod
    def get_latest_closed_bars(mt5_conn, symbol: str, timeframe: int, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Получает закрытые бары в указанном диапазоне"""
        # ИСПРАВЛЕНИЕ: используем mt5.copy_rates_range напрямую, а не mt5_conn.copy_rates_range
        rates = mt5.copy_rates_range(
            symbol,
            timeframe,
            start_time.replace(second=0, microsecond=0),
            end_time.replace(second=0, microsecond=0)
        )
        
        if rates is None:
            return None
        
        df = pd.DataFrame(rates)
        if df.empty:
            return None
        
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        cols_order = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        df = df[cols_order]
        df['symbol'] = symbol
        
        return df