# src/utils/history_manager.py

import pandas as pd
import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime
from typing import Optional

class HistoryManager:
    TIMEFRAME_MAPPING = {
        'M1': mt5.TIMEFRAME_M1,
        'M5': mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'H1': mt5.TIMEFRAME_H1,
        'D1': mt5.TIMEFRAME_D1
    }

    @staticmethod
    def check_and_create_history_dir(base_dir: Path) -> bool:
        """Проверяет и создает директорию history."""
        print(f"[HistoryManager.check_and_create_history_dir] Проверка директории: {base_dir}")
        
        try:
            # Проверяем существование директории
            if base_dir.exists():
                print(f"[HistoryManager.check_and_create_history_dir] Директория {base_dir} уже существует")
                return True
            
            # Создаем директорию
            print(f"[HistoryManager.check_and_create_history_dir] Создание директории {base_dir}...")
            base_dir.mkdir(parents=True, exist_ok=True)
            print(f"[HistoryManager.check_and_create_history_dir] Директория {base_dir} создана")
            return True
            
        except Exception as e:
            print(f"[HistoryManager.check_and_create_history_dir] Ошибка при создании директории {base_dir}: {e}")
            return False

    @staticmethod
    def validate_settings(settings: dict) -> tuple:
        """
        Проверяет настройки на противоречия.
        Возвращает (True, []) если все ок, иначе (False, [список ошибок])
        """
        print("[HistoryManager.validate_settings] Начало валидации настроек...")
        print(f"[HistoryManager.validate_settings] Настройки: {settings}")
        
        errors = []
        
        try:
            # Проверяем наличие секции history
            if 'history' not in settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует секция 'history'")
                errors.append("Отсутствует секция 'history' в настройках")
                return False, errors
            
            print("[HistoryManager.validate_settings] Секция 'history' найдена")
            history_settings = settings['history']
            print(f"[HistoryManager.validate_settings] Настройки history: {history_settings}")
            
            # Проверяем enabled
            print("[HistoryManager.validate_settings] Проверка параметра 'enabled'...")
            if 'enabled' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'enabled'")
                errors.append("Отсутствует параметр 'enabled' в history")
            elif not isinstance(history_settings['enabled'], bool):
                print(f"[HistoryManager.validate_settings] Ошибка: 'enabled' не boolean: {history_settings['enabled']}")
                errors.append("Параметр 'enabled' должен быть boolean")
            else:
                print(f"[HistoryManager.validate_settings] 'enabled' = {history_settings['enabled']}")
            
            # Проверяем symbols
            print("[HistoryManager.validate_settings] Проверка параметра 'symbols'...")
            if 'symbols' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'symbols'")
                errors.append("Отсутствует параметр 'symbols' в history")
            elif not isinstance(history_settings['symbols'], list):
                print(f"[HistoryManager.validate_settings] Ошибка: 'symbols' не список: {history_settings['symbols']}")
                errors.append("Параметр 'symbols' должен быть списком")
            elif len(history_settings['symbols']) == 0:
                print("[HistoryManager.validate_settings] Ошибка: список символов пуст")
                errors.append("Список символов пуст")
            else:
                print(f"[HistoryManager.validate_settings] 'symbols' = {history_settings['symbols']} (количество: {len(history_settings['symbols'])})")
            
            # Проверяем timeframe
            print("[HistoryManager.validate_settings] Проверка параметра 'timeframe'...")
            if 'timeframe' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'timeframe'")
                errors.append("Отсутствует параметр 'timeframe' в history")
            elif history_settings['timeframe'] not in HistoryManager.TIMEFRAME_MAPPING:
                print(f"[HistoryManager.validate_settings] Ошибка: неподдерживаемый таймфрейм: {history_settings['timeframe']}")
                errors.append(f"Неподдерживаемый таймфрейм: {history_settings['timeframe']}")
            else:
                print(f"[HistoryManager.validate_settings] 'timeframe' = {history_settings['timeframe']}")
            
            # Проверяем start_date
            print("[HistoryManager.validate_settings] Проверка параметра 'start_date'...")
            if 'start_date' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'start_date'")
                errors.append("Отсутствует параметр 'start_date' в history")
            else:
                try:
                    # Пробуем распарсить дату
                    datetime.strptime(history_settings['start_date'], '%Y-%m-%d')
                    print(f"[HistoryManager.validate_settings] 'start_date' = {history_settings['start_date']} (корректный формат)")
                except ValueError:
                    print(f"[HistoryManager.validate_settings] Ошибка: неверный формат даты: {history_settings['start_date']}")
                    errors.append(f"Неверный формат даты: {history_settings['start_date']}")
            
            # Проверяем mode
            print("[HistoryManager.validate_settings] Проверка параметра 'mode'...")
            if 'mode' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'mode'")
                errors.append("Отсутствует параметр 'mode' в history")
            elif history_settings['mode'] not in ['all', 'custom']:
                print(f"[HistoryManager.validate_settings] Ошибка: неподдерживаемый режим: {history_settings['mode']}")
                errors.append(f"Неподдерживаемый режим: {history_settings['mode']}")
            else:
                print(f"[HistoryManager.validate_settings] 'mode' = {history_settings['mode']}")
            
            # Проверяем symbols_directories
            print("[HistoryManager.validate_settings] Проверка параметра 'symbols_directories'...")
            if 'symbols_directories' not in history_settings:
                print("[HistoryManager.validate_settings] Ошибка: отсутствует 'symbols_directories'")
                errors.append("Отсутствует параметр 'symbols_directories' в history")
            elif history_settings['symbols_directories'] not in ['on', 'off', 'all']:
                print(f"[HistoryManager.validate_settings] Ошибка: неподдерживаемое значение: {history_settings['symbols_directories']}")
                errors.append(f"Неподдерживаемое значение symbols_directories: {history_settings['symbols_directories']}")
            else:
                print(f"[HistoryManager.validate_settings] 'symbols_directories' = {history_settings['symbols_directories']}")
            
            # Если mode == 'custom', проверяем что symbols содержит только разрешенные символы
            if history_settings.get('mode') == 'custom' and isinstance(history_settings.get('symbols'), list):
                print("[HistoryManager.validate_settings] Проверка режима 'custom'...")
                if len(history_settings['symbols']) == 0:
                    print("[HistoryManager.validate_settings] Ошибка: в режиме 'custom' нет символов")
                    errors.append("В режиме 'custom' должен быть указан хотя бы один символ")
                else:
                    print(f"[HistoryManager.validate_settings] В режиме 'custom' указано символов: {len(history_settings['symbols'])}")
            
        except Exception as e:
            print(f"[HistoryManager.validate_settings] Ошибка при валидации: {e}")
            errors.append(f"Ошибка при валидации настроек: {e}")
        
        if errors:
            print("[HistoryManager.validate_settings] Найдены ошибки в настройках:")
            for error in errors:
                print(f"  - {error}")
            return False, errors
        
        print("[HistoryManager.validate_settings] Настройки корректны")
        return True, []

    @staticmethod
    def save_real_csv_header(filename: Path) -> None:
        """Создает/перезаписывает real.csv с заголовками."""
        print(f"[HistoryManager.save_real_csv_header] Создание/перезапись файла: {filename}")
        
        required_columns = ['time', 'open', 'high', 'low', 'close', 
                        'tick_volume', 'spread', 'real_volume', 'symbol']
        print(f"[HistoryManager.save_real_csv_header] Колонки: {required_columns}")
        
        try:
            # Создаём директорию, если её нет
            print(f"[HistoryManager.save_real_csv_header] Создание директории: {filename.parent}")
            filename.parent.mkdir(parents=True, exist_ok=True)
            
            # Записываем заголовки (перезапись)
            print("[HistoryManager.save_real_csv_header] Запись заголовков...")
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                f.write(','.join(required_columns) + '\n')
            
            print(f"[HistoryManager.save_real_csv_header] Файл {filename} создан/перезаписан с заголовками")
            
        except Exception as e:
            print(f"[HistoryManager.save_real_csv_header] Ошибка при создании файла {filename}: {e}")
            raise

    @staticmethod
    def append_to_real_csv(df: pd.DataFrame, filename: Path) -> bool:
        """
        Добавляет данные в real.csv.
        Предполагается, что файл уже существует с заголовками.
        """
        print(f"[HistoryManager.append_to_real_csv] Добавление данных в файл: {filename}")
        print(f"[HistoryManager.append_to_real_csv] Размер DataFrame: {df.shape}")
        
        if df.empty:
            print("[HistoryManager.append_to_real_csv] Пустой DataFrame, нечего добавлять")
            return False
        
        try:
            # Конвертируем время в строку
            print("[HistoryManager.append_to_real_csv] Конвертация времени в строку...")
            df_display = df.copy()
            df_display['time'] = df_display['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
            print(f"[HistoryManager.append_to_real_csv] Первые записи: {df_display.head()}")
            
            # Проверяем, что файл существует
            if not filename.exists():
                print(f"[HistoryManager.append_to_real_csv] Файл {filename} не существует. Создаем с заголовками.")
                HistoryManager.save_real_csv_header(filename)
            else:
                print(f"[HistoryManager.append_to_real_csv] Файл {filename} существует")
            
            # Добавляем данные
            print("[HistoryManager.append_to_real_csv] Добавление данных в файл...")
            df_display.to_csv(
                filename,
                mode='a',  # append mode
                index=False,
                header=False  # заголовки уже есть
            )
            
            print(f"[HistoryManager.append_to_real_csv] Добавлено {len(df_display)} записей в {filename}")
            return True
            
        except Exception as e:
            print(f"[HistoryManager.append_to_real_csv] Ошибка при добавлении данных в {filename}: {e}")
            return False


    @staticmethod
    def save_daily_data_avro(df: pd.DataFrame, date: datetime, base_dir: Path):
        """Сохраняет дневные данные в AVRO файл с форматом DD-MM-YYYY (все символы в одном файле)"""
        print(f"[HistoryManager.save_daily_data_avro] Сохранение дневных данных за {date}...")
        print(f"[HistoryManager.save_daily_data_avro] Размер DataFrame: {df.shape}")
        print(f"[HistoryManager.save_daily_data_avro] Базовая директория: {base_dir}")
        
        date_str = date.strftime('%d-%m-%Y')
        file_path = base_dir / f"{date_str}.avro"
        print(f"[HistoryManager.save_daily_data_avro] Путь к файлу: {file_path}")
        
        # Конвертируем время в строку для AVRO
        print("[HistoryManager.save_daily_data_avro] Конвертация времени в строку...")
        df_display = df.copy()
        df_display['time'] = df_display['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
        print(f"[HistoryManager.save_daily_data_avro] Первые записи: {df_display.head()}")
        
        try:
            if file_path.exists():
                print(f"[HistoryManager.save_daily_data_avro] Файл {file_path} существует, объединение...")
                # Читаем существующий файл
                existing_df = pd.read_parquet(file_path)
                print(f"[HistoryManager.save_daily_data_avro] Существующие данные: {existing_df.shape}")
                
                # Объединяем с новыми данными
                combined_df = pd.concat([existing_df, df_display], ignore_index=True)
                print(f"[HistoryManager.save_daily_data_avro] Объединенные данные: {combined_df.shape}")
                
                # Удаляем дубликаты по времени и символу
                combined_df.drop_duplicates(subset=['time', 'symbol'], keep='last', inplace=True)
                print(f"[HistoryManager.save_daily_data_avro] После удаления дубликатов: {combined_df.shape}")
                
                # Сортируем по времени и символу
                combined_df = combined_df.sort_values(['time', 'symbol'])
                print("[HistoryManager.save_daily_data_avro] Данные отсортированы")
                
                print("[HistoryManager.save_daily_data_avro] Сохранение в parquet...")
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                print(f"[HistoryManager.save_daily_data_avro] Файл {file_path} не существует, создание нового...")
                # Сортируем перед сохранением
                df_display = df_display.sort_values(['time', 'symbol'])
                print("[HistoryManager.save_daily_data_avro] Данные отсортированы")
                
                print("[HistoryManager.save_daily_data_avro] Сохранение в parquet...")
                df_display.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"[HistoryManager.save_daily_data_avro] Данные за {date_str} сохранены в {file_path} (всего записей: {len(df_display)})")
        except Exception as e:
            print(f"[HistoryManager.save_daily_data_avro] Ошибка при сохранении данных за {date_str}: {e}")

    @staticmethod
    def get_last_recorded_time(filename: Path) -> Optional[datetime]:
        """Возвращает время последней записи из real.csv в UTC"""
        print(f"[HistoryManager.get_last_recorded_time] Получение последней записи из: {filename}")
        
        if not filename.exists():
            print(f"[HistoryManager.get_last_recorded_time] Файл {filename} не существует")
            return None
        
        try:
            print("[HistoryManager.get_last_recorded_time] Чтение файла...")
            df = pd.read_csv(filename)
            print(f"[HistoryManager.get_last_recorded_time] Прочитано записей: {len(df)}")
            
            if df.empty:
                print("[HistoryManager.get_last_recorded_time] Файл пустой")
                return None
            
            if 'time' not in df.columns:
                print("[HistoryManager.get_last_recorded_time] Колонка 'time' не найдена")
                return None
            
            # Парсим время в формате DD-MM-YYYY HH:MM:SS
            print("[HistoryManager.get_last_recorded_time] Парсинг времени...")
            df['time'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', utc=True)
            last_time = df['time'].iloc[-1]
            last_minute = last_time.replace(second=0, microsecond=0)
            print(f"[HistoryManager.get_last_recorded_time] Последняя запись: {last_minute}")
            return last_minute
            
        except Exception as e:
            print(f"[HistoryManager.get_last_recorded_time] Ошибка чтения last_time из {filename}: {e}")
            return None

    @staticmethod
    def get_latest_closed_bars(symbol: str, timeframe: int, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Получает закрытые бары в указанном диапазоне"""
        print(f"[HistoryManager.get_latest_closed_bars] Получение баров для {symbol}...")
        print(f"[HistoryManager.get_latest_closed_bars] Таймфрейм: {timeframe}")
        print(f"[HistoryManager.get_latest_closed_bars] Начало: {start_time}")
        print(f"[HistoryManager.get_latest_closed_bars] Конец: {end_time}")
        
        print("[HistoryManager.get_latest_closed_bars] Вызов mt5.copy_rates_range...")
        rates = mt5.copy_rates_range(
            symbol,
            timeframe,
            start_time.replace(second=0, microsecond=0),
            end_time.replace(second=0, microsecond=0)
        )
        
        if rates is None:
            print(f"[HistoryManager.get_latest_closed_bars] Нет данных для {symbol}")
            return None
        
        print(f"[HistoryManager.get_latest_closed_bars] Получено {len(rates)} записей")
        
        df = pd.DataFrame(rates)
        if df.empty:
            print(f"[HistoryManager.get_latest_closed_bars] Пустой DataFrame для {symbol}")
            return None
        
        print("[HistoryManager.get_latest_closed_bars] Обработка данных...")
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        cols_order = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
        df = df[cols_order]
        df['symbol'] = symbol
        
        print(f"[HistoryManager.get_latest_closed_bars] Итоговый DataFrame: {df.shape}")
        return df