# src/utils/history_manager.py

import os
import pandas as pd
import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime
from typing import Optional
from src.utils.file_checker import TIMEFRAME_MAPPING

class HistoryManager:



    @staticmethod
    def reset_real_csv(filename: Path) -> None:
        """
        Создаёт/перезаписывает real.csv с заголовками.
        Атомарная замена: пишем во временный файл, затем os.replace.
        """
        print(f"[HistoryManager.reset_real_csv] Создание/перезапись файла: {filename}")

        required_columns = ['time', 'open', 'high', 'low', 'close',
                            'tick_volume', 'spread', 'real_volume', 'symbol']
        print(f"[HistoryManager.reset_real_csv] Колонки: {required_columns}")

        try:
            # Создаём директорию, если её нет
            print(f"[HistoryManager.reset_real_csv] Создание директории: {filename.parent}")
            filename.parent.mkdir(parents=True, exist_ok=True)

            # Пишем во временный файл
            tmp_path = filename.with_suffix(filename.suffix + '.tmp')
            print(f"[HistoryManager.reset_real_csv] Запись во временный файл: {tmp_path}")

            with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
                f.write(','.join(required_columns) + '\n')
                f.flush()
                os.fsync(f.fileno())  # гарантируем, что данные на диске

            # Атомарная замена
            print(f"[HistoryManager.reset_real_csv] Атомарная замена {tmp_path} -> {filename}")
            os.replace(tmp_path, filename)

            print(f"[HistoryManager.reset_real_csv] Файл {filename} создан/перезаписан с заголовками")

        except Exception as e:
            print(f"[HistoryManager.reset_real_csv] Ошибка при создании файла {filename}: {e}")
            # Пытаемся убрать временный файл, если он остался
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            raise

    @staticmethod
    def append_to_real_csv(df: pd.DataFrame, filename: Path) -> bool:
        """
        Добавляет данные в real.csv с сохранением сортировки (time, symbol).
        Читает существующий файл, объединяет с новыми данными,
        сортирует, перезаписывает атомарно.
        """
        print(f"[HistoryManager.append_to_real_csv] Добавление данных в файл: {filename}")
        print(f"[HistoryManager.append_to_real_csv] Размер новых данных: {df.shape}")

        if df.empty:
            print("[HistoryManager.append_to_real_csv] Пустой DataFrame, нечего добавлять")
            return False

        try:
            # 1. Готовим новые данные
            df_new = df.copy()
            df_new['time'] = df_new['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
            print(f"[HistoryManager.append_to_real_csv] Первые новые записи: {df_new.head(3)}")

            # 2. Читаем существующий файл (если есть)
            if filename.exists():
                print(f"[HistoryManager.append_to_real_csv] Чтение существующего файла {filename}")
                df_existing = pd.read_csv(filename, dtype={'time': str})
                print(f"[HistoryManager.append_to_real_csv] Прочитано {len(df_existing)} старых записей")
            else:
                print(f"[HistoryManager.append_to_real_csv] Файл {filename} не существует — создаём новый")
                df_existing = pd.DataFrame(columns=df_new.columns)

            # 3. Объединяем
            combined = pd.concat([df_existing, df_new], ignore_index=True)

            # 4. Сортируем по (time, symbol) — тот же порядок, что в save_daily_data_avro
            print("[HistoryManager.append_to_real_csv] Сортировка по (time, symbol)...")
            combined = combined.sort_values(['time', 'symbol'])
            combined = combined.reset_index(drop=True)

            # 5. Атомарная перезапись: пишем во временный файл, потом os.replace
            tmp_path = filename.with_suffix(filename.suffix + '.tmp')
            print(f"[HistoryManager.append_to_real_csv] Запись во временный файл: {tmp_path}")

            # Пишем во временный файл, fsync на write-дескрипторе (не read!)
            with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
                combined.to_csv(f, index=False)
                f.flush()
                os.fsync(f.fileno())

            print(f"[HistoryManager.append_to_real_csv] Атомарная замена {tmp_path} -> {filename}")
            os.replace(tmp_path, filename)
            return True

        except Exception as e:
            print(f"[HistoryManager.append_to_real_csv] Ошибка: {e}")
            # Пытаемся убрать временный файл
            try:
                tmp_path = filename.with_suffix(filename.suffix + '.tmp')
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return False

    @staticmethod
    def save_daily_data_avro(df: pd.DataFrame, date: datetime, base_dir: Path):
        """Сохраняет дневные данные в AVRO файл с форматом DD-MM-YYYY (все символы в одном файле)"""
        print(f"[HistoryManager.save_daily_data_avro] Сохранение дневных данных за {date}...")
        print(f"[HistoryManager.save_daily_data_avro] Размер DataFrame: {df.shape}")
        print(f"[HistoryManager.save_daily_data_avro] Базовая директория: {base_dir}")
        
        date_str = date.strftime('%Y-%m-%d')
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

