# src/utils/history_manager.py

import os
import pandas as pd
#import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime
#from typing import Optional
# from src.utils.file_checker import TIMEFRAME_MAPPING
from src.utils.file_checker import REAL_CSV_COLUMNS

class HistoryManager:



    @staticmethod
    def reset_real_csv(filename: Path) -> None:
        """
        Создаёт/перезаписывает real.csv с заголовками.
        Атомарная замена: пишем во временный файл, затем os.replace.
        """
        print(f"[HistoryManager.reset_real_csv] Создание/перезапись файла: {filename}")

        required_columns = REAL_CSV_COLUMNS
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
            df_new['time'] = df_new['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
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
            combined = combined[REAL_CSV_COLUMNS]          # ← выравниваем порядок колонок

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
    def save_daily_parquet(
        df: pd.DataFrame,
        date: datetime,
        base_dir: Path,
        timeframe: str,
    ) -> bool:
        """
        Сохраняет бары за день в history/<timeframe>/YYYY-MM-DD.parquet.

        Используется и для M1, и для старших ТФ (M5, M15, H1, D1).
        Сортировка: (symbol, time).
        Перезаписывает файл целиком (атомарно).

        :param df: DataFrame с колонками time, open, high, low, close,
                tick_volume, symbol
        :param date: дата
        :param base_dir: корень history/
        :param timeframe: 'M1' | 'M5' | 'M15' | 'H1' | 'D1'
        """
        print(f"[HistoryManager.save_daily_parquet] {timeframe} за {date}...")

        if df is None or df.empty:
            print("[HistoryManager.save_daily_parquet] Пустой DataFrame, пропуск")
            return False

        tf_dir = base_dir / timeframe
        date_str = date.strftime('%Y-%m-%d')
        file_path = tf_dir / f"{date_str}.parquet"
        tmp_path = tf_dir / f"{date_str}.parquet.tmp"

        # 1. Готовим данные
        df_display = df.copy()
        df_display['time'] = pd.to_datetime(df_display['time'])
        df_display['time'] = df_display['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Сортировка по (symbol, time)
        if 'symbol' in df_display.columns:
            df_display = df_display.sort_values(['symbol', 'time']).reset_index(drop=True)
        else:
            df_display = df_display.sort_values(['time']).reset_index(drop=True)

        try:
            tf_dir.mkdir(parents=True, exist_ok=True)

            with open(tmp_path, 'wb') as f:
                df_display.to_parquet(f, engine='pyarrow', index=False)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, file_path)
            print(f"[HistoryManager.save_daily_parquet] Записано {len(df_display)} строк "
                f"в {file_path}")
            return True

        except Exception as e:
            print(f"[HistoryManager.save_daily_parquet] Ошибка: {e}")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return False

    @staticmethod
    def append_to_single_parquet(df: pd.DataFrame, file_path: Path) -> bool:
        """
        Идемпотентно добавляет данные в один общий parquet-файл (для D1).
        Дедуп по (symbol, time): при повторе запись перезаписывается свежей.
        """
        print(f"[HistoryManager.append_to_single_parquet] {file_path}")
        if df is None or df.empty:
            return False

        try:
            df_new = df.copy()
            df_new['time'] = pd.to_datetime(df_new['time'])
            df_new['time'] = df_new['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

            if file_path.exists():
                df_existing = pd.read_parquet(file_path)
                if 'time' in df_existing.columns:
                    df_existing['time'] = pd.to_datetime(df_existing['time'])
                    df_existing['time'] = df_existing['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                combined = df_new

            combined = combined.drop_duplicates(subset=['symbol', 'time'], keep='last')
            if 'symbol' in combined.columns:
                combined = combined.sort_values(['symbol', 'time']).reset_index(drop=True)
            else:
                combined = combined.sort_values(['time']).reset_index(drop=True)

            file_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = file_path.with_suffix(file_path.suffix + '.tmp')
            with open(tmp_path, 'wb') as f:
                combined.to_parquet(f, engine='pyarrow', index=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, file_path)
            print(f"[HistoryManager.append_to_single_parquet] "
                f"Записано {len(df_new)} новых, всего {len(combined)}")
            return True

        except Exception as e:
            print(f"[HistoryManager.append_to_single_parquet] Ошибка: {e}")
            try:
                tmp_path = file_path.with_suffix(file_path.suffix + '.tmp')
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return False