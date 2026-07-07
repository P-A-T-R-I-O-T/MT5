# src/core/realtime_collector.py

import time
import threading
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import MetaTrader5 as mt5

from src.utils.json_manager import JsonManager


class RealtimeCollector:
    """Класс для сбора данных в реальном времени с конвертацией в UTC."""
    
    def __init__(self, mt5_conn, json_manager: JsonManager):
        self.mt5_conn = mt5_conn
        self.json_manager = json_manager
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._symbols: List[str] = []
        self._timeframe = mt5.TIMEFRAME_M1
        self._last_times: Dict[str, datetime] = {}
        self._lock = threading.Lock()
        self._buffer: List[pd.DataFrame] = []  # Буфер для накопления данных
        self._buffer_size = 100  # Записываем в файл каждые 100 свечей
        self._last_save_time = datetime.utcnow()
        self._save_interval = timedelta(minutes=5)  # Или каждые 5 минут
        self._stop_requested = False  # Флаг для быстрой остановки
        
        # Кэшируем смещение времени сервера
        self._server_offset = None
        
        # Настройки
        settings = json_manager.load_settings()
        history_settings = settings.get('history', {})
        realtime_settings = history_settings.get('realtime', {})
        
        # Используем символы из realtime или из общего списка
        self._symbols = realtime_settings.get('symbols', [])
        if not self._symbols:
            self._symbols = history_settings.get('symbols', [])
        
        # Таймфрейм из настроек
        timeframe_str = realtime_settings.get('timeframe', 'M1')
        self._timeframe = self._get_timeframe(timeframe_str)
        
        # Путь для сохранения
        self.history_dir = Path('F:/MT5-soft/history')
        self.real_file = self.history_dir / 'real.parquet'
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # Загружаем существующие данные
        self._existing_data = self._load_existing_data()
        
        # Инициализируем last_times
        self._init_last_times()
    
    def _get_timeframe(self, timeframe_str: str) -> int:
        """Преобразует строку таймфрейма в константу MT5."""
        mapping = {
            'M1': mt5.TIMEFRAME_M1,
            'M5': mt5.TIMEFRAME_M5,
            'M15': mt5.TIMEFRAME_M15,
            'H1': mt5.TIMEFRAME_H1,
            'D1': mt5.TIMEFRAME_D1
        }
        return mapping.get(timeframe_str, mt5.TIMEFRAME_M1)
    
    def _load_existing_data(self) -> pd.DataFrame:
        """Загружает существующие данные из real.parquet."""
        if self.real_file.exists():
            try:
                df = pd.read_parquet(self.real_file)
                if 'time' in df.columns:
                    # Приводим время к UTC
                    if not pd.api.types.is_datetime64_any_dtype(df['time']):
                        df['time'] = pd.to_datetime(df['time'], utc=True)
                    else:
                        if df['time'].dt.tz is None:
                            df['time'] = df['time'].dt.tz_localize('UTC')
                        else:
                            df['time'] = df['time'].dt.tz_convert('UTC')
                return df
            except Exception as e:
                print(f"[RealtimeCollector] Ошибка загрузки real.parquet: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    def _init_last_times(self):
        """Инициализирует последние времена для каждого символа."""
        if self._existing_data.empty:
            return
        
        for symbol in self._symbols:
            symbol_data = self._existing_data[self._existing_data['symbol'] == symbol]
            if not symbol_data.empty:
                self._last_times[symbol] = symbol_data['time'].max()
    
    def start(self):
        """Запускает сбор данных в реальном времени."""
        if self._running:
            print("[RealtimeCollector] Уже запущен")
            return
        
        if not self._symbols:
            print("[RealtimeCollector] Нет символов для отслеживания")
            return
        
        if not self.mt5_conn._is_connected:
            print("[RealtimeCollector] MT5 не подключен")
            return
        
        self._running = True
        self._stop_requested = False
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()
        print(f"[RealtimeCollector] Запущен для {len(self._symbols)} символов")
    
    def stop(self):
        """Останавливает сбор данных и сохраняет буфер."""
        print("[RealtimeCollector] Запрос остановки...")
        self._stop_requested = True
        self._running = False
        
        if self._thread:
            # Ждем завершения потока с таймаутом
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                print("[RealtimeCollector] Поток не завершился, принудительная остановка...")
        
        # Сохраняем остатки буфера
        self._flush_buffer()
        print("[RealtimeCollector] Остановлен")
    
    def _collect_loop(self):
        """Основной цикл сбора данных с синхронизацией по UTC."""
        try:
            # Ждем начала следующей минуты для синхронизации
            self._sync_to_next_minute()
            
            while self._running and not self._stop_requested:
                try:
                    # Проверяем подключение
                    if not self._check_connection():
                        if self._stop_requested:
                            break
                        time.sleep(30)
                        continue
                    
                    # Собираем данные за последние минуты с запасом
                    self._collect_recent_data()
                    
                    # Сохраняем буфер при необходимости
                    self._check_and_save_buffer()
                    
                    # Ждем до следующей минуты, но проверяем флаг остановки
                    self._wait_until_next_minute()
                    
                except Exception as e:
                    print(f"[RealtimeCollector] Ошибка в цикле сбора: {e}")
                    if self._stop_requested:
                        break
                    time.sleep(10)
        except Exception as e:
            print(f"[RealtimeCollector] Критическая ошибка в цикле: {e}")
        finally:
            # Сохраняем буфер при выходе
            self._flush_buffer()
            print("[RealtimeCollector] Цикл сбора завершен")
    
    def _sync_to_next_minute(self):
        """Синхронизирует начало работы с началом следующей минуты."""
        now = datetime.utcnow()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_seconds = (next_minute - now).total_seconds()
        if wait_seconds > 0 and wait_seconds < 60:  # Не ждем больше минуты
            print(f"[RealtimeCollector] Синхронизация: ожидание {wait_seconds:.1f} секунд")
            # Проверяем флаг остановки во время ожидания
            for _ in range(int(wait_seconds)):
                if self._stop_requested:
                    return
                time.sleep(1)
    
    def _wait_until_next_minute(self):
        """Ожидает до начала следующей минуты с проверкой флага остановки."""
        now = datetime.utcnow()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_seconds = (next_minute - now).total_seconds()
        if wait_seconds > 0 and wait_seconds < 60:
            # Проверяем флаг остановки во время ожидания
            for _ in range(int(wait_seconds)):
                if self._stop_requested:
                    return
                time.sleep(1)
    
    def _check_connection(self) -> bool:
        """Проверяет подключение к MT5."""
        if not self.mt5_conn._is_connected:
            print("[RealtimeCollector] MT5 отключен, ожидание переподключения...")
            return False
        
        try:
            # Проверяем через терминал
            info = mt5.terminal_info()
            if info is None:
                print("[RealtimeCollector] MT5 недоступен")
                return False
            return True
        except:
            return False
    
    def _get_server_timezone_offset(self) -> int:
        """
        Определяет смещение времени сервера MT5 относительно UTC.
        Возвращает количество часов (например, 2 или 3).
        Результат кэшируется для ускорения.
        """
        if self._server_offset is not None:
            return self._server_offset
        
        try:
            # Пробуем получить время сервера через тик
            tick = mt5.symbol_info_tick("EURUSD")
            if tick and tick.time:
                server_time = datetime.fromtimestamp(tick.time)
                utc_time = datetime.utcnow()
                
                # Вычисляем разницу в часах
                diff = (server_time - utc_time).total_seconds() / 3600
                offset = round(diff)
                
                # Для форекс брокеров обычно 2 или 3
                if offset in [2, 3]:
                    print(f"[RealtimeCollector] Определено смещение сервера: GMT+{offset}")
                    self._server_offset = offset
                    return offset
                else:
                    print(f"[RealtimeCollector] Обнаружено нестандартное смещение: GMT+{offset}, используем 3")
                    self._server_offset = 3
                    return 3
        except Exception as e:
            print(f"[RealtimeCollector] Ошибка определения смещения: {e}")
        
        # По умолчанию для большинства форекс брокеров - GMT+3 (лето) или GMT+2 (зима)
        # Определяем по текущей дате
        now = datetime.utcnow()
        # В Европе летнее время с последнего воскресенья марта по последнее воскресенье октября
        is_summer = self._is_european_summer_time(now)
        offset = 3 if is_summer else 2
        print(f"[RealtimeCollector] Используем смещение по умолчанию: GMT+{offset} (летнее время: {is_summer})")
        self._server_offset = offset
        return offset
    
    def _is_european_summer_time(self, dt: datetime) -> bool:
        """Проверяет, является ли дата европейским летним временем."""
        year = dt.year
        # Последнее воскресенье марта
        last_sunday_march = self._get_last_sunday(year, 3)
        # Последнее воскресенье октября
        last_sunday_october = self._get_last_sunday(year, 10)
        
        return last_sunday_march <= dt <= last_sunday_october
    
    def _get_last_sunday(self, year: int, month: int) -> datetime:
        """Возвращает дату последнего воскресенья в указанном месяце."""
        from datetime import date
        # Последний день месяца
        if month == 12:
            last_day = date(year, month, 31)
        else:
            last_day = date(year, month + 1, 1) - timedelta(days=1)
        
        # Ищем воскресенье
        while last_day.weekday() != 6:  # 6 - воскресенье
            last_day -= timedelta(days=1)
        
        return datetime(last_day.year, last_day.month, last_day.day)
    
    def _collect_recent_data(self):
        """Собирает свечи за последние несколько минут и конвертирует в UTC."""
        now = datetime.utcnow()
        
        # Получаем последние 10 свечей для каждого символа с запасом
        count = 10  # Запрашиваем 10 свечей
        
        all_new_data = []
        
        # Получаем смещение сервера один раз
        server_offset = self._get_server_timezone_offset()
        
        for symbol in self._symbols:
            # Проверяем флаг остановки
            if self._stop_requested:
                break
                
            try:
                # Получаем последние N свечей
                rates = mt5.copy_rates_from_pos(
                    symbol,
                    self._timeframe,
                    0,  # Начинаем с последней свечи
                    count
                )
                
                if rates is None or len(rates) == 0:
                    continue
                
                # Конвертируем в DataFrame
                df = pd.DataFrame(rates)
                
                # КЛЮЧЕВОЙ МОМЕНТ: Конвертируем время в UTC
                # MT5 возвращает время в секундах с 1970-01-01 в локальном времени сервера
                # Мы переводим это в UTC, используя смещение сервера
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                
                # Корректируем время: вычитаем смещение, чтобы получить UTC
                # Если сервер в GMT+3, то вычитаем 3 часа
                df['time'] = df['time'] - pd.Timedelta(hours=server_offset)
                
                df['symbol'] = symbol
                
                # Фильтруем только новые данные
                last_time = self._last_times.get(symbol)
                if last_time:
                    # Для сравнения используем UTC время
                    df = df[df['time'] > last_time]
                
                if not df.empty:
                    all_new_data.append(df)
                    self._last_times[symbol] = df['time'].max()
                    # Выводим пример времени для отладки
                    sample_time = df['time'].iloc[0]
                    print(f"[RealtimeCollector] {symbol}: получено {len(df)} новых свечей, "
                          f"последняя в {sample_time.strftime('%H:%M:%S')} UTC")
                    
            except Exception as e:
                print(f"[RealtimeCollector] Ошибка для {symbol}: {e}")
        
        # Добавляем в буфер
        if all_new_data and not self._stop_requested:
            with self._lock:
                self._buffer.extend(all_new_data)
    
    def _check_and_save_buffer(self):
        """Проверяет буфер и сохраняет при необходимости."""
        if self._stop_requested:
            return
            
        should_save = False
        
        with self._lock:
            buffer_count = sum(len(df) for df in self._buffer)
            if buffer_count >= self._buffer_size:
                should_save = True
            elif (datetime.utcnow() - self._last_save_time) >= self._save_interval:
                if buffer_count > 0:
                    should_save = True
        
        if should_save:
            self._flush_buffer()
    
    def _flush_buffer(self):
        """Сохраняет буфер в файл."""
        with self._lock:
            if not self._buffer:
                return
            
            try:
                # Объединяем все данные из буфера
                new_df = pd.concat(self._buffer, ignore_index=True)
                self._buffer = []
                self._last_save_time = datetime.utcnow()
                
                # Сохраняем в файл
                self._save_data(new_df)
                
            except Exception as e:
                print(f"[RealtimeCollector] Ошибка сохранения буфера: {e}")
    
    def _save_data(self, new_df: pd.DataFrame):
        """Сохраняет новые данные в real.parquet."""
        if new_df.empty:
            return
        
        try:
            # Загружаем существующие данные
            if self.real_file.exists():
                existing_df = pd.read_parquet(self.real_file)
                # Приводим время к UTC
                if 'time' in existing_df.columns and not pd.api.types.is_datetime64_any_dtype(existing_df['time']):
                    existing_df['time'] = pd.to_datetime(existing_df['time'], utc=True)
                
                # Объединяем
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Удаляем дубликаты по time + symbol
            combined_df.drop_duplicates(subset=['time', 'symbol'], keep='last', inplace=True)
            
            # Сортируем по времени
            combined_df.sort_values('time', inplace=True)
            
            # Сохраняем
            combined_df.to_parquet(self.real_file, engine='pyarrow', index=False)
            
            # Обновляем кэш
            self._existing_data = combined_df
            
            print(f"[RealtimeCollector] Сохранено {len(new_df)} записей в real.parquet")
            
        except Exception as e:
            print(f"[RealtimeCollector] Ошибка сохранения: {e}")
    
    def get_data(self, symbol: str = None, start_time: datetime = None, 
                 end_time: datetime = None) -> pd.DataFrame:
        """Возвращает данные с фильтрацией."""
        with self._lock:
            if self._existing_data.empty:
                return pd.DataFrame()
            
            df = self._existing_data.copy()
            
            if symbol:
                df = df[df['symbol'] == symbol]
            
            if start_time:
                df = df[df['time'] >= start_time]
            
            if end_time:
                df = df[df['time'] <= end_time]
            
            return df.sort_values('time')
    
    def get_latest_candle(self, symbol: str) -> Optional[pd.Series]:
        """Возвращает последнюю свечу для символа."""
        df = self.get_data(symbol=symbol)
        if df.empty:
            return None
        return df.iloc[-1]
    
    def get_stats(self) -> dict:
        """Возвращает статистику по собранным данным."""
        with self._lock:
            if self._existing_data.empty:
                return {
                    'total_records': 0,
                    'symbols': [],
                    'buffer_size': sum(len(df) for df in self._buffer),
                    'is_running': self._running
                }
            
            return {
                'total_records': len(self._existing_data),
                'symbols': self._existing_data['symbol'].unique().tolist(),
                'start_time': self._existing_data['time'].min(),
                'end_time': self._existing_data['time'].max(),
                'buffer_size': sum(len(df) for df in self._buffer),
                'records_by_symbol': self._existing_data['symbol'].value_counts().to_dict(),
                'is_running': self._running,
                'server_offset': self._server_offset
            }