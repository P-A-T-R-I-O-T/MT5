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
    """Класс для сбора данных в реальном времени с правильной синхронизацией."""
    
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
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()
        print(f"[RealtimeCollector] Запущен для {len(self._symbols)} символов")
    
    def stop(self):
        """Останавливает сбор данных и сохраняет буфер."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        # Сохраняем остатки буфера
        self._flush_buffer()
        print("[RealtimeCollector] Остановлен")
    
    def _collect_loop(self):
        """Основной цикл сбора данных с синхронизацией по UTC."""
        # Ждем начала следующей минуты для синхронизации
        self._sync_to_next_minute()
        
        while self._running:
            try:
                # Проверяем подключение
                if not self._check_connection():
                    time.sleep(30)
                    continue
                
                # Собираем данные за последние минуты с запасом
                self._collect_recent_data()
                
                # Сохраняем буфер при необходимости
                self._check_and_save_buffer()
                
                # Ждем до следующей минуты
                self._wait_until_next_minute()
                
            except Exception as e:
                print(f"[RealtimeCollector] Ошибка в цикле сбора: {e}")
                time.sleep(10)
    
    def _sync_to_next_minute(self):
        """Синхронизирует начало работы с началом следующей минуты."""
        now = datetime.utcnow()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_seconds = (next_minute - now).total_seconds()
        if wait_seconds > 0:
            print(f"[RealtimeCollector] Синхронизация: ожидание {wait_seconds:.1f} секунд")
            time.sleep(wait_seconds)
    
    def _wait_until_next_minute(self):
        """Ожидает до начала следующей минуты."""
        now = datetime.utcnow()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_seconds = (next_minute - now).total_seconds()
        if wait_seconds > 0:
            time.sleep(wait_seconds)
    
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
    
    def _collect_recent_data(self):
        """Собирает свечи за последние несколько минут с запасом."""
        now = datetime.utcnow()
        
        # Берем с запасом в 5 минут, чтобы не пропустить данные
        start_time = now - timedelta(minutes=5)
        end_time = now
        
        all_new_data = []
        
        for symbol in self._symbols:
            try:
                # Получаем данные с запасом
                rates = mt5.copy_rates_range(
                    symbol,
                    self._timeframe,
                    start_time,
                    end_time
                )
                
                if rates is None or len(rates) == 0:
                    continue
                
                # Конвертируем в DataFrame
                df = pd.DataFrame(rates)
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                df['symbol'] = symbol
                
                # Фильтруем только новые данные
                last_time = self._last_times.get(symbol)
                if last_time:
                    df = df[df['time'] > last_time]
                
                if not df.empty:
                    all_new_data.append(df)
                    self._last_times[symbol] = df['time'].max()
                    print(f"[RealtimeCollector] {symbol}: получено {len(df)} новых свечей")
                    
            except Exception as e:
                print(f"[RealtimeCollector] Ошибка для {symbol}: {e}")
        
        # Добавляем в буфер
        if all_new_data:
            with self._lock:
                self._buffer.extend(all_new_data)
    
    def _check_and_save_buffer(self):
        """Проверяет буфер и сохраняет при необходимости."""
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
                    'buffer_size': sum(len(df) for df in self._buffer)
                }
            
            return {
                'total_records': len(self._existing_data),
                'symbols': self._existing_data['symbol'].unique().tolist(),
                'start_time': self._existing_data['time'].min(),
                'end_time': self._existing_data['time'].max(),
                'buffer_size': sum(len(df) for df in self._buffer),
                'records_by_symbol': self._existing_data['symbol'].value_counts().to_dict()
            }