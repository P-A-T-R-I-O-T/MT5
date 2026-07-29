# src/core/realtime_collector.py

import time
import threading
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import MetaTrader5 as mt5
from src.utils.json_manager import JsonManager

class RealtimeCollector:
    def __init__(self, mt5_conn, json_manager: JsonManager):
        self.mt5_conn = mt5_conn
        self.json_manager = json_manager
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._symbol = None
        self._timeframe = mt5.TIMEFRAME_M1
        self._output_file = Path('F:/MT5-soft/history') / 'real.csv'
        self._base_dir = Path('F:/MT5-soft/history')
        
        # Настройки
        settings = json_manager.load_settings()
        history_settings = settings.get('history', {})
        self._symbol = history_settings.get('symbols', ['GBPUSD'])[0]
        timeframe_str = history_settings.get('timeframe', 'M1')
        self._timeframe = self._get_timeframe(timeframe_str)
                
        # Проверяем и подготавливаем файл
        self._prepare_initial_data()

    def _prepare_initial_data(self):
        """Подготовка данных при инициализации"""
        from src.utils.history_manager import HistoryManager
        
        # Проверяем и подготавливаем real.csv
        is_valid = HistoryManager.check_or_create_output_file(self._output_file)
        
        if not is_valid:
            print("[WARN] Файл real.csv признан битым. Удаляю и создаю заново.")
            try:
                self._output_file.unlink()
            except:
                pass
            HistoryManager.check_or_create_output_file(self._output_file)

    def start(self):
        if self._running:
            return
        
        if not self._symbol:
            return
        
        if not self.mt5_conn._is_connected:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()
        print(f"[RealtimeCollector] Запущен для {self._symbol}")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[RealtimeCollector] Остановлен")

    def _collect_loop(self):
        """Основной цикл сбора данных"""
        while self._running:
            utc_now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            
            # Проверяем активность торговой сессии
            if not self._is_trading_active(utc_now):
                print(f"[{utc_now.strftime('%H:%M:%S')}] Торговая сессия неактивна. Ждём 1 час...")
                time.sleep(3600)
                continue
            
            # Синхронизируемся на начало следующей минуты + 1 секунда
            self._wait_until_next_minute_utc(delay_after_minute_start=1.0)
            
            # Получаем синхронизированное время
            utc_now_synced = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            
            # Определяем диапазон для догрузки
            last_time = self._get_last_recorded_time()
            if last_time is None:
                last_time = utc_now_synced.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(minutes=1)
            
            start_time = last_time + timedelta(minutes=1)
            end_time = utc_now_synced
            
            if start_time > end_time:
                time.sleep(10)
                continue
            
            # Получаем данные
            df = self._get_latest_closed_bars(self._symbol, self._timeframe, start_time, end_time)
            
            if df is None or df.empty:
                continue
            
            # Сохраняем в CSV
            self._save_bars_to_csv(df)
            
            min_time = df['time'].min().strftime('%H:%M')
            max_time = df['time'].max().strftime('%H:%M')
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Записаны новые бары: {min_time} - {max_time} ({len(df)} шт.)")

    def _wait_until_next_minute_utc(self, delay_after_minute_start=1.0):
        """Ждёт до начала следующей минуты + задержка"""
        now = datetime.now(timezone.utc)
        current_minute_start = now.replace(second=0, microsecond=0)
        next_minute_start = current_minute_start + timedelta(minutes=1)
        target_time = next_minute_start + timedelta(seconds=delay_after_minute_start)
        
        wait_seconds = (target_time - now).total_seconds()
        
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _get_latest_closed_bars(self, symbol, timeframe, start_time, end_time):
        """Получает закрытые бары"""
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

    def _save_bars_to_csv(self, df: pd.DataFrame) -> bool:
        """Сохраняет бары в CSV с форматом DD-MM-YYYY"""
        if df.empty:
            return False
        
        # Конвертируем время в формат DD-MM-YYYY HH:MM:SS
        df_display = df.copy()
        df_display['time'] = df_display['time'].dt.strftime('%d-%m-%Y %H:%M:%S')
        
        file_exists = self._output_file.exists()
        df_display.to_csv(
            self._output_file,
            mode='a',
            index=False,
            header=not file_exists
        )
        return True

    def _get_last_recorded_time(self):
        """Возвращает время последней записи из real.csv"""
        if not self._output_file.exists():
            return None
        
        try:
            df = pd.read_csv(self._output_file)
            if df.empty:
                return None
            
            if 'time' not in df.columns:
                return None
            
            # Парсим время в формате DD-MM-YYYY HH:MM:SS
            df['time'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', utc=True)
            last_time = df['time'].iloc[-1]
            return last_time.replace(second=0, microsecond=0)
        except Exception:
            return None

    def _is_trading_active(self, utc_now: datetime) -> bool:
        """Проверяет активность торговой сессии"""
        weekday = utc_now.weekday()
        return weekday < 5  # Пн-Пт

    def _get_timeframe(self, timeframe_str: str) -> int:
        """Преобразует строку таймфрейма в константу MT5"""
        mapping = {
            'M1': mt5.TIMEFRAME_M1,
            'M5': mt5.TIMEFRAME_M5,
            'M15': mt5.TIMEFRAME_M15,
            'H1': mt5.TIMEFRAME_H1,
            'D1': mt5.TIMEFRAME_D1
        }
        return mapping.get(timeframe_str, mt5.TIMEFRAME_M1)

    def get_stats(self) -> dict:
        """Возвращает статистику"""
        return {
            'symbol': self._symbol,
            'is_running': self._running,
            'output_file': str(self._output_file)
        }