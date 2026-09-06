# src/core/worker_mt5_task.py

import threading
import queue
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone
import pandas as pd

from src.core.mt5_connection import MT5Connection
from src.utils.json_manager import JsonManager


@dataclass
class MT5Task:
    """Тип задачи для MT5-воркера."""
    task_type: str  # 'connect', 'get_terminal_info', 'get_account_info',
                   # 'get_symbols', 'get_historical_data', 'get_current_bars'
    payload: Optional[Dict[str, Any]] = field(default_factory=dict)
    result_callback: Optional[callable] = None
    error_callback: Optional[callable] = None


class WorkerMT5:
    def __init__(self, mt5_connection: MT5Connection):
        print("[WorkerMT5.__init__] Инициализация WorkerMT5...")
        
        print("[WorkerMT5.__init__] получение MT5Connection...")
        self.mt5_conn = MT5Connection()
        print("[WorkerMT5.__init__] MT5Connection получен")
        
        self._queue: queue.Queue[MT5Task] = queue.Queue()
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._is_running = False
        
        print("[WorkerMT5.__init__] Инициализация завершена")

    def start(self):
        print("[WorkerMT5.start] Запуск WorkerMT5...")
        
        if self._is_running:
            print("[WorkerMT5.start] WorkerMT5 уже запущен, пропускаем")
            return
            
        self._stop_flag = False
        self._is_running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"[WorkerMT5.start] Поток запущен с ID: {self._thread.ident}")
        print("[WorkerMT5.start] WorkerMT5 запущен")

    def stop(self):
        """Корректная остановка с освобождением всех ресурсов."""
        print("[WorkerMT5.stop] Начало остановки...")
        self._stop_flag = True
        self._is_running = False
        print("[WorkerMT5.stop] Флаги остановки установлены")
        
        # Ожидаем завершения потока
        if self._thread:
            print(f"[WorkerMT5.stop] Ожидание завершения потока {self._thread.ident}...")
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                print("[WorkerMT5.stop] Поток не завершился, продолжаем...")
            else:
                print("[WorkerMT5.stop] Поток успешно завершен")
        
        # Отключаемся от MT5
        print("[WorkerMT5.stop] Отключение от MT5...")
        self.mt5_conn.disconnect()
        
        print("[WorkerMT5.stop] WorkerMT5 остановлен")

    def submit(self, task: MT5Task):
        """Добавляет задачу в очередь."""
        print(f"[WorkerMT5.submit] Добавление задачи: {task.task_type}")
        print(f"[WorkerMT5.submit] Payload: {task.payload}")
        self._queue.put(task)
        print(f"[WorkerMT5.submit] Задача добавлена. Размер очереди: {self._queue.qsize()}")

    def _run_loop(self):
        """Основной цикл обработки задач."""
        print("[WorkerMT5._run_loop] Запуск цикла обработки задач...")
        processed_tasks = 0
        
        while not self._stop_flag:
            try:
                task = self._queue.get(timeout=0.5)
                print(f"[WorkerMT5._run_loop] Получена задача: {task.task_type}")
                
            except queue.Empty:
                continue

            try:
                # Проверяем флаг остановки перед выполнением задачи
                if self._stop_flag:
                    print("[WorkerMT5._run_loop] Пропуск задачи из-за флага остановки")
                    break
                    
                print(f"[WorkerMT5._run_loop] Выполнение задачи: {task.task_type}")
                self._execute_task(task)
                processed_tasks += 1
                print(f"[WorkerMT5._run_loop] Задача {task.task_type} выполнена успешно")
                
            except Exception as e:
                print(f"[WorkerMT5._run_loop] Ошибка при выполнении задачи {task.task_type}: {e}")
                if task.error_callback:
                    print("[WorkerMT5._run_loop] Вызов error_callback")
                    task.error_callback(e)
                else:
                    print(f"[WorkerMT5._run_loop] Необработанная ошибка задачи: {e}")
            finally:
                # Вызываем task_done() только если задача была обработана
                if not self._stop_flag:
                    self._queue.task_done()
                    print(f"[WorkerMT5._run_loop] Задача {task.task_type} обработана")
                else:
                    print("[WorkerMT5._run_loop] Задача не обработана из-за остановки")
        
        # Очищаем очередь при выходе
        print("[WorkerMT5._run_loop] Очистка очереди...")
        cleared_tasks = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                cleared_tasks += 1
            except queue.Empty:
                break
        
        print(f"[WorkerMT5._run_loop] Очищено задач: {cleared_tasks}")
        print(f"[WorkerMT5._run_loop] Цикл обработки задач завершен. Всего обработано: {processed_tasks}")

    def _execute_task(self, task: MT5Task):
        """Выполняет конкретную задачу."""
        t_type = task.task_type
        print(f"[WorkerMT5._execute_task] Начало выполнения задачи: {t_type}")
        print(f"[WorkerMT5._execute_task] Payload: {task.payload}")

        if t_type == "connect":
            print("[WorkerMT5._execute_task] Подключение к MT5...")
            ok = self.mt5_conn.connect()
            print(f"[WorkerMT5._execute_task] Результат подключения: {ok}")
            
            if ok:
                print("[WorkerMT5._execute_task] Подключение к MT5 успешно.")
                # Запускаем мониторинг
                print("[WorkerMT5._execute_task] Запуск мониторинга подключения...")
                self.mt5_conn._start_connection_monitor()
                print("[WorkerMT5._execute_task] Мониторинг запущен")
            else:
                print("[WorkerMT5._execute_task] Ошибка подключения к MT5.")
                if task.error_callback:
                    print("[WorkerMT5._execute_task] Вызов error_callback")
                    task.error_callback(RuntimeError("Не удалось подключиться к MT5"))
            
            if task.result_callback:
                print("[WorkerMT5._execute_task] Вызов result_callback")
                task.result_callback({"connected": ok})
                print("[WorkerMT5._execute_task] result_callback выполнен")

        elif t_type == "get_terminal_info":
            print("[WorkerMT5._execute_task] Получение информации о терминале...")
            info = self.mt5_conn.get_terminal_info()
            print(f"[WorkerMT5._execute_task] Получена информация: {info}")
            
            if task.result_callback:
                print("[WorkerMT5._execute_task] Вызов result_callback")
                task.result_callback({"terminal_info": info})
                print("[WorkerMT5._execute_task] result_callback выполнен")

        elif t_type == "get_account_info":
            print("[WorkerMT5._execute_task] Получение информации о счете...")
            acc = self.mt5_conn.get_account_info()
            print(f"[WorkerMT5._execute_task] Получена информация: {acc}")
            
            if task.result_callback:
                print("[WorkerMT5._execute_task] Вызов result_callback")
                task.result_callback({"account_info": acc})
                print("[WorkerMT5._execute_task] result_callback выполнен")

        elif t_type == "get_symbols":
            print("[WorkerMT5._execute_task] Получение списка символов...")
            symbols = self.mt5_conn.symbols_get()
            print(f"[WorkerMT5._execute_task] Получено символов: {len(symbols) if symbols else 0}")
            
            if task.result_callback:
                print("[WorkerMT5._execute_task] Вызов result_callback")
                task.result_callback({"symbols": symbols})
                print("[WorkerMT5._execute_task] result_callback выполнен")

        elif t_type == "get_historical_data":
            print("[WorkerMT5._execute_task] Получение исторических данных...")
            # Получение исторических данных
            symbol = task.payload.get('symbol')
            timeframe = task.payload.get('timeframe')
            start_date = task.payload.get('start_date')
            end_date = task.payload.get('end_date')
            
            print(f"[WorkerMT5._execute_task] Символ: {symbol}")
            print(f"[WorkerMT5._execute_task] Таймфрейм: {timeframe}")
            print(f"[WorkerMT5._execute_task] Начало: {start_date}")
            print(f"[WorkerMT5._execute_task] Конец: {end_date}")
            
            if not all([symbol, timeframe, start_date, end_date]):
                print("[WorkerMT5._execute_task] Ошибка: недостаточно параметров")
                if task.error_callback:
                    task.error_callback(ValueError("Недостаточно параметров для получения исторических данных"))
                return
            
            # Получаем данные через mt5_conn
            print("[WorkerMT5._execute_task] Вызов mt5_conn.get_historical_data...")
            df = self.mt5_conn.get_historical_data(symbol, timeframe, start_date, end_date)
            print(f"[WorkerMT5._execute_task] Получен DataFrame: {df.shape if df is not None else 'None'}")
            
            if task.result_callback:
                print("[WorkerMT5._execute_task] Вызов result_callback")
                task.result_callback({
                    "data": df,
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date
                })
                print("[WorkerMT5._execute_task] result_callback выполнен")

        elif t_type == "get_current_bars":
            print("[WorkerMT5._execute_task] Получение текущих баров...")
            # Получение баров за текущий день
            symbols = task.payload.get('symbols', [])
            timeframe = task.payload.get('timeframe')
            
            print(f"[WorkerMT5._execute_task] Символы: {symbols}")
            print(f"[WorkerMT5._execute_task] Таймфрейм: {timeframe}")
            
            if not symbols or not timeframe:
                print("[WorkerMT5._execute_task] Ошибка: недостаточно параметров")
                if task.error_callback:
                    task.error_callback(ValueError("Недостаточно параметров для получения текущих баров"))
                return
            
            # Определяем начало текущего дня
            utc_now = datetime.now(timezone.utc)
            day_start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
            print(f"[WorkerMT5._execute_task] Текущее время UTC: {utc_now}")
            print(f"[WorkerMT5._execute_task] Начало дня: {day_start}")
            
            all_data = []
            for symbol in symbols:
                print(f"[WorkerMT5._execute_task] Получение данных для {symbol}...")
                df = self.mt5_conn.get_historical_data(
                    symbol, 
                    timeframe, 
                    day_start, 
                    utc_now
                )
                if df is not None and not df.empty:
                    print(f"[WorkerMT5._execute_task] {symbol}: получено {len(df)} записей")
                    df['symbol'] = symbol
                    all_data.append(df)
                else:
                    print(f"[WorkerMT5._execute_task] {symbol}: данные не получены")
            
            print(f"[WorkerMT5._execute_task] Всего собрано DataFrame: {len(all_data)}")
            
            if task.result_callback:
                if all_data:
                    print("[WorkerMT5._execute_task] Объединение данных...")
                    combined_df = pd.concat(all_data, ignore_index=True)
                    combined_df = combined_df.sort_values(['time', 'symbol'])
                    print(f"[WorkerMT5._execute_task] Объединенный DataFrame: {combined_df.shape}")
                    
                    print("[WorkerMT5._execute_task] Вызов result_callback")
                    task.result_callback({"data": combined_df})
                else:
                    print("[WorkerMT5._execute_task] Нет данных для объединения")
                    print("[WorkerMT5._execute_task] Вызов result_callback с None")
                    task.result_callback({"data": None})
                print("[WorkerMT5._execute_task] result_callback выполнен")
        
        else:
            msg = f"[WorkerMT5._execute_task] Неизвестный тип задачи: {t_type}"
            print(msg)
            if task.error_callback:
                print("[WorkerMT5._execute_task] Вызов error_callback")
                task.error_callback(ValueError(msg))
                print("[WorkerMT5._execute_task] error_callback выполнен")
        
        print(f"[WorkerMT5._execute_task] Завершение выполнения задачи: {t_type}")