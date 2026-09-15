# src/core/worker_mt5_task.py

import threading
import queue
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone
import pandas as pd
import time

from src.core.mt5_connection import MT5Connection
from src.core.workers.file_worker import FileTask



@dataclass
class MT5Task:
    """Тип задачи для MT5-воркера."""
    task_type: str  # 'connect', 'get_terminal_info', 'get_account_info',
                   # 'get_symbols', 'get_historical_data', 'get_current_bars'
    payload: Optional[Dict[str, Any]] = field(default_factory=dict)
    result_callback: Optional[callable] = None
    error_callback: Optional[callable] = None


class WorkerMT5:
    def __init__(self, mt5_connection: MT5Connection, settings: dict = None, 
                 file_worker=None, base_dir=None, output_file=None):
        print("[WorkerMT5.__init__] Инициализация WorkerMT5...")
        
        print("[WorkerMT5.__init__] получение MT5Connection...")
        self.mt5_conn = mt5_connection
        print("[WorkerMT5.__init__] получение settings...")
        self.settings = settings or {}
        print("[WorkerMT5.__init__] получение file_worker...")
        self.file_worker = file_worker
        print("[WorkerMT5.__init__] получение base_dir...")
        self.base_dir = base_dir
        print("[WorkerMT5.__init__] получение output_file...")
        self.output_file = output_file

        # --- Настройки мониторинга (из settings) ---
        system_settings = self.settings.get('system', {})
        self.monitor_interval = system_settings.get('connection_monitor_interval', 30)
        self.reconnection_delays = system_settings.get(
            'reconnection_delays', [0, 10, 30, 60, 300]
        )
        print(f"[WorkerMT5.__init__] monitor_interval = {self.monitor_interval}")
        print(f"[WorkerMT5.__init__] reconnection_delays = {self.reconnection_delays}")

    
        # --- Очередь задач ---
        self._queue: queue.Queue[MT5Task] = queue.Queue()
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

        # Состояние
        self.is_connected = False
        print(f"[WorkerMT5.__init__] is_connected = {self.is_connected}")
        self.system_running = True
        print(f"[WorkerMT5.__init__] system_running = {self.system_running}")

        # Флаг завершения инициализации (используется SystemOrchestrator.start())
        self._init_done = False
        print(f"[WorkerMT5.__init__] _init_done = {self._init_done}")

        # --- Мониторинг подключения ---
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitoring_active = False
        
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

        # 1. Останавливаем мониторинг ПЕРВЫМ (пока MT5 ещё подключён)
        self._stop_monitoring()

        # 2. Останавливаем основной цикл
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
        
        # 3. Отключаемся от MT5
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

        # Выполняем инициализацию с обработкой ошибок
        print("[WorkerMT5._run_loop] _initialize_system инициализируем систему ")
        try:
            self._initialize_system()
        except Exception as e:
            import traceback
            print(f"[WorkerMT5._run_loop] КРИТИЧЕСКАЯ ОШИБКА инициализации: {e}")
            traceback.print_exc()
            self.system_running = False
            self._is_running = False
            return

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

        elif t_type == "get_server_time":
            print("[WorkerMT5._execute_task] Получение серверного времени...")
            server_time = self.mt5_conn.get_server_time()
            print(f"[WorkerMT5._execute_task] Серверное время: {server_time}")

            if task.result_callback:
                task.result_callback({"server_time": server_time})

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

        elif t_type == "get_last_closed_bar":
            print("[WorkerMT5._execute_task] Получение последнего закрытого бара...")
            symbol = task.payload.get('symbol')
            timeframe = task.payload.get('timeframe')

            if not all([symbol, timeframe]):
                if task.error_callback:
                    task.error_callback(ValueError("Недостаточно параметров"))
                return

            df = self.mt5_conn.get_last_closed_bar(symbol, timeframe)

            if task.result_callback:
                task.result_callback({
                    "data": df,
                    "symbol": symbol
                })

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

    def _initialize_system(self):
        """
        Инициализация WorkerMT5 (3 шага):
          1. Подключение к MT5.
          2. Информация о терминале и счёте.
          3. Список символов + сравнение с symbols_list.json.

        Шаги 4–6 (история, real.csv, цикл сбора) перенесены в SystemOrchestrator:
          - _prepare_real_csv()  — подготовка real.csv и дозагрузка за сегодня
          - _start_collection_loop() — запуск потока сбора
        """
        print("[WorkerMT5._initialize_system] Начало инициализации...")

        # Шаг 1: Подключение к MT5
        print("1 [WorkerMT5._initialize_system] Шаг 1: Подключение к MT5")
        if not self._connect_to_mt5():
            print("[WorkerMT5._initialize_system] Ошибка подключения, инициализация прервана")
            self.system_running = False
            return

        # Шаг 2: Информация о терминале и счёте
        print("2 [WorkerMT5._initialize_system] Шаг 2: Получение информации")
        self._get_terminal_and_account_info()

        # Шаг 3: Список символов + сравнение
        print("3 [WorkerMT5._initialize_system] Шаг 3: Получение списка символов")
        self._get_symbols_list()

        # Сигнал оркестратору, что инициализация завершена
        self._init_done = True
        print("[WorkerMT5._initialize_system] Инициализация завершена (_init_done=True)")
        
    def _handle_symbols_comparison(self, res):
        """Обработка результата сравнения списка символов."""
        comparison = res.get('comparison', {})
        added = comparison.get('added', [])
        removed = comparison.get('removed', [])
        unchanged = comparison.get('unchanged', [])

        print("\n[WorkerMT5._handle_symbols_comparison] Результат сравнения символов:")
        print(f"  Было в symbols_list.json: {comparison.get('old_count', 0)}")
        print(f"  Пришло от MT5:            {comparison.get('new_count', 0)}")
        print(f"  Новых (added):            {len(added)}")
        print(f"  Пропали (removed):        {len(removed)}")
        print(f"  Без изменений:            {len(unchanged)}")

        if added:
            print(f"  +++ Новые символы: {added}")
        if removed:
            print(f"  --- Пропавшие символы: {removed}")
    

    def _connect_to_mt5(self) -> bool:
        """Подключение к MT5 + запуск мониторинга."""
        print("\n[WorkerMT5._connect_to_mt5] Подключение к MT5...")

        ok = self.mt5_conn.connect()

        if ok:
            self.is_connected = True
            self._start_monitoring()   # ← мониторинг запускает воркер
            print("[WorkerMT5._connect_to_mt5] Подключение успешно")
            # Прогрев истории для символов из settings.history.symbols
            self._warmup_history_symbols()
        else:
            print("[WorkerMT5._connect_to_mt5] Ошибка подключения")

        return ok

    def _warmup_history_symbols(self):
        """
        Прогревает историю для символов, которые будут собираться в real.csv.
        Без этого copy_rates_range может вернуть пусто для длинных диапазонов.
        """
        print("\n[WorkerMT5._warmup_history_symbols] Прогрев истории для символов из настроек...")

        history_settings = self.settings.get('history', {})
        symbols = history_settings.get('symbols', [])
        timeframe_str = history_settings.get('timeframe', 'M1')

        # Импортируем TIMEFRAME_MAPPING локально, чтобы не плодить зависимости
        from src.utils.file_checker import TIMEFRAME_MAPPING
        timeframe = TIMEFRAME_MAPPING.get(timeframe_str)
        if timeframe is None:
            print(f"[WorkerMT5._warmup_history_symbols] Неизвестный таймфрейм: {timeframe_str}")
            return

        print(f"[WorkerMT5._warmup_history_symbols] Символы: {symbols}")
        print(f"[WorkerMT5._warmup_history_symbols] Таймфрейм: {timeframe}")

        for symbol in symbols:
            self.mt5_conn.warmup_symbol(symbol, timeframe, count=5000)

        print("[WorkerMT5._warmup_history_symbols] Прогрев завершён")

    def _start_monitoring(self):
        """Запуск потока мониторинга подключения."""
        print("[WorkerMT5._start_monitoring] Запуск мониторинга подключения...")

        if self._monitor_thread and self._monitor_thread.is_alive():
            print("[WorkerMT5._start_monitoring] Мониторинг уже запущен, пропускаем")
            return

        self._monitoring_active = True
        self._monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self._monitor_thread.start()
        print(f"[WorkerMT5._start_monitoring] Поток мониторинга запущен с ID: "
              f"{self._monitor_thread.ident}")

    def _stop_monitoring(self):
        """Остановка потока мониторинга."""
        print("[WorkerMT5._stop_monitoring] Остановка мониторинга...")

        self._monitoring_active = False

        if self._monitor_thread:
            print(f"[WorkerMT5._stop_monitoring] Ожидание потока {self._monitor_thread.ident}...")
            self._monitor_thread.join(timeout=2)
            if self._monitor_thread.is_alive():
                print("[WorkerMT5._stop_monitoring] Поток не завершился за 2 секунды")
            else:
                print("[WorkerMT5._stop_monitoring] Поток мониторинга завершён")

        print("[WorkerMT5._stop_monitoring] Мониторинг остановлен")

    def _monitoring_loop(self):
        """Цикл мониторинга подключения."""
        print("[WorkerMT5._monitoring_loop] Запуск цикла мониторинга...")
        print(f"[WorkerMT5._monitoring_loop] Интервал проверки: {self.monitor_interval} секунд")

        while self._monitoring_active:
            # Спим маленькими порциями — чтобы быстро реагировать на stop
            slept = 0
            while slept < self.monitor_interval and self._monitoring_active:
                time.sleep(1)
                slept += 1

            if not self._monitoring_active:
                break

            print("[WorkerMT5._monitoring_loop] Проверка подключения...")
            if not self.mt5_conn.is_connected():
                print("[WorkerMT5._monitoring_loop] Обнаружено отключение от MT5!")
                self._reconnect_with_strategy()
            else:
                print("[WorkerMT5._monitoring_loop] Подключение активно")

        print("[WorkerMT5._monitoring_loop] Цикл мониторинга завершён")

    def _reconnect_with_strategy(self) -> bool:
        """Переподключение с использованием стратегии задержек."""
        print("[WorkerMT5._reconnect_with_strategy] Начало переподключения...")
        delays = self.reconnection_delays
        print(f"[WorkerMT5._reconnect_with_strategy] Задержки: {delays}")

        for attempt, delay in enumerate(delays, 1):
            if not self._monitoring_active:
                print("[WorkerMT5._reconnect_with_strategy] Остановка мониторинга — "
                      "прерываем переподключение")
                return False

            if delay > 0:
                print(f"[WorkerMT5._reconnect_with_strategy] Ожидание {delay} сек "
                      f"перед попыткой {attempt}...")
                # Дробим sleep, чтобы быстро реагировать на stop
                slept = 0
                while slept < delay and self._monitoring_active:
                    time.sleep(1)
                    slept += 1

                if not self._monitoring_active:
                    return False

            print(f"[WorkerMT5._reconnect_with_strategy] Попытка {attempt}/{len(delays)}...")
            if self.mt5_conn.connect():
                print("[WorkerMT5._reconnect_with_strategy] Переподключение успешно!")
                self.is_connected = True
                return True
            else:
                print(f"[WorkerMT5._reconnect_with_strategy] Попытка {attempt} не удалась")

        print("[WorkerMT5._reconnect_with_strategy] Все попытки исчерпаны")
        self.is_connected = False
        return False

    def _get_terminal_and_account_info(self):
        print("\n[WorkerMT5._get_terminal_and_account_info] Получение информации...")
        terminal_info = self.mt5_conn.get_terminal_info()
        account_info = self.mt5_conn.get_account_info()
        if terminal_info:
            print(f"Терминал: {terminal_info}")
        if account_info:
            print(f"Счет: {account_info}")

    def _get_symbols_list(self):
        print("\n[WorkerMT5._get_symbols_list] Получение списка символов...")

        symbols = self.mt5_conn.symbols_get()
        if not symbols:
            print("[WorkerMT5._get_symbols_list] Список символов пуст")
            return

        symbol_names = [s.name for s in symbols]
        print(f"[WorkerMT5._get_symbols_list] Получено символов: {len(symbol_names)}")

        self.file_worker.submit(FileTask(
            task_type="compare_symbols",
            payload={"symbols": symbol_names},
            result_callback=self._handle_symbols_comparison,
            error_callback=lambda e: print(f"[WorkerMT5._get_symbols_list] Ошибка: {e}")
        ))

