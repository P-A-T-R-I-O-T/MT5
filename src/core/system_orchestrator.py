# src/core/system_orchestrator.py

import threading
import time
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd

from src.core.mt5_connection import MT5Connection
from src.core.workers.worker_mt5 import WorkerMT5, MT5Task
from src.core.workers.file_worker import FileWorker, FileTask
from src.utils.history_manager import HistoryManager
from src.utils.symbol_utils import group_and_display_symbols


class SystemOrchestrator:
    """
    Оркестратор системы.
    Управляет двумя воркерами и координирует их работу.
    """
    
    def __init__(self):
        print("1 [SystemOrchestrator.__init__] Инициализация SystemOrchestrator...")

        print("2 [SystemOrchestrator.__init__] создание FileWorker")
        self.file_worker = FileWorker() 
        print("2.1 [SystemOrchestrator.__init__] FileWorker создан")

        # 2. Запускаем поток FileWorker
        print("3 [SystemOrchestrator.__init__] Запуск FileWorker...")
        self.file_worker.start()
        print("3.1 [SystemOrchestrator.__init__] FileWorker запущен")

        # 3. Ждем готовности JsonManager
        print("4 [SystemOrchestrator.__init__] Ожидание готовности JsonManager...")
        if not self.file_worker.wait_for_json_manager():
            print("[SystemOrchestrator.__init__] Ошибка: JsonManager не готов")
            raise RuntimeError("JsonManager не инициализирован")
        print("5 [SystemOrchestrator.__init__] JsonManager готов")
        
        # 4. Загружаем настройки ЧЕРЕЗ FileWorker (асинхронно)
        print("6 [SystemOrchestrator.__init__] Загрузка настроек через FileWorker...")
        self.settings = self._load_data_via_file_worker(
            task_type='load_settings',
            result_key='settings'
        )
        print(f"6.1 [SystemOrchestrator.__init__] Настройки загружены: {self.settings}")

        # Загружаем credentials через FileWorker
        print("7 [SystemOrchestrator.__init__] Загрузка credentials через FileWorker...")
        self.credentials = self._load_data_via_file_worker(
            task_type='load_credentials',
            result_key='credentials'
        )
        print(f"7.1 [SystemOrchestrator.__init__] Настройки загружены: {self.credentials}")


        # Пути
        self.base_dir = pathlib.Path('F:/MT5-soft/history')
        self.output_file = self.base_dir / 'real.csv'
        print(f"8 [SystemOrchestrator.__init__] Базовая директория: {self.base_dir}")
        print(f"9 [SystemOrchestrator.__init__] Выходной файл: {self.output_file}")
        print("\n" *5)

        # Получаем данные из кеша FileWorker Прямой доступ к json_manager (безопасно, т.к. FileWorker готов)
        print("[SystemOrchestrator.__init__] Получение данных из кеша...")
        json_manager = self.file_worker.json_manager
        
        # Загружаем credentials из кеша
        credentials = json_manager.load_credentials()
        print(f"[SystemOrchestrator.__init__] Credentials загружены")
        
        # Получаем системные настройки из уже загруженных данных
        system_settings = self.settings.get('system', {})
        monitor_interval = system_settings.get('connection_monitor_interval', 30)
        reconnection_delays = system_settings.get('reconnection_delays', [0, 10, 30, 60, 300])
        
        print(f"[SystemOrchestrator.__init__] Monitor interval: {monitor_interval}")
        print(f"[SystemOrchestrator.__init__] Reconnection delays: {reconnection_delays}")
        
        # Создаем MT5Connection
        print("[SystemOrchestrator.__init__] Создание MT5Connection...")
        mt5_connection = MT5Connection(
            credentials=credentials,
            monitor_interval=monitor_interval,
            reconnection_delays=reconnection_delays
        )
        print("[SystemOrchestrator.__init__] MT5Connection создан")
        print("\n" *5)



        # Воркеры
        print("[SystemOrchestrator.__init__] Создание WorkerMT5...")
        self.r_mt5 = WorkerMT5()
        print("[SystemOrchestrator.__init__] WorkerMT5 создан")

        
        # Состояние
        self.system_running = True
        self.is_connected = False
        self.collection_thread: Optional[threading.Thread] = None
        
        # События для синхронизации
        self._settings_checked = threading.Event()
        self._connected = threading.Event()
        
        print("[SystemOrchestrator.__init__] Инициализация завершена")
        
    def start(self):
        """Запуск системы."""
        print("\n[SystemOrchestrator.start] Запуск системы...")

        print("[SystemOrchestrator.start] Запуск FileWorker...")
        self.file_worker.start()
        print("[SystemOrchestrator.start] FileWorker запущен")
        
        # Запускаем воркеры
        print("[SystemOrchestrator.start] Запуск WorkerMT5...")
        self.mt5_worker.start()
        print("[SystemOrchestrator.start] WorkerMT5 запущен")
        
        # Запускаем основной цикл в отдельном потоке
        print("[SystemOrchestrator.start] Запуск основного цикла в отдельном потоке...")
        self.main_thread = threading.Thread(target=self._run_system, daemon=True)
        self.main_thread.start()
        print(f"[SystemOrchestrator.start] Основной поток запущен с ID: {self.main_thread.ident}")
        
        # Запускаем поток ввода команд
        print("[SystemOrchestrator.start] Запуск потока ввода команд...")
        self.input_thread = threading.Thread(
            target=self._user_input_loop,
            daemon=True
        )
        self.input_thread.start()
        print(f"[SystemOrchestrator.start] Поток ввода команд запущен с ID: {self.input_thread.ident}")
        
    def stop(self):
        """Остановка системы."""
        print("\n[SystemOrchestrator.stop] Остановка системы...")
        
        self.system_running = False
        print("[SystemOrchestrator.stop] Флаг system_running установлен в False")
        
        # Останавливаем цикл сбора
        if self.collection_thread and self.collection_thread.is_alive():
            print("[SystemOrchestrator.stop] Остановка цикла сбора...")
            self.collection_thread.join(timeout=5)
            if self.collection_thread.is_alive():
                print("[SystemOrchestrator.stop] Цикл сбора не завершился за 5 секунд")
            else:
                print("[SystemOrchestrator.stop] Цикл сбора завершен")
        
        # Останавливаем воркеры
        print("[SystemOrchestrator.stop] Остановка WorkerMT5...")
        self.mt5_worker.stop()
        print("[SystemOrchestrator.stop] WorkerMT5 остановлен")
        
        print("[SystemOrchestrator.stop] Остановка FileWorker...")
        self.file_worker.stop()
        print("[SystemOrchestrator.stop] FileWorker остановлен")
        
        print("[SystemOrchestrator.stop] Система остановлена")
        
    def _run_system(self):
        """Основной цикл работы системы."""
        print("\n[SystemOrchestrator._run_system] Запуск основного цикла...")
        
        # Шаг 1: Проверка настроек
        print("[SystemOrchestrator._run_system] Переход к шагу 1: Проверка настроек")
        if not self._check_settings():
            print("[SystemOrchestrator._run_system] Ошибка на шаге 1, остановка")
            self.system_running = False
            return
        
        # Шаг 2: Подключение к MT5
        print("[SystemOrchestrator._run_system] Переход к шагу 2: Подключение к MT5")
        if not self._connect_to_mt5():
            print("[SystemOrchestrator._run_system] Ошибка на шаге 2, остановка")
            self.system_running = False
            return
        
        # Шаг 3: Получение информации
        print("[SystemOrchestrator._run_system] Переход к шагу 3: Получение информации")
        self._get_terminal_and_account_info()
        
        # Шаг 4: Получение списка символов
        print("[SystemOrchestrator._run_system] Переход к шагу 4: Получение списка символов")
        self._get_symbols_list()
        
        # Шаг 5: Загрузка исторических данных
        print("[SystemOrchestrator._run_system] Переход к шагу 5: Загрузка исторических данных")
        self._load_historical_data()
        
        # Шаг 6: Подготовка real.csv
        print("[SystemOrchestrator._run_system] Переход к шагу 6: Подготовка real.csv")
        self._prepare_real_csv()
        
        # Шаг 7: Запуск цикла сбора данных
        print("[SystemOrchestrator._run_system] Переход к шагу 7: Запуск цикла сбора")
        self._start_collection_loop()
        
        # Ждем завершения работы
        print("[SystemOrchestrator._run_system] Ожидание завершения работы...")
        while self.system_running:
            time.sleep(1)
            
    def _check_settings(self) -> bool:
        """Проверка настроек через FileWorker."""
        print("\n[SystemOrchestrator._check_settings] Шаг 1: Проверка настроек...")
        
        result = {}
        settings_checked = threading.Event()
        
        def on_settings_checked(res):
            print(f"[SystemOrchestrator._check_settings] Получен результат: {res}")
            result.update(res)
            settings_checked.set()
        
        print("[SystemOrchestrator._check_settings] Отправка задачи в FileWorker...")
        self.file_worker.submit(FileTask(
            task_type="check_settings",
            payload={"settings": self.settings},
            result_callback=on_settings_checked,
            error_callback=lambda e: self._handle_error("проверки настроек", e, settings_checked)
        ))
        
        # Ждем результат (максимум 10 секунд)
        print("[SystemOrchestrator._check_settings] Ожидание результата (максимум 10 секунд)...")
        if not settings_checked.wait(timeout=10):
            print("[SystemOrchestrator._check_settings] Таймаут при проверке настроек")
            return False
        
        valid = result.get('valid', False)
        print(f"[SystemOrchestrator._check_settings] Результат валидации: {valid}")
        
        if valid:
            print("[SystemOrchestrator._check_settings] Настройки корректны")
        else:
            errors = result.get('errors', [])
            print("[SystemOrchestrator._check_settings] Найдены ошибки:")
            for error in errors:
                print(f"  - {error}")
        
        return valid


    def _connect_to_mt5(self) -> bool:
        """Подключение к MT5."""
        print("\n[SystemOrchestrator._connect_to_mt5] Шаг 2: Подключение к MT5...")
        
        result = {}
        connect_event = threading.Event()
        
        def on_connect(res):
            print(f"[SystemOrchestrator._connect_to_mt5] Получен результат: {res}")
            result.update(res)
            connect_event.set()
        
        print("[SystemOrchestrator._connect_to_mt5] Отправка задачи подключения...")
        self.mt5_worker.submit(MT5Task(
            task_type="connect",
            result_callback=on_connect,
            error_callback=lambda e: self._handle_error("подключения к MT5", e, connect_event)
        ))
        
        # Ждем результат (максимум 30 секунд)
        print("[SystemOrchestrator._connect_to_mt5] Ожидание результата (максимум 30 секунд)...")
        if not connect_event.wait(timeout=30):
            print("[SystemOrchestrator._connect_to_mt5] Таймаут при подключении к MT5")
            return False
        
        self.is_connected = result.get('connected', False)
        print(f"[SystemOrchestrator._connect_to_mt5] Статус подключения: {self.is_connected}")
        
        if self.is_connected:
            print("[SystemOrchestrator._connect_to_mt5] Подключение к MT5 успешно")
        else:
            print("[SystemOrchestrator._connect_to_mt5] Не удалось подключиться к MT5")
        
        return self.is_connected


    def _handle_error(self, operation: str, error: Exception, event: threading.Event):
        """Обработка ошибок с установкой события."""
        print(f"[SystemOrchestrator._handle_error] Ошибка {operation}: {error}")
        event.set()
        
    def _get_terminal_and_account_info(self):
        """Получение информации о терминале и счете."""
        print("\n[SystemOrchestrator._get_terminal_and_account_info] Шаг 3: Получение информации...")
        
        if self.display_settings.get('terminal_info', True):
            print("[SystemOrchestrator._get_terminal_and_account_info] Запрос информации о терминале...")
            self.mt5_worker.submit(MT5Task(
                task_type="get_terminal_info",
                result_callback=self._handle_terminal_info,
                error_callback=lambda e: print(f"[SystemOrchestrator._get_terminal_and_account_info] Ошибка: {e}")
            ))
        
        if self.display_settings.get('account_info', True):
            print("[SystemOrchestrator._get_terminal_and_account_info] Запрос информации о счете...")
            self.mt5_worker.submit(MT5Task(
                task_type="get_account_info",
                result_callback=self._handle_account_info,
                error_callback=lambda e: print(f"[SystemOrchestrator._get_terminal_and_account_info] Ошибка: {e}")
            ))
            
    def _get_symbols_list(self):
        """Получение списка символов."""
        print("\n[SystemOrchestrator._get_symbols_list] Шаг 4: Получение списка символов...")
        
        if self.display_settings.get('all_symbols', True):
            print("[SystemOrchestrator._get_symbols_list] Запрос списка символов...")
            self.mt5_worker.submit(MT5Task(
                task_type="get_symbols",
                result_callback=self._handle_symbols,
                error_callback=lambda e: print(f"[SystemOrchestrator._get_symbols_list] Ошибка: {e}")
            ))
        else:
            print("[SystemOrchestrator._get_symbols_list] Получение списка символов отключено в настройках")
            
    def _load_historical_data(self):
        """Загрузка исторических данных."""
        print("\n[SystemOrchestrator._load_historical_data] Шаг 5: Загрузка исторических данных...")
        
        if not self.history_settings.get('enabled', False):
            print("[SystemOrchestrator._load_historical_data] Загрузка истории отключена")
            return
        
        symbols = self.history_settings.get('symbols', [])
        timeframe_str = self.history_settings.get('timeframe', 'M1')
        start_date_str = self.history_settings.get('start_date', '')
        
        print(f"[SystemOrchestrator._load_historical_data] Символы: {symbols}")
        print(f"[SystemOrchestrator._load_historical_data] Таймфрейм: {timeframe_str}")
        print(f"[SystemOrchestrator._load_historical_data] Дата начала: {start_date_str}")
        
        if not symbols or not start_date_str:
            print("[SystemOrchestrator._load_historical_data] Нет символов или даты начала")
            return
        
        timeframe = HistoryManager.TIMEFRAME_MAPPING[timeframe_str]
        start_date = self.json_manager.parse_utc_date_string(start_date_str)
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        
        print(f"[SystemOrchestrator._load_historical_data] Таймфрейм MT5: {timeframe}")
        print(f"[SystemOrchestrator._load_historical_data] Дата начала: {start_date}")
        print(f"[SystemOrchestrator._load_historical_data] Сегодня: {today}")
        print(f"[SystemOrchestrator._load_historical_data] Вчера: {yesterday}")
        
        current_date = start_date.date()
        days_count = 0
        while current_date <= yesterday:
            day_start = datetime.combine(current_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            day_end = datetime.combine(current_date, datetime.max.time()).replace(tzinfo=timezone.utc)
            
            print(f"[SystemOrchestrator._load_historical_data] Загрузка данных за {current_date}...")
            # Собираем данные по всем символам за день
            self._load_day_data(symbols, timeframe, day_start, day_end, current_date)
            
            current_date += timedelta(days=1)
            days_count += 1
            
        print(f"[SystemOrchestrator._load_historical_data] Всего дней для загрузки: {days_count}")
            
    def _load_day_data(self, symbols, timeframe, start_time, end_time, date):
        """Загрузка данных за конкретный день."""
        print(f"[SystemOrchestrator._load_day_data] Загрузка данных за {date}...")
        print(f"[SystemOrchestrator._load_day_data] Символы: {symbols}")
        print(f"[SystemOrchestrator._load_day_data] Начало: {start_time}")
        print(f"[SystemOrchestrator._load_day_data] Конец: {end_time}")
        
        all_data = []
        
        for symbol in symbols:
            print(f"[SystemOrchestrator._load_day_data] Запрос данных для {symbol}...")
            self.mt5_worker.submit(MT5Task(
                task_type="get_historical_data",
                payload={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_date": start_time,
                    "end_date": end_time
                },
                result_callback=lambda res: self._collect_day_data(res, all_data, date),
                error_callback=lambda e: print(f"[SystemOrchestrator._load_day_data] Ошибка: {e}")
            ))
            
    def _collect_day_data(self, res, all_data, date):
        """Сбор данных за день и сохранение."""
        print(f"[SystemOrchestrator._collect_day_data] Получены данные за {date}")
        
        if res.get('data') is not None and not res['data'].empty:
            print(f"[SystemOrchestrator._collect_day_data] Данные не пустые: {res['data'].shape}")
            all_data.append(res['data'])
            print(f"[SystemOrchestrator._collect_day_data] Всего собрано: {len(all_data)} из {len(self.history_settings.get('symbols', []))}")
            
            # Если собрали все данные, сохраняем
            if len(all_data) == len(self.history_settings.get('symbols', [])):
                print("[SystemOrchestrator._collect_day_data] Все данные собраны, объединение...")
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = combined_df.sort_values(['time', 'symbol'])
                print(f"[SystemOrchestrator._collect_day_data] Объединенный DataFrame: {combined_df.shape}")
                
                print("[SystemOrchestrator._collect_day_data] Отправка на сохранение в AVRO...")
                self.file_worker.submit(FileTask(
                    task_type="save_avro",
                    payload={"data": combined_df, "date": date},
                    result_callback=lambda r: print(f"[SystemOrchestrator._collect_day_data] Данные за {date} сохранены"),
                    error_callback=lambda e: print(f"[SystemOrchestrator._collect_day_data] Ошибка: {e}")
                ))
        else:
            print(f"[SystemOrchestrator._collect_day_data] Данные пустые или None для {date}")
                
    def _prepare_real_csv(self):
        """Подготовка real.csv."""
        print("\n[SystemOrchestrator._prepare_real_csv] Шаг 6: Подготовка real.csv...")
        
        need_new_file = True
        if self.output_file.exists():
            print(f"[SystemOrchestrator._prepare_real_csv] Файл {self.output_file} существует")
            last_time = HistoryManager.get_last_recorded_time(self.output_file)
            print(f"[SystemOrchestrator._prepare_real_csv] Последняя запись: {last_time}")
            
            today = datetime.now(timezone.utc).date()
            if last_time and last_time.date() == today:
                print("[SystemOrchestrator._prepare_real_csv] Файл актуален (за сегодня)")
                need_new_file = False
        else:
            print(f"[SystemOrchestrator._prepare_real_csv] Файл {self.output_file} не существует")
                
        if need_new_file:
            print("[SystemOrchestrator._prepare_real_csv] Подготовка нового файла...")
            self.file_worker.submit(FileTask(
                task_type="prepare_real_csv",
                payload={"filename": self.output_file},
                result_callback=lambda r: print(f"[SystemOrchestrator._prepare_real_csv] real.csv подготовлен"),
                error_callback=lambda e: print(f"[SystemOrchestrator._prepare_real_csv] Ошибка: {e}")
            ))
            
            # Загружаем текущие бары
            if self.history_settings.get('enabled', False):
                symbols = self.history_settings.get('symbols', [])
                timeframe = HistoryManager.TIMEFRAME_MAPPING[
                    self.history_settings.get('timeframe', 'M1')
                ]
                
                print(f"[SystemOrchestrator._prepare_real_csv] Загрузка текущих баров для {symbols}...")
                self.mt5_worker.submit(MT5Task(
                    task_type="get_current_bars",
                    payload={"symbols": symbols, "timeframe": timeframe},
                    result_callback=lambda res: self._save_current_bars(res),
                    error_callback=lambda e: print(f"[SystemOrchestrator._prepare_real_csv] Ошибка: {e}")
                ))
                
    def _start_collection_loop(self):
        """Запуск цикла сбора данных."""
        print("\n[SystemOrchestrator._start_collection_loop] Шаг 7: Запуск цикла сбора...")
        
        if not self.history_settings.get('enabled', False):
            print("[SystemOrchestrator._start_collection_loop] Сбор данных отключен")
            return
            
        print("[SystemOrchestrator._start_collection_loop] Создание потока сбора...")
        self.collection_thread = threading.Thread(
            target=self._collection_loop,
            daemon=True
        )
        self.collection_thread.start()
        print(f"[SystemOrchestrator._start_collection_loop] Поток сбора запущен с ID: {self.collection_thread.ident}")
        
    def _collection_loop(self):
        """Цикл сбора данных."""
        print("[SystemOrchestrator._collection_loop] Цикл сбора запущен")
        
        symbols = self.history_settings.get('symbols', [])
        timeframe = HistoryManager.TIMEFRAME_MAPPING[
            self.history_settings.get('timeframe', 'M1')
        ]
        
        print(f"[SystemOrchestrator._collection_loop] Символы: {symbols}")
        print(f"[SystemOrchestrator._collection_loop] Таймфрейм: {timeframe}")
        
        iteration = 0
        while self.system_running:
            try:
                iteration += 1
                print(f"[SystemOrchestrator._collection_loop] Итерация {iteration}")
                
                # Синхронизация с UTC
                utc_now = datetime.now(timezone.utc)
                next_minute = utc_now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                wait_time = (next_minute - utc_now).total_seconds() + 1
                
                print(f"[SystemOrchestrator._collection_loop] Текущее время UTC: {utc_now}")
                print(f"[SystemOrchestrator._collection_loop] Следующая минута: {next_minute}")
                print(f"[SystemOrchestrator._collection_loop] Ожидание: {wait_time} секунд")
                
                if wait_time > 0:
                    time.sleep(wait_time)
                
                # Проверка на новый день
                print("[SystemOrchestrator._collection_loop] Проверка на новый день...")
                self._check_new_day()
                
                # Получение данных за последнюю минуту
                utc_now = datetime.now(timezone.utc)
                minute_start = utc_now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                minute_end = utc_now.replace(second=0, microsecond=0)
                
                print(f"[SystemOrchestrator._collection_loop] Диапазон: {minute_start} - {minute_end}")
                
                for symbol in symbols:
                    print(f"[SystemOrchestrator._collection_loop] Запрос данных для {symbol}...")
                    self.mt5_worker.submit(MT5Task(
                        task_type="get_historical_data",
                        payload={
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "start_date": minute_start,
                            "end_date": minute_end
                        },
                        result_callback=lambda res: self._save_current_bars(res),
                        error_callback=lambda e: print(f"[SystemOrchestrator._collection_loop] Ошибка: {e}")
                    ))
                    
            except Exception as e:
                print(f"[SystemOrchestrator._collection_loop] Ошибка в цикле сбора: {e}")
                time.sleep(5)
                
    def _check_new_day(self):
        """Проверка на новый день и архивация."""
        print("[SystemOrchestrator._check_new_day] Проверка на новый день...")
        utc_now = datetime.now(timezone.utc)
        today = utc_now.date()
        print(f"[SystemOrchestrator._check_new_day] Сегодня: {today}")
        
        if self.output_file.exists():
            last_time = HistoryManager.get_last_recorded_time(self.output_file)
            print(f"[SystemOrchestrator._check_new_day] Последняя запись: {last_time}")
            
            if last_time and last_time.date() != today:
                print(f"[SystemOrchestrator._check_new_day] Новый день {today}. Архивация...")
                
                # Архивация вчерашнего дня
                yesterday = today - timedelta(days=1)
                day_start = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=timezone.utc)
                day_end = datetime.combine(yesterday, datetime.max.time()).replace(tzinfo=timezone.utc)
                
                print(f"[SystemOrchestrator._check_new_day] Архивация дня {yesterday}")
                print(f"[SystemOrchestrator._check_new_day] Диапазон: {day_start} - {day_end}")
                
                symbols = self.history_settings.get('symbols', [])
                timeframe = HistoryManager.TIMEFRAME_MAPPING[
                    self.history_settings.get('timeframe', 'M1')
                ]
                
                self._load_day_data(symbols, timeframe, day_start, day_end, yesterday)
                
                # Пересоздаем real.csv
                print(f"[SystemOrchestrator._check_new_day] Пересоздание real.csv для {today}")
                self.file_worker.submit(FileTask(
                    task_type="prepare_real_csv",
                    payload={"filename": self.output_file},
                    result_callback=lambda r: print(f"[SystemOrchestrator._check_new_day] real.csv пересоздан для {today}"),
                    error_callback=lambda e: print(f"[SystemOrchestrator._check_new_day] Ошибка: {e}")
                ))
            else:
                print("[SystemOrchestrator._check_new_day] День не изменился")
        else:
            print(f"[SystemOrchestrator._check_new_day] Файл {self.output_file} не существует")
                
    def _save_current_bars(self, res):
        """Сохранение текущих баров в real.csv."""
        print("[SystemOrchestrator._save_current_bars] Сохранение текущих баров...")
        
        if res.get('data') is not None and not res['data'].empty:
            print(f"[SystemOrchestrator._save_current_bars] Данные: {res['data'].shape}")
            self.file_worker.submit(FileTask(
                task_type="save_to_real_csv",
                payload={"data": res['data'], "filename": self.output_file},
                result_callback=lambda r: None,  # Тихое сохранение
                error_callback=lambda e: print(f"[SystemOrchestrator._save_current_bars] Ошибка сохранения: {e}")
            ))
        else:
            print("[SystemOrchestrator._save_current_bars] Нет данных для сохранения")
            
    def _handle_terminal_info(self, res):
        """Обработка информации о терминале."""
        print("[SystemOrchestrator._handle_terminal_info] Обработка информации о терминале...")
        info = res.get("terminal_info")
        if info:
            print("\nИнформация о терминале:")
            print(f" Версия: {info['build']}")
            print(f" Название: {info['name']}")
        else:
            print("[SystemOrchestrator._handle_terminal_info] Информация о терминале не получена")
            
    def _handle_account_info(self, res):
        """Обработка информации о счете."""
        print("[SystemOrchestrator._handle_account_info] Обработка информации о счете...")
        acc = res.get("account_info")
        if acc:
            print("\nИнформация о счёте:")
            print(f" Логин: {acc['login']}")
            print(f" Баланс: {acc['balance']} {acc['currency']}")
            print(f" Эквити: {acc['equity']} {acc['currency']}")
        else:
            print("[SystemOrchestrator._handle_account_info] Информация о счете не получена")
            
    def _handle_symbols(self, res):
        """Обработка списка символов."""
        print("[SystemOrchestrator._handle_symbols] Обработка списка символов...")
        symbols = res.get("symbols", [])
        print(f"[SystemOrchestrator._handle_symbols] Получено символов: {len(symbols)}")
        
        if symbols:
            # Сохраняем через FileWorker
            print("[SystemOrchestrator._handle_symbols] Сохранение символов через FileWorker...")
            self.file_worker.submit(FileTask(
                task_type="save_symbols",
                payload={"symbols": [s.name for s in symbols]},
                result_callback=lambda r: print(f"[SystemOrchestrator._handle_symbols] Символы сохранены: {len(symbols)} шт."),
                error_callback=lambda e: print(f"[SystemOrchestrator._handle_symbols] Ошибка: {e}")
            ))
            
            # Отображаем
            if self.display_settings.get('grouped_symbols', True):
                print("[SystemOrchestrator._handle_symbols] Отображение символов...")
                group_and_display_symbols(symbols)
            else:
                print("[SystemOrchestrator._handle_symbols] Отображение символов отключено")
        else:
            print("[SystemOrchestrator._handle_symbols] Список символов пуст")
                
    def _user_input_loop(self):
        """Цикл обработки пользовательского ввода."""
        print("[SystemOrchestrator._user_input_loop] Запуск цикла ввода команд...")
        
        while self.system_running:
            try:
                user_input = input("\nВведите команду: ").strip()
                print(f"[SystemOrchestrator._user_input_loop] Введена команда: '{user_input}'")
            except (EOFError, KeyboardInterrupt):
                print("[SystemOrchestrator._user_input_loop] Прерывание ввода")
                break
                
            if user_input.lower() == self.shutdown_command.lower():
                print("[SystemOrchestrator._user_input_loop] Получена команда отключения...")
                self.stop()
                break
            elif user_input == "status":
                print("[SystemOrchestrator._user_input_loop] Запрос статуса")
                self._show_status()
            elif user_input == "exit":
                print("[SystemOrchestrator._user_input_loop] Команда выхода")
                self.stop()
                break
            elif user_input:
                print(f"[SystemOrchestrator._user_input_loop] Неизвестная команда: '{user_input}'")
                self._show_help()
                
    def _show_status(self):
        """Показ статуса системы."""
        print(f"\nСтатус системы:")
        print(f"  MT5 Worker: {'работает' if self.mt5_worker._is_running else 'остановлен'}")
        print(f"  File Worker: {'работает' if self.file_worker._is_running else 'остановлен'}")
        print(f"  Подключение: {'есть' if self.is_connected else 'нет'}")
        print(f"  System running: {self.system_running}")
        
    def _show_help(self):
        """Показ справки."""
        print(f"Доступные команды:")
        print(f"  {self.shutdown_command} - завершение работы")
        print(f"  status - статус системы")
        print(f"  exit - выход из программы")

    def _check_files_integrity(self) -> bool:
        """Шаг 0: Проверка целостности файлов."""
        print("\n[SystemOrchestrator._check_files_integrity] Шаг 0: Проверка целостности...")
        
        result = {}
        check_event = threading.Event()
        
        def on_check_result(res):
            print(f"[SystemOrchestrator._check_files_integrity] Результат: {res}")
            result.update(res)
            check_event.set()
        
        # Отправляем задачу в FileWorker
        self.file_worker.submit(FileTask(
            task_type="check_integrity",
            result_callback=on_check_result,
            error_callback=lambda e: self._handle_error("проверки целостности", e, check_event)
        ))
        
        # Ждем результат
        if not check_event.wait(timeout=10):
            print("[SystemOrchestrator._check_files_integrity] Таймаут")
            return False
        
        # Проверяем результаты
        all_ok = all(result.values())
        print(f"[SystemOrchestrator._check_files_integrity] Все проверки пройдены: {all_ok}")
        
        if not all_ok:
            print("[SystemOrchestrator._check_files_integrity] Найдены проблемы:")
            for key, value in result.items():
                if not value:
                    print(f"  - {key}: FAILED")
        
        return all_ok

    def _load_data_via_file_worker(self, task_type: str, result_key: str):
        """
        Универсальный метод загрузки данных через FileWorker.
        
        :param task_type: тип задачи ('load_settings', 'load_credentials')
        :param result_key: ключ для извлечения результата ('settings', 'credentials')
        :return: загруженные данные
        """
        print(f"[SystemOrchestrator._load_data_via_file_worker] Загрузка {result_key}...")
        
        result = {}
        event = threading.Event()
        
        def on_result(res):
            result.update(res)
            event.set()
        
        # Отправляем задачу в FileWorker
        self.file_worker.submit(FileTask(
            task_type=task_type,
            result_callback=on_result,
            error_callback=lambda e: self._handle_error(f"загрузки {result_key}", e, event)
        ))
        
        # Ждем результат
        if not event.wait(timeout=10):
            print(f"[SystemOrchestrator._load_data_via_file_worker] Таймаут при загрузке {result_key}")
            return {}
        
        return result.get(result_key, {})