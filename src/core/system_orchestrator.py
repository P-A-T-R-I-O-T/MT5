# src/core/system_orchestrator.py

import threading
import time
import pathlib
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd

from src.core.mt5_connection import MT5Connection
from src.core.workers.worker_mt5 import WorkerMT5, MT5Task
from src.core.workers.file_worker import FileWorker, FileTask
from src.utils.file_checker import TIMEFRAME_MAPPING
from src.utils.file_checker import (
    check_all_files_integrity,
    is_all_files_integrity_ok,
    collect_all_errors,
)



class SystemOrchestrator:
    """
    Оркестратор системы.
    Управляет двумя воркерами и координирует их работу.
    """
    
    def __init__(self):

        # внутри __init__, самое начало:
        print("0 [SystemOrchestrator.__init__] Проверка целостности файлов...")
        base_path = pathlib.Path('F:/MT5-soft')
        integrity_results = check_all_files_integrity(base_path)

        if not is_all_files_integrity_ok(integrity_results):
            errors = collect_all_errors(integrity_results)
            for err in errors:
                print(f"[SystemOrchestrator.__init__] ОШИБКА: {err}")
            raise RuntimeError(
                f"Проверка целостности файлов не пройдена. Ошибок: {len(errors)}"
            )
        print("0.1 [SystemOrchestrator.__init__] Все файлы прошли проверку")

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

        # --- Настройки из settings.json (разложены по секциям) ---
        print("10 [SystemOrchestrator.__init__] Разбор настроек по секциям...")
        self.display_settings = self.settings.get('display', {})
        self.history_settings = self.settings.get('history', {})
        self.shutdown_command = self.settings.get('system', {}).get('shutdown_command', '0')
        print(f"10.1 [SystemOrchestrator.__init__] display_settings: {self.display_settings}")
        print(f"10.2 [SystemOrchestrator.__init__] history_settings: {self.history_settings}")
        print(f"10.3 [SystemOrchestrator.__init__] shutdown_command: '{self.shutdown_command}'")
        
        # Создаем MT5Connection
        print("[SystemOrchestrator.__init__] Создание MT5Connection...")
        mt5_connection = MT5Connection(credentials=self.credentials)
        print("[SystemOrchestrator.__init__] MT5Connection создан")



        # Воркеры
        print("[SystemOrchestrator.__init__] Создание WorkerMT5...")
        self.mt5_worker = WorkerMT5(
            mt5_connection=mt5_connection,
            settings=self.settings,
            file_worker=self.file_worker
        )
        print("[SystemOrchestrator.__init__] WorkerMT5 создан")

        
        # Состояние
        self.system_running = True
        self.is_connected = False

        # --- Поток сбора данных (создаётся в _start_collection_loop) ---
        self.collection_thread = None

        # --- Последнее записанное время (защита от дублей на уровне оркестратора,
        #     основная защита — в FileWorker._last_written_time) ---
        self._last_written_time = None

        print("[SystemOrchestrator.__init__] Инициализация завершена")
        
    def start(self):
        """Запуск системы."""
        print("[SystemOrchestrator.start] Запуск системы...")

        # Запускаем WorkerMT5
        print("[SystemOrchestrator.start] Запуск WorkerMT5...")
        self.mt5_worker.start()
        print("[SystemOrchestrator.start] WorkerMT5 запущен")

        # Ждём, пока WorkerMT5 завершит инициализацию
        # (подключение к MT5 + получение списка символов).
        # Простейший способ — дождаться, пока в WorkerMT5
        # флаг _init_done станет True. Пока используем простую задержку.
        print("[SystemOrchestrator.start] Ожидание инициализации WorkerMT5...")
        init_wait = 0
        while init_wait < 30:
            if getattr(self.mt5_worker, '_init_done', False):
                print("[SystemOrchestrator.start] WorkerMT5 инициализирован")
                break
            time.sleep(0.5)
            init_wait += 0.5
        else:
            print("[SystemOrchestrator.start] ПРЕДУПРЕЖДЕНИЕ: WorkerMT5 не подтвердил инициализацию за 30 сек")

        # Подготовка real.csv и запуск цикла сбора
        print("[SystemOrchestrator.start] Подготовка real.csv...")
        self._prepare_real_csv()

        print("[SystemOrchestrator.start] Запуск цикла сбора...")
        self._start_collection_loop()

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

        # Останавливаем поток сбора (если запущен)
        if self.collection_thread and self.collection_thread.is_alive():
            print("[SystemOrchestrator.stop] Ожидание завершения потока сбора...")
            self.collection_thread.join(timeout=3)
            if self.collection_thread.is_alive():
                print("[SystemOrchestrator.stop] Поток сбора не завершился за 3 сек")
            else:
                print("[SystemOrchestrator.stop] Поток сбора завершён")

        # Останавливаем WorkerMT5
        print("[SystemOrchestrator.stop] Остановка WorkerMT5...")
        self.mt5_worker.stop()

        # Останавливаем FileWorker
        print("[SystemOrchestrator.stop] Остановка FileWorker...")
        self.file_worker.stop()

        print("[SystemOrchestrator.stop] Система остановлена")
        
    def _handle_error(self, operation: str, error: Exception, event: threading.Event):
        """Обработка ошибок с установкой события."""
        print(f"[SystemOrchestrator._handle_error] Ошибка {operation}: {error}")
        event.set()
                
    def _prepare_real_csv(self):
        """
        Подготовка real.csv:
        1. Перезаписывает файл заголовками (reset_real_csv).
        2. После успешной перезаписи — дозагружает бары за сегодня из MT5.
        """
        print("\n[SystemOrchestrator._prepare_real_csv] Подготовка real.csv...")

        if not self.history_settings.get('enabled', False):
            print("[SystemOrchestrator._prepare_real_csv] История отключена в настройках")
            return

        print("[SystemOrchestrator._prepare_real_csv] Отправка reset_real_csv в FileWorker...")
        self.file_worker.submit(FileTask(
            task_type="reset_real_csv",
            payload={"filename": self.output_file},
            result_callback=lambda r: self._on_reset_real_csv_ready(r),
            error_callback=lambda e: print(f"[SystemOrchestrator._prepare_real_csv] Ошибка: {e}")
        ))

    def _on_reset_real_csv_ready(self, res):
        """Callback после успешной перезаписи real.csv — запускаем дозагрузку баров за сегодня."""
        print(f"[SystemOrchestrator._on_reset_real_csv_ready] real.csv готов: {res}")
        self._load_today_bars()

    def _load_today_bars(self):
        """
        Дозагрузка баров за сегодня из MT5.
        """
        print("\n[SystemOrchestrator._load_today_bars] Дозагрузка баров за сегодня...")

        symbols = self.history_settings.get('symbols', [])
        timeframe_str = self.history_settings.get('timeframe', 'M1')
        timeframe = TIMEFRAME_MAPPING[timeframe_str]

        # Получаем серверное время MT5
        server_now = self._get_server_time_now()
        if server_now is None:
            print("[SystemOrchestrator._load_today_bars] Не удалось получить серверное время, пропуск")
            return

        # Начало дня по серверному времени
        day_start = server_now.replace(hour=0, minute=0, second=0, microsecond=0)
        last_closed_minute = server_now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        print(f"\n\n\n\n\n\n\n[SystemOrchestrator._load_today_bars] last_closed_minute: {last_closed_minute}")
        print(f"[SystemOrchestrator._load_today_bars] Диапазон: {day_start} — {last_closed_minute}")

        print(f"[SystemOrchestrator._load_today_bars] Символы: {symbols}")
        print(f"[SystemOrchestrator._load_today_bars] Таймфрейм: {timeframe}")
        print(f"[SystemOrchestrator._load_today_bars] Диапазон: {day_start} — {server_now}")

        for symbol in symbols:
            print(f"[SystemOrchestrator._load_today_bars] Запрос для {symbol}...")
            self.mt5_worker.submit(MT5Task(
                task_type="get_historical_data",
                payload={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_date": day_start,
                    "end_date": last_closed_minute
                },
                result_callback=self._save_current_bars,
                error_callback=lambda e: print(f"[SystemOrchestrator._load_today_bars] Ошибка: {e}")
            ))


    def _get_server_time_now(self):
        """
        Синхронно запрашивает у WorkerMT5 серверное время.
        Возвращает naive datetime или None при таймауте.
        """
        print("[SystemOrchestrator._get_server_time_now] Запрос серверного времени...")

        result = {}
        event = threading.Event()

        def on_result(res):
            result.update(res)
            event.set()

        def on_error(e):
            print(f"[SystemOrchestrator._get_server_time_now] Ошибка: {e}")
            event.set()

        self.mt5_worker.submit(MT5Task(
            task_type="get_server_time",
            result_callback=on_result,
            error_callback=on_error
        ))

        if not event.wait(timeout=5):
            print("[SystemOrchestrator._get_server_time_now] Таймаут")
            return None

        server_time = result.get("server_time")
        print(f"[SystemOrchestrator._get_server_time_now] Получено: {server_time}")
        return server_time
                
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
        """
        Цикл сбора данных.
        Каждую минуту (+2 сек после закрытия) запрашивает бар за прошедшую минуту
        и отправляет его в FileWorker для записи в real.csv.
        Использует серверное время MT5 (не UTC).
        При переходе через 23:59 инициирует архивацию дня.
        """
        print("[SystemOrchestrator._collection_loop] Цикл сбора запущен")

        symbols = self.history_settings.get('symbols', [])
        timeframe = TIMEFRAME_MAPPING[self.history_settings.get('timeframe', 'M1')]

        print(f"[SystemOrchestrator._collection_loop] Символы: {symbols}")
        print(f"[SystemOrchestrator._collection_loop] Таймфрейм: {timeframe}")

        iteration = 0
        while self.system_running:
            try:
                iteration += 1
                print(f"[SystemOrchestrator._collection_loop] Итерация {iteration}")

                # Синхронизация с серверным временем MT5
                server_now = self._get_server_time_now()
                if server_now is None:
                    print("[SystemOrchestrator._collection_loop] Нет серверного времени, ждём 5 сек")
                    time.sleep(5)
                    continue

                next_minute = server_now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                wait_time = (next_minute - server_now).total_seconds() + 2

                print(f"[SystemOrchestrator._collection_loop] Серверное время: {server_now}")
                print(f"[SystemOrchestrator._collection_loop] Следующая минута: {next_minute}")
                print(f"[SystemOrchestrator._collection_loop] Ожидание: {wait_time:.2f} сек")

                if wait_time > 0:
                    time.sleep(wait_time)

                if not self.system_running:
                    break

                # Определяем только что закрывшуюся минуту по серверному времени
                server_now = self._get_server_time_now()
                if server_now is None:
                    print("[SystemOrchestrator._collection_loop] Нет серверного времени, пропуск итерации")
                    continue

             
                # Если закрылась минута 23:59 — это конец дня
                just_closed = server_now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                if just_closed.hour == 23 and just_closed.minute == 59:
                    print("[SystemOrchestrator._collection_loop] Обнаружен конец дня, запуск архивации")
                    self._on_day_end(just_closed.date())

                # Запрос бара за минуту по каждому символу
                for symbol in symbols:
                    print(f"[SystemOrchestrator._collection_loop] Запрос для {symbol}...")
                    self.mt5_worker.submit(MT5Task(
                        task_type="get_last_closed_bar",
                        payload={
                            "symbol": symbol,
                            "timeframe": timeframe
                        },
                        result_callback=self._save_current_bars,
                        error_callback=lambda e: print(f"[SystemOrchestrator._collection_loop] Ошибка: {e}")
                    ))

            except Exception as e:
                print(f"[SystemOrchestrator._collection_loop] Ошибка в цикле сбора: {e}")
                time.sleep(5)

        print("[SystemOrchestrator._collection_loop] Цикл сбора завершён")

    def _on_day_end(self, date):
        """
        Обработка конца дня.
        Запрашивает у MT5 историю за весь прошедший день (в серверном времени),
        по готовности — сохраняет в history/YYYY-MM-DD.parquet,
        затем очищает real.csv и дозагружает новый день.
        """
        print(f"\n[SystemOrchestrator._on_day_end] Конец дня {date}, архивация...")

        symbols = self.history_settings.get('symbols', [])
        timeframe = TIMEFRAME_MAPPING[self.history_settings.get('timeframe', 'M1')]

        # Диапазон в серверном времени (naive datetime)
        day_start = datetime.combine(date, datetime.min.time())
        day_end = datetime.combine(date, datetime.max.time())

        print(f"[SystemOrchestrator._on_day_end] Диапазон: {day_start} — {day_end}")
        
        all_data = []
        lock = threading.Lock()
        expected = len(symbols)

        def on_symbol_data(res):
            with lock:
                if res.get('data') is not None and not res['data'].empty:
                    all_data.append(res['data'])
                    print(f"[SystemOrchestrator._on_day_end] Собрано {len(all_data)}/{expected}")
                else:
                    print(f"[SystemOrchestrator._on_day_end] Пустые данные для одного из символов")

                if len(all_data) >= expected:
                    print("[SystemOrchestrator._on_day_end] Все данные собраны, архивация...")
                    self._archive_day(date, all_data)

        for symbol in symbols:
            self.mt5_worker.submit(MT5Task(
                task_type="get_historical_data",
                payload={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_date": day_start,
                    "end_date": day_end
                },
                result_callback=on_symbol_data,
                error_callback=lambda e: print(f"[SystemOrchestrator._on_day_end] Ошибка: {e}")
            ))

    def _archive_day(self, date, all_data):
        """Объединяет данные и отправляет в FileWorker для сохранения в parquet."""
        print(f"[SystemOrchestrator._archive_day] Объединение данных за {date}...")

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['time', 'symbol'])
        print(f"[SystemOrchestrator._archive_day] Объединённый DataFrame: {combined_df.shape}")

        self.file_worker.submit(FileTask(
            task_type="save_avro",
            payload={"data": combined_df, "date": date},
            result_callback=lambda r: self._on_archive_success(date),
            error_callback=lambda e: self._on_archive_error(date, e)
        ))

    def _on_archive_success(self, date):
        """Архив сохранён — очищаем real.csv."""
        print(f"[SystemOrchestrator._on_archive_success] Архив за {date} сохранён. Очистка real.csv...")

        self.file_worker.submit(FileTask(
            task_type="reset_real_csv",
            payload={"filename": self.output_file},
            result_callback=lambda r: self._on_reset_success(date),
            error_callback=lambda e: print(f"[SystemOrchestrator._on_archive_success] Ошибка очистки: {e}")
        ))

    def _on_reset_success(self, date):
        """real.csv очищен — дозагружаем новый день."""
        print(f"[SystemOrchestrator._on_reset_success] real.csv очищен. Дозагрузка нового дня...")
        self._load_today_bars()

    def _on_archive_error(self, date, error):
        """Архивация упала — real.csv НЕ трогаем, повтор через 60 сек."""
        print(f"[SystemOrchestrator._on_archive_error] Архивация {date} упала: {error}")
        print("[SystemOrchestrator._on_archive_error] real.csv НЕ очищен. Повтор через 60 сек...")

        # Повтор через таймер
        retry_timer = threading.Timer(60.0, lambda: self._on_day_end(date))
        retry_timer.daemon = True
        retry_timer.start()
                
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

    def _load_data_via_file_worker(self, task_type: str, result_key: str):
        
        """
        Универсальный метод загрузки данных через FileWorker.
        
        Метод выполняет следующие шаги:
        1. Создает контейнер для результата и событие синхронизации
        2. Определяет callback-функцию для получения результата
        3. Отправляет задачу в FileWorker (асинхронно)
        4. Ожидает завершения задачи (максимум 10 секунд)
        5. Возвращает полученные данные
        
        :param task_type: тип задачи ('load_settings', 'load_credentials')
        :param result_key: ключ для извлечения результата ('settings', 'credentials')
        :return: загруженные данные
        """
        print(f"[SystemOrchestrator._load_data_via_file_worker] Загрузка {result_key}...")
        print(f"[SystemOrchestrator._load_data_via_file_worker] Тип задачи: {task_type}")
        print(f"[SystemOrchestrator._load_data_via_file_worker] Ключ результата: {result_key}")
        
        # Шаг 1: Создаем словарь для хранения результата
        # Сюда callback-функция положит данные
        result = {}
        print(f"[SystemOrchestrator._load_data_via_file_worker] Создан контейнер для результата: {result}")
        
        # Шаг 2: Создаем событие для синхронизации потоков
        # Событие будет установлено, когда результат будет получен
        event = threading.Event()
        print(f"[SystemOrchestrator._load_data_via_file_worker] Создано событие синхронизации: {event}")
        
        # Шаг 3: Определяем callback-функцию
        # Она будет вызвана FileWorker'ом, когда задача будет выполнена
        def on_result(res):
            print(f"[SystemOrchestrator._load_data_via_file_worker] Получен результат от FileWorker: {res}")
            
            # Копируем все данные из результата в наш контейнер
            result.update(res)
            print(f"[SystemOrchestrator._load_data_via_file_worker] Результат обновлен: {result}")
            
            # Устанавливаем событие - сигнализируем, что результат готов
            event.set()
            print(f"[SystemOrchestrator._load_data_via_file_worker] Событие установлено - результат готов")
        
        print(f"[SystemOrchestrator._load_data_via_file_worker] Callback-функция определена")
        
        # Шаг 4: Отправляем задачу в FileWorker
        print(f"[SystemOrchestrator._load_data_via_file_worker] Отправка задачи '{task_type}' в FileWorker...")
        
        self.file_worker.submit(FileTask(
            task_type=task_type,  # Тип задачи, например 'load_settings'
            result_callback=on_result,  # Callback для успешного выполнения
            error_callback=lambda e: self._handle_error(f"загрузки {result_key}", e, event)  # Callback для ошибки
        ))
        
        print(f"[SystemOrchestrator._load_data_via_file_worker] Задача '{task_type}' отправлена в очередь FileWorker")
        print(f"[SystemOrchestrator._load_data_via_file_worker] Ожидание результата (максимум 10 секунд)...")
        
        # Шаг 5: Ждем, пока событие не будет установлено (или таймаут 10 секунд)
        if not event.wait(timeout=10):
            # Если таймаут истек, а событие не установлено
            print(f"[SystemOrchestrator._load_data_via_file_worker] ТАЙМАУТ при загрузке {result_key}")
            print(f"[SystemOrchestrator._load_data_via_file_worker] FileWorker не ответил за 10 секунд")
            return {}
        
        # Шаг 6: Событие установлено - извлекаем данные
        print(f"[SystemOrchestrator._load_data_via_file_worker] Событие установлено, извлекаем данные...")
        
        # Достаем данные по ключу (например 'settings' или 'credentials')
        loaded_data = result.get(result_key, {})
        print(f"[SystemOrchestrator._load_data_via_file_worker] Данные по ключу '{result_key}': {loaded_data}")
        
        # Если данные пустые, выводим предупреждение
        if not loaded_data:
            print(f"[SystemOrchestrator._load_data_via_file_worker] ВНИМАНИЕ: Данные по ключу '{result_key}' пустые или отсутствуют")
            print(f"[SystemOrchestrator._load_data_via_file_worker] Содержимое result: {result}")
        
        print(f"[SystemOrchestrator._load_data_via_file_worker] Загрузка {result_key} завершена")
        return loaded_data