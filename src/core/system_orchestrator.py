# src/core/system_orchestrator.py

import threading
import time
from datetime import datetime, timedelta, timezone
from dataclasses import asdict
from typing import Optional
import pandas as pd

from config.settings import (
    SYSTEM, DISPLAY, HISTORY, AI,
    BASE_DIR, HISTORY_DIR, REAL_CSV_PATH,
)
from config.credentials import CREDENTIALS



from src.core.mt5_connection import MT5Connection
from src.core.workers.worker_mt5 import WorkerMT5, MT5Task
from src.core.workers.file_worker import FileWorker, FileTask
from src.core.workers.bar_state_worker import BarStateWorker
from src.utils.file_checker import TIMEFRAME_MAPPING
from src.utils.file_checker import TIMEFRAME_MINUTES
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
        print("0 [SystemOrchestrator.__init__] Проверка целостности файлов...")
        integrity_results = check_all_files_integrity(BASE_DIR)

        if not is_all_files_integrity_ok(integrity_results):
            errors = collect_all_errors(integrity_results)
            for err in errors:
                print(f"[SystemOrchestrator.__init__] ОШИБКА: {err}")
            raise RuntimeError(
                f"Проверка целостности файлов не пройдена. Ошибок: {len(errors)}"
            )
        print("0.1 [SystemOrchestrator.__init__] Все файлы прошли проверку")

        print("1 [SystemOrchestrator.__init__] Инициализация SystemOrchestrator...")

        # --- FileWorker ---
        self.file_worker = FileWorker()
        self.file_worker.start()
        if not self.file_worker.wait_for_ready():
            raise RuntimeError("FileWorker не готов")

        # --- Настройки из config.settings ---
        self.system_settings = SYSTEM
        self.display_settings = asdict(DISPLAY)
        self.history_settings = {
            **asdict(HISTORY),
            'start_date': HISTORY.start_date.strftime('%Y-%m-%d'),
        }
        self.settings = {
            'system': asdict(SYSTEM),
            'display': asdict(DISPLAY),
            'history': self.history_settings,
        }
        self.shutdown_command = SYSTEM.shutdown_command

        print(f"[SystemOrchestrator.__init__] settings: {self.settings}")

        # --- Учётные данные ---
        self.credentials = CREDENTIALS

        # --- Пути — из config.settings, не хардкод ---
        self.base_dir = HISTORY_DIR
        self.output_file = REAL_CSV_PATH

        # --- MT5Connection ---
        mt5_connection = MT5Connection(credentials=self.credentials)

        # --- WorkerMT5 ---
        self.mt5_worker = WorkerMT5(
            mt5_connection=mt5_connection,
            settings=self.settings,
            file_worker=self.file_worker,
        )

        # --- AI: BarStateWorker ---
        self.ai_enabled = AI.enabled
        self.bar_state_worker = None

        if self.ai_enabled:
            print("[SystemOrchestrator.__init__] AI включён, создаём BarStateWorker")
            self.bar_state_worker = BarStateWorker(
                on_tf_close=self._on_tf_close_callback,
                queue_maxsize=100,
            )
        else:
            print("[SystemOrchestrator.__init__] AI выключен, BarStateWorker не создаётся")

        # Состояние
        self.system_running = True
        self.is_connected = False
        self.collection_thread = None
        self._last_written_time = None
        self._archive_in_progress = False
        self._stop_lock = threading.Lock()
        self._stop_called = False
        self._stop_done = threading.Event()

        # --- Планировщики сбора по ТФ ---
        self._collection_threads: list[threading.Thread] = []
        self._day_end_thread: Optional[threading.Thread] = None

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

        # 1. Первичная загрузка истории (с start_date до вчера)
        print("[SystemOrchestrator.start] Первичная загрузка истории...")
        self._initial_history_load()

        # 2 Подготовка real.csv и запуск цикла сбора
        print("[SystemOrchestrator.start] Подготовка real.csv...")
        self._prepare_real_csv()

        print("[SystemOrchestrator.start] Запуск циклов сбора по ТФ...")
        self._start_collection_loops()

        print("[SystemOrchestrator.start] Запуск day-end watchdog...")
        self._start_day_end_watchdog()

        # --- AI: запуск BarStateWorker ---
        if self.bar_state_worker is not None:
            print("[SystemOrchestrator.start] Запуск BarStateWorker...")
            self.bar_state_worker.start()

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

        """Остановка системы. Защищена от повторного вызова."""
        with self._stop_lock:
            if self._stop_called:
                already_called = True
                print("[SystemOrchestrator.stop] Остановка уже выполнена, пропуск")
            else:
                self._stop_called = True
                already_called = False

        if already_called:
            # Кто-то уже запустил остановку — дождёмся её завершения
            print("[SystemOrchestrator.stop] Ожидание завершения уже идущей остановки...")
            self._stop_done.wait(timeout=30)
            return

        try:
            print("\n[SystemOrchestrator.stop] Остановка системы...")
            self.system_running = False

            print("[SystemOrchestrator.stop] Флаг system_running установлен в False")

            # Останавливаем все циклы сбора по ТФ
            for t in self._collection_threads:
                if t.is_alive():
                    t.join(timeout=2)
            self._collection_threads.clear()

            # Day-end watchdog
            if self._day_end_thread and self._day_end_thread.is_alive():
                self._day_end_thread.join(timeout=3)

            # --- AI: остановка BarStateWorker ---
            if self.bar_state_worker is not None:
                print("[SystemOrchestrator.stop] Остановка BarStateWorker...")
                self.bar_state_worker.stop()

            # Останавливаем WorkerMT5
            print("[SystemOrchestrator.stop] Остановка WorkerMT5...")
            self.mt5_worker.stop()

            # Останавливаем FileWorker
            print("[SystemOrchestrator.stop] Остановка FileWorker...")
            self.file_worker.stop()

        finally:
            self._stop_done.set()
            print("[SystemOrchestrator.stop] Система остановлена")
                
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
        """Дозагрузка баров за сегодня из MT5 (только ЗАКРЫТЫЕ бары)."""
        print("\n[SystemOrchestrator._load_today_bars] Дозагрузка баров за сегодня...")
        symbols = self.history_settings.get('symbols', [])
        timeframes = self.history_settings.get('timeframes', ['M1'])
        single_file = set(self.history_settings.get('single_file_timeframes', []))

        server_now = self._get_server_time_now()
        if server_now is None:
            return
        if server_now.tzinfo is None:
            server_now = server_now.replace(tzinfo=timezone.utc)
        else:
            server_now = server_now.astimezone(timezone.utc)

        day_start = server_now.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_since_midnight = server_now.hour * 60 + server_now.minute

        for tf_str in timeframes:
            if tf_str in single_file:
                continue
            tf = TIMEFRAME_MAPPING[tf_str]
            tf_minutes = TIMEFRAME_MINUTES[tf_str]

            # Время открытия последнего ЗАКРЫТОГО бара этого ТФ
            last_closed_start_minutes = (
                (minutes_since_midnight // tf_minutes) - 1
            ) * tf_minutes

            if last_closed_start_minutes < 0:
                # день только начался — ни один бар этого ТФ ещё не закрылся
                print(f"[_load_today_bars {tf_str}] день только начался, пропуск")
                continue

            tf_last_closed_start = day_start + timedelta(minutes=last_closed_start_minutes)

            print(f"[_load_today_bars {tf_str}] диапазон: {day_start} — {tf_last_closed_start}")

            for symbol in symbols:
                self.mt5_worker.submit(MT5Task(
                    task_type="get_historical_data",
                    payload={
                        "symbol": symbol, "timeframe": tf,
                        "start_date": day_start, "end_date": tf_last_closed_start,
                    },
                    result_callback=lambda res, t=tf_str: self._save_current_bars(res, t),
                    error_callback=lambda e, t=tf_str: print(
                        f"[_load_today_bars {t}] Ошибка: {e}"
                    ),
                ))

    def _get_server_time_now(self):
        """
        Синхронно запрашивает у WorkerMT5 серверное время.
        Использует первый символ из HISTORY.symbols как источник тика.
        Возвращает UTC-aware datetime или None при таймауте.
        """
        print("[SystemOrchestrator._get_server_time_now] Запрос серверного времени...")

        symbols = self.history_settings.get('symbols', [])
        ref_symbol = symbols[0] if symbols else "GBPUSD"

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
            payload={"symbol": ref_symbol},
            result_callback=on_result,
            error_callback=on_error
        ))

        if not event.wait(timeout=5):
            print("[SystemOrchestrator._get_server_time_now] Таймаут")
            return None

        server_time = result.get("server_time")
        print(f"[SystemOrchestrator._get_server_time_now] Получено: {server_time}")
        return server_time
                
    # ---------- Планировщик и циклы сбора ----------

    @staticmethod
    def _next_tf_close(server_now: datetime, tf_minutes: int) -> datetime:
        """Ближайший момент закрытия бара заданной длины."""
        if tf_minutes >= 1440:
            return server_now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        minutes_since_midnight = server_now.hour * 60 + server_now.minute
        next_boundary = ((minutes_since_midnight // tf_minutes) + 1) * tf_minutes
        midnight = server_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return midnight + timedelta(minutes=next_boundary)

    def _start_collection_loops(self):
        if not self.history_settings.get('enabled', False):
            print("[SystemOrchestrator._start_collection_loops] История отключена")
            return

        timeframes = self.history_settings.get('timeframes', ['M1'])
        single_file = set(self.history_settings.get('single_file_timeframes', []))

        for tf_str in timeframes:
            if tf_str in single_file:
                continue   # D1 собирается в _day_end_watchdog
            t = threading.Thread(
                target=self._collection_loop_tf, args=(tf_str,),
                daemon=True, name=f"collect_{tf_str}",
            )
            t.start()
            self._collection_threads.append(t)
            print(f"[SystemOrchestrator._start_collection_loops] "
                f"Поток {tf_str} запущен (id={t.ident})")

    def _collection_loop_tf(self, tf_str: str):
        """Цикл сбора закрытых баров одного ТФ."""

        print(f"[_collection_loop_tf {tf_str}] Старт")
        symbols = self.history_settings.get('symbols', [])
        tf = TIMEFRAME_MAPPING[tf_str]
        tf_minutes = TIMEFRAME_MINUTES[tf_str]
        offset = self.history_settings.get('close_offset_seconds', 2)

        while self.system_running:
            try:
                server_now = self._get_server_time_now()
                if server_now is None:
                    time.sleep(5)
                    continue

                if server_now.tzinfo is None:
                    server_now = server_now.replace(tzinfo=timezone.utc)
                else:
                    server_now = server_now.astimezone(timezone.utc)

                next_close = self._next_tf_close(server_now, tf_minutes)
                wait_time = (next_close - server_now).total_seconds() + offset

                print(f"[_collection_loop_tf {tf_str}] {server_now} -> "
                    f"close {next_close} (wait {wait_time:.1f}s)")

                slept = 0.0
                while slept < wait_time and self.system_running:
                    time.sleep(0.5)
                    slept += 0.5
                if not self.system_running:
                    break

                # Запрос последнего закрытого бара по каждому символу
                for symbol in symbols:
                    self.mt5_worker.submit(MT5Task(
                        task_type="get_last_closed_bar",
                        payload={"symbol": symbol, "timeframe": tf},
                        result_callback=lambda res, t=tf_str: self._save_current_bars(res, t),
                        error_callback=lambda e, t=tf_str: print(
                            f"[_collection_loop_tf {t}] Ошибка: {e}"
                        ),
                    ))
            except Exception as e:
                print(f"[_collection_loop_tf {tf_str}] Ошибка: {e}")
                time.sleep(5)

        print(f"[_collection_loop_tf {tf_str}] Остановлен")

    def _start_day_end_watchdog(self):
        self._day_end_thread = threading.Thread(
            target=self._day_end_watchdog, daemon=True, name="day_end_watchdog",
        )
        self._day_end_thread.start()
        print(f"[SystemOrchestrator._start_day_end_watchdog] "
            f"Запущен (id={self._day_end_thread.ident})")

    def _day_end_watchdog(self):
        """Раз в ~20 сек проверяет, не сменились ли сутки. По смене — архивация."""
        print("[_day_end_watchdog] Старт")
        prev_day = None
        while self.system_running:
            try:
                server_now = self._get_server_time_now()
                if server_now is None:
                    time.sleep(10)
                    continue
                if server_now.tzinfo is None:
                    server_now = server_now.replace(tzinfo=timezone.utc)
                else:
                    server_now = server_now.astimezone(timezone.utc)

                current_day = server_now.date()
                if prev_day is not None and current_day > prev_day:
                    print(f"[_day_end_watchdog] Смена дня: {prev_day} -> {current_day}")
                    self._on_day_end(prev_day)
                prev_day = current_day
            except Exception as e:
                print(f"[_day_end_watchdog] Ошибка: {e}")
            time.sleep(60)
        print("[_day_end_watchdog] Остановлен")

    def _on_day_end(self, date):
        if not self.history_settings.get('enabled', False):
            return
        if self._archive_in_progress:
            print("[_on_day_end] Архивация уже идёт, пропуск")
            return
        self._archive_in_progress = True

        print(f"\n[SystemOrchestrator._on_day_end] Архивируем {date}")

        timeframes = self.history_settings.get('timeframes', ['M1'])
        single_file = set(self.history_settings.get('single_file_timeframes', []))
        symbols = self.history_settings.get('symbols', [])

        regular_tfs = [t for t in timeframes if t not in single_file]
        single_tfs = [t for t in timeframes if t in single_file]

        total_tasks = (len(regular_tfs) + len(single_tfs)) * len(symbols)
        state = {'pending': total_tasks, 'results': {}, 'lock': threading.Lock()}

        def _try_finalize():
            with state['lock']:
                if state['pending'] > 0:
                    return
                results = dict(state['results'])
            self._finalize_day_end(date, regular_tfs, single_tfs, results)

        def on_data(res, tf_str):
            with state['lock']:
                df = res.get('data')
                if df is not None and not df.empty:
                    state['results'][(tf_str, res.get('symbol'))] = df
                state['pending'] -= 1
            _try_finalize()

        def on_error(e, tf_str):
            print(f"[_on_day_end] Ошибка {tf_str}: {e}")
            with state['lock']:
                state['pending'] -= 1
            _try_finalize()

        # regular TFs — полный диапазон за день
        for tf_str in regular_tfs:
            tf = TIMEFRAME_MAPPING[tf_str]
            day_start = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
            day_end = datetime.combine(date, datetime.max.time(), tzinfo=timezone.utc).replace(microsecond=0)
            for symbol in symbols:
                self.mt5_worker.submit(MT5Task(
                    task_type="get_historical_data",
                    payload={"symbol": symbol, "timeframe": tf,
                            "start_date": day_start, "end_date": day_end},
                    result_callback=lambda res, t=tf_str: on_data(res, t),
                    error_callback=lambda e, t=tf_str: on_error(e, t),
                ))

        # single-file (D1) — последний закрытый бар (вчерашний)
        for tf_str in single_tfs:
            tf = TIMEFRAME_MAPPING[tf_str]
            for symbol in symbols:
                self.mt5_worker.submit(MT5Task(
                    task_type="get_last_closed_bar",
                    payload={"symbol": symbol, "timeframe": tf},
                    result_callback=lambda res, t=tf_str: on_data(res, t),
                    error_callback=lambda e, t=tf_str: on_error(e, t),
                ))

    def _finalize_day_end(self, date, regular_tfs, single_tfs, results):
        try:
            # 1. Regular TFs → history/<TF>/YYYY-MM-DD.parquet
            for tf_str in regular_tfs:
                tf_data = {s: df for (t, s), df in results.items() if t == tf_str}
                if not tf_data:
                    print(f"[_finalize_day_end] {tf_str}: данных нет, пропуск")
                    continue
                combined = pd.concat(tf_data.values(), ignore_index=True)
                combined = combined.sort_values(['time', 'symbol']).reset_index(drop=True)
                self.file_worker.submit(FileTask(
                    task_type="save_daily_parquet",
                    payload={"data": combined, "date": date,
                            "base_dir": self.base_dir, "timeframe": tf_str},
                    result_callback=lambda r, t=tf_str: print(
                        f"[_finalize_day_end] {t} сохранён: {r}"
                    ),
                    error_callback=lambda e, t=tf_str: print(
                        f"[_finalize_day_end] {t}: ошибка {e}"
                    ),
                ))

            # 2. Single-file TFs (D1) → history/D1/D1.parquet (append с дедупом)
            for tf_str in single_tfs:
                tf_data = {s: df for (t, s), df in results.items() if t == tf_str}
                if not tf_data:
                    print(f"[_finalize_day_end] {tf_str}: данных нет, пропуск")
                    continue
                combined = pd.concat(tf_data.values(), ignore_index=True)
                combined = combined.sort_values(['time', 'symbol']).reset_index(drop=True)
                file_path = self.base_dir / tf_str / f"{tf_str}.parquet"
                self.file_worker.submit(FileTask(
                    task_type="append_to_single_parquet",
                    payload={"data": combined, "file_path": file_path},
                    result_callback=lambda r, t=tf_str: print(
                        f"[_finalize_day_end] {t} append: {r}"
                    ),
                    error_callback=lambda e, t=tf_str: print(
                        f"[_finalize_day_end] {t} append ошибка: {e}"
                    ),
                ))

            # 3. real.csv сбрасываем и загружаем данные нового дня
            self._archive_in_progress = False
            self.file_worker.submit(FileTask(
                task_type="reset_real_csv",
                payload={"filename": self.output_file},
                result_callback=lambda r: self._load_today_bars(),
                error_callback=lambda e: print(f"[_finalize_day_end] reset: {e}"),
            ))
        except Exception as e:
            print(f"[_finalize_day_end] Ошибка: {e}")
            self._archive_in_progress = False

    def _save_daily_parquet(self, date, collected):
        """
        Объединяет данные со всех символов и отправляет в FileWorker.
        Если данных нет вообще — всё равно сбрасывает real.csv и грузит новый день.
        """
        print(f"[SystemOrchestrator._save_daily_parquet] Дата: {date}, "
            f"собрано символов: {len(collected)}")

        if not collected:
            print("[SystemOrchestrator._save_daily_parquet] Нет данных от MT5, "
                "пропуск сохранения. Сброс real.csv и загрузка нового дня.")
            self._reset_and_reload(date)
            return

        combined_df = pd.concat(collected.values(), ignore_index=True)
        combined_df = combined_df.sort_values(['time', 'symbol']).reset_index(drop=True)
        print(f"[SystemOrchestrator._save_daily_parquet] Объединённый DataFrame: {combined_df.shape}")

        self.file_worker.submit(FileTask(
            task_type="save_daily_parquet",
            payload={"data": combined_df, "date": date, "base_dir": self.base_dir, "timeframe": "M1"},
            result_callback=lambda r: self._on_archive_success(date),
            error_callback=lambda e: self._on_archive_error(date, e),
        ))

        print(f"[SystemOrchestrator._save_daily_parquet] Отправка в FileWorker: "
        f"{combined_df.shape[0]} строк, "
        f"символов: {combined_df['symbol'].nunique()}, "
        f"диапазон времени: {combined_df['time'].min()} — {combined_df['time'].max()}")

    def _reset_and_reload(self, date):
        """
        Сбрасывает real.csv и запускает загрузку нового дня.
        Используется, когда архивация не удалась или данных не было.
        """
        self._archive_in_progress = False

        print(f"[SystemOrchestrator._reset_and_reload] Сброс real.csv для {date}...")
        self.file_worker.submit(FileTask(
            task_type="reset_real_csv",
            payload={"filename": self.output_file},
            result_callback=lambda r: self._on_reset_success(date),
            error_callback=lambda e: print(f"[SystemOrchestrator._reset_and_reload] Ошибка: {e}"),
        ))

    def _on_archive_success(self, date):
        """Архив сохранён — очищаем real.csv."""
        self._archive_in_progress = False

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
        print(f"[SystemOrchestrator._on_archive_error] Архивация {date} упала: {error}")
        print("[SystemOrchestrator._on_archive_error] real.csv НЕ очищен. Повтор через 60 сек...")

        self._archive_in_progress = False   # сбрасываем, чтобы ретрай мог запуститься

        retry_timer = threading.Timer(60.0, lambda: self._on_day_end(date))
        retry_timer.daemon = True
        retry_timer.start()
                
    def _save_current_bars(self, res, tf_str: str = "M1"):
        print(f"[_save_current_bars {tf_str}] Сохранение...")
        if res.get('data') is None or res['data'].empty:
            return

        df = res['data'].copy()
        if 'timeframe' not in df.columns:
            df['timeframe'] = tf_str

        # Определяем дату по первому бару (все бары в пакете — за один день)
        try:
            date = pd.to_datetime(df['time'].iloc[0]).date()
        except Exception as e:
            print(f"[_save_current_bars {tf_str}] Не удалось определить дату: {e}")
            return

        # --- 1. Для ВСЕХ обычных ТФ — upsert в history/<TF>/YYYY-MM-DD.parquet
        single_file = set(self.history_settings.get('single_file_timeframes', []))
        if tf_str not in single_file:
            self.file_worker.submit(FileTask(
                task_type="upsert_daily_parquet",
                payload={
                    "data": df,
                    "date": date,
                    "base_dir": self.base_dir,
                    "timeframe": tf_str,
                },
                result_callback=lambda r, t=tf_str: None,
                error_callback=lambda e, t=tf_str: print(
                    f"[_save_current_bars {t}] Ошибка upsert: {e}"
                ),
            ))

        # --- 2. M1 — дополнительно в real.csv и в BarStateWorker
        if tf_str == "M1":
            self.file_worker.submit(FileTask(
                task_type="save_to_real_csv",
                payload={"data": df, "filename": self.output_file, "timeframe": "M1"},
                result_callback=lambda r: None,
                error_callback=lambda e: print(f"[_save_current_bars M1] Ошибка: {e}"),
            ))

            if self.bar_state_worker is not None:
                for _, row in df.iterrows():
                    self.bar_state_worker.submit_m1(row["symbol"], {
                        "time": row["time"],
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "tick_volume": int(row.get("tick_volume", 0)),
                    })

    def _on_tf_close_callback(self, symbol: str, timeframe: str, bar: dict) -> None:
        """Лог закрытого старшего ТФ.

        Персист закрытых M5/M15/H1 идёт нативно из MT5 через _collection_loop_tf
        и _save_current_bars → upsert_daily_parquet.
        D1 персистится отдельно в day-end через append_to_single_parquet.
        """
        print(f"[_on_tf_close_callback] {symbol} {timeframe} закрыт @ "
            f"{bar.get('time')} C={bar['close']} V={bar['tick_volume']}")

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

    def _find_missing_days(self, tf_str: str, start_date, end_date) -> list:
        missing = []
        current = start_date
        while current <= end_date:
            date_str = current.strftime('%Y-%m-%d')
            new_path = self.base_dir / tf_str / f"{date_str}.parquet"
            old_path = self.base_dir / f"{date_str}.parquet"   # legacy для M1
            if not new_path.exists() and not (tf_str == "M1" and old_path.exists()):
                missing.append(current)
            current += timedelta(days=1)
        return missing

    def _initial_history_load(self):
        print("\n[SystemOrchestrator._initial_history_load] Загрузка истории...")
        if not self.history_settings.get('enabled', False):
            return

        server_now = self._get_server_time_now()
        if server_now is None:
            return
        if server_now.tzinfo is None:
            server_now = server_now.replace(tzinfo=timezone.utc)
        else:
            server_now = server_now.astimezone(timezone.utc)

        try:
            start_date = datetime.strptime(
                self.history_settings.get('start_date', ''), '%Y-%m-%d'
            ).date()
        except ValueError:
            print("[_initial_history_load] Неверный start_date")
            return

        yesterday = (server_now - timedelta(days=1)).date()
        if start_date > yesterday:
            return

        symbols = self.history_settings.get('symbols', [])
        timeframes = self.history_settings.get('timeframes', ['M1'])
        single_file = set(self.history_settings.get('single_file_timeframes', []))

        # Regular TFs
        for tf_str in timeframes:
            if tf_str in single_file:
                continue
            missing = self._find_missing_days(tf_str, start_date, yesterday)
            if not missing:
                print(f"[_initial_history_load] {tf_str}: всё загружено")
                continue
            print(f"[_initial_history_load] {tf_str}: не хватает {len(missing)} дней")
            tf = TIMEFRAME_MAPPING[tf_str]
            for day in missing:
                day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                day_end = datetime.combine(day, datetime.max.time(), tzinfo=timezone.utc).replace(microsecond=0)
                self._load_single_day_sync(tf_str, day, day_start, day_end, symbols, tf)

        # Single-file TFs (D1)
        for tf_str in single_file:
            self._load_single_file_tf_sync(tf_str, start_date, yesterday, symbols)

    def _load_single_day_sync(self, tf_str, day, day_start, day_end, symbols, tf):
        print(f"[_load_single_day_sync {tf_str}] {day}")
        if day_start.tzinfo is None:
            day_start = day_start.replace(tzinfo=timezone.utc)
        if day_end.tzinfo is None:
            day_end = day_end.replace(tzinfo=timezone.utc)

        collected, answered, done_event = {}, {'n': 0}, threading.Event()
        lock = threading.Lock()

        def on_data(res):
            with lock:
                answered['n'] += 1
                if res.get('data') is not None and not res['data'].empty:
                    collected[res['symbol']] = res['data']
                if answered['n'] >= len(symbols):
                    done_event.set()

        def on_error(e):
            with lock:
                answered['n'] += 1
                if answered['n'] >= len(symbols):
                    done_event.set()

        for symbol in symbols:
            self.mt5_worker.submit(MT5Task(
                task_type="get_historical_data",
                payload={"symbol": symbol, "timeframe": tf,
                        "start_date": day_start, "end_date": day_end},
                result_callback=on_data, error_callback=on_error,
            ))

        if not done_event.wait(timeout=300):
            print(f"[_load_single_day_sync {tf_str}] {day}: таймаут")
            return
        if not collected:
            return

        combined = pd.concat(collected.values(), ignore_index=True)
        combined = combined.sort_values(['time', 'symbol']).reset_index(drop=True)
        evt = threading.Event()
        self.file_worker.submit(FileTask(
            task_type="save_daily_parquet",
            payload={"data": combined, "date": day,
                    "base_dir": self.base_dir, "timeframe": tf_str},
            result_callback=lambda r: evt.set(),
            error_callback=lambda e: evt.set(),
        ))
        evt.wait(timeout=120)

    def _load_single_file_tf_sync(self, tf_str, start_date, end_date, symbols):
        print(f"[_load_single_file_tf_sync {tf_str}] {start_date}..{end_date}")
        tf = TIMEFRAME_MAPPING[tf_str]
        day_start = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
        day_end = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc).replace(microsecond=0)

        collected, answered, done = {}, {'n': 0}, threading.Event()
        lock = threading.Lock()

        def on_data(res):
            with lock:
                answered['n'] += 1
                if res.get('data') is not None and not res['data'].empty:
                    collected[res['symbol']] = res['data']
                if answered['n'] >= len(symbols):
                    done.set()

        def on_error(e):
            with lock:
                answered['n'] += 1
                if answered['n'] >= len(symbols):
                    done.set()

        for symbol in symbols:
            self.mt5_worker.submit(MT5Task(
                task_type="get_historical_data",
                payload={"symbol": symbol, "timeframe": tf,
                        "start_date": day_start, "end_date": day_end},
                result_callback=on_data, error_callback=on_error,
            ))

        if not done.wait(timeout=180) or not collected:
            print(f"[_load_single_file_tf_sync {tf_str}] нет данных / таймаут")
            return

        combined = pd.concat(collected.values(), ignore_index=True)
        combined = combined.sort_values(['time', 'symbol']).reset_index(drop=True)
        file_path = self.base_dir / tf_str / f"{tf_str}.parquet"
        evt = threading.Event()
        self.file_worker.submit(FileTask(
            task_type="append_to_single_parquet",
            payload={"data": combined, "file_path": file_path},
            result_callback=lambda r: evt.set(),
            error_callback=lambda e: evt.set(),
        ))
        evt.wait(timeout=60)


