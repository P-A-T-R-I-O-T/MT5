# src/core/file_worker.py
import threading
import queue
import pathlib
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import pandas as pd

from src.utils.history_manager import HistoryManager
from src.utils.symbols_manager import (
    load_symbols as _load_symbols_file,
    save_symbols as _save_symbols_file,
    compare_symbols as _compare_symbols,
)
from config.settings import SYMBOLS_LIST_PATH


@dataclass
class FileTask:
    """Тип задачи для файлового воркера."""
    task_type: str  # 'load_credentials', 'prepare_real_csv',
            # 'save_to_real_csv', 'save_daily_parquet',
            # 'save_symbols', 'compare_symbols', 'reset_real_csv'
    payload: Optional[Dict[str, Any]] = field(default_factory=dict)
    result_callback: Optional[callable] = None
    error_callback: Optional[callable] = None

class FileWorker:
    def __init__(self):

        print("1 [FileWorker.__init__] Инициализация FileWorker...")

        self._ready = threading.Event()
        self._queue: queue.Queue[FileTask] = queue.Queue()
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

        self.base_dir = pathlib.Path('F:/MT5-soft/history')
        self._last_written_time: Dict[str, datetime] = {}


        print("2 [FileWorker.__init__] Инициализация завершена")

        
    def start(self):
        print("[FileWorker.start] Запуск FileWorker...")
        if self._is_running:
            print("[FileWorker.start] FileWorker уже запущен, пропускаем")
            return
        self._stop_flag = False
        self._is_running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"[FileWorker.start] Поток запущен с ID: {self._thread.ident}")
        print("[FileWorker.start] FileWorker успешно запущен")

    def stop(self):
        print("[FileWorker.stop] Остановка FileWorker...")

        self._stop_flag = True
        self._is_running = False
        if self._thread:
            print(f"[FileWorker.stop] Ожидание завершения потока {self._thread.ident}...")
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                print("[FileWorker.stop] Поток не завершился за 5 секунд")
            else:
                print("[FileWorker.stop] Поток успешно завершен")
        print("[FileWorker.stop] FileWorker остановлен")

    def submit(self, task: FileTask):
        print(f"[FileWorker.submit] Добавление задачи: {task.task_type}")
        print(f"[FileWorker.submit] Payload: {task.payload}")
        self._queue.put(task)
        print(f"[FileWorker.submit] Задача добавлена в очередь. Размер очереди: {self._queue.qsize()}")

    def _run_loop(self):
        print("[1 FileWorker._run_loop] Запуск цикла обработки задач...")
        self._ready.set()
        print("2 [FileWorker._run_loop] FileWorker готов к работе")

        processed_tasks = 0
        
        while not self._stop_flag:
            try:
                task = self._queue.get(timeout=0.5)
                print(f"[FileWorker._run_loop] Получена задача: {task.task_type}")
                
            except queue.Empty:
                continue
                
            try:
                print(f"[FileWorker._run_loop] Выполнение задачи: {task.task_type}")
                self._execute_task(task)
                processed_tasks += 1
                print(f"[FileWorker._run_loop] Задача {task.task_type} выполнена успешно")
                
            except Exception as e:
                print(f"[FileWorker._run_loop] Ошибка при выполнении задачи {task.task_type}: {e}")
                if task.error_callback:
                    task.error_callback(e)
                else:
                    print(f"[FileWorker._run_loop] Необработанная ошибка: {e}")
            finally:
                self._queue.task_done()
                print(f"[FileWorker._run_loop] Задача {task.task_type} обработана")
        
        print(f"[FileWorker._run_loop] Цикл завершен. Всего обработано задач: {processed_tasks}")

    def _execute_task(self, task: FileTask):
        """Выполняет конкретную задачу."""
        t_type = task.task_type
        print(f"[FileWorker._execute_task] Начало выполнения задачи: {t_type}")
        print(f"[FileWorker._execute_task] Payload: {task.payload}")
        
        if t_type == "save_to_real_csv":
            data = task.payload.get('data')
            filename = task.payload.get('filename')
            tf_str = task.payload.get('timeframe', 'M1')

            if data is None or filename is None or data.empty:
                if task.result_callback:
                    task.result_callback({"status": "skipped"})
                return

            symbol = data['symbol'].iloc[0]
            # Ключ дедупа теперь (symbol, timeframe)
            key = (symbol, tf_str)
            last_time = self._last_written_time.get(key)

            if last_time is not None:
                before = len(data)
                data = data[data['time'] > last_time]
                print(f"[FileWorker] Фильтр дублей {key}: {before} -> {len(data)}")

            if data.empty:
                if task.result_callback:
                    task.result_callback({"status": "skipped"})
                return

            success = HistoryManager.append_to_real_csv(data, filename)
            if success:
                self._last_written_time[key] = data['time'].max()

            if task.result_callback:
                task.result_callback({"status": "saved" if success else "error"})

        elif t_type == "save_daily_parquet":
                print("[FileWorker._execute_task] Сохранение дневного parquet...")
                data = task.payload.get('data')
                date = task.payload.get('date')
                base_dir = task.payload.get('base_dir', self.base_dir)
                timeframe = task.payload.get('timeframe', 'M1')          # ← НОВОЕ
                print(f"[FileWorker._execute_task] Дата: {date}")
                print(f"[FileWorker._execute_task] Таймфрейм: {timeframe}")  # ← НОВОЕ
                print(f"[FileWorker._execute_task] Директория: {base_dir}")
                print(f"[FileWorker._execute_task] Данные: {data.shape if data is not None else 'None'}")

                if data is not None and not data.empty and date:
                    success = HistoryManager.save_daily_parquet(
                        df=data,
                        date=date,
                        base_dir=base_dir,
                        timeframe=timeframe
                    )
                    if task.result_callback:
                        task.result_callback({
                            "status": "saved" if success else "error",
                            "date": date.strftime('%Y-%m-%d')
                        })
                else:
                    print("[FileWorker._execute_task] Ошибка: data пустые или date не указана")
                    if task.result_callback:
                        task.result_callback({"status": "error"})

        elif t_type == "append_to_single_parquet":
            df = task.payload.get('data')
            file_path = task.payload.get('file_path')
            if df is not None and not df.empty and file_path:
                ok = HistoryManager.append_to_single_parquet(df, pathlib.Path(file_path))
                if task.result_callback:
                    task.result_callback({"status": "saved" if ok else "error"})
            else:
                if task.result_callback:
                    task.result_callback({"status": "error"})

        elif t_type == "load_symbols":
            print("[FileWorker._execute_task] Загрузка символов из txt...")
            symbols = _load_symbols_file(SYMBOLS_LIST_PATH)
            if task.result_callback:
                task.result_callback({"symbols": symbols})

        elif t_type == "compare_symbols":
            print("[FileWorker._execute_task] Сравнение списка символов...")
            new_symbols = task.payload.get('symbols', [])
            if not new_symbols:
                if task.error_callback:
                    task.error_callback(ValueError("Список символов пуст для сравнения"))
                return

            old_symbols = _load_symbols_file(SYMBOLS_LIST_PATH)
            comparison = _compare_symbols(old_symbols, new_symbols)

            print(f"[FileWorker._execute_task]   Было в файле:  {comparison['old_count']}")
            print(f"[FileWorker._execute_task]   Пришло от MT5: {comparison['new_count']}")
            print(f"[FileWorker._execute_task]   added: {len(comparison['added'])}")
            print(f"[FileWorker._execute_task]   removed: {len(comparison['removed'])}")
            if comparison['added']:
                print(f"[FileWorker._execute_task]   +++ {comparison['added']}")
            if comparison['removed']:
                print(f"[FileWorker._execute_task]   --- {comparison['removed']}")

            if task.result_callback:
                task.result_callback({"status": "compared", "comparison": comparison})
        
        elif t_type == "save_symbols":
            print("[FileWorker._execute_task] Сохранение символов в txt...")
            symbols = task.payload.get('symbols', [])
            success = _save_symbols_file(SYMBOLS_LIST_PATH, symbols)
            if task.result_callback:
                task.result_callback({"status": "saved" if success else "error",
                                    "count": len(symbols)})

        elif t_type == "reset_real_csv":
            print("[FileWorker._execute_task] Подготовка real.csv...")
            filename = task.payload.get('filename')
            print(f"[FileWorker._execute_task] Файл для подготовки: {filename}")

            if filename:
                print("[FileWorker._execute_task] Вызов HistoryManager.reset_real_csv")
                HistoryManager.reset_real_csv(filename)

                # Сбрасываем счётчик дублей — файл пуст, начинаем заново
                self._last_written_time = {}
                print("[FileWorker._execute_task] _last_written_time сброшен в None")

                if task.result_callback:
                    print("[FileWorker._execute_task] Вызов result_callback для reset_real_csv")
                    task.result_callback({"status": "prepared", "filename": str(filename)})
                    print("[FileWorker._execute_task] result_callback выполнен")
            else:
                print("[FileWorker._execute_task] Ошибка: filename не указан")
        
        elif t_type == "upsert_daily_parquet":
            print("[FileWorker._execute_task] Upsert дневного parquet...")
            data = task.payload.get('data')
            date = task.payload.get('date')
            base_dir = task.payload.get('base_dir', self.base_dir)
            timeframe = task.payload.get('timeframe', 'M1')

            print(f"[FileWorker._execute_task] Дата: {date}, ТФ: {timeframe}, "
                f"Данные: {data.shape if data is not None else 'None'}")

            if data is not None and not data.empty and date:
                success = HistoryManager.upsert_daily_parquet(
                    df=data, date=date, base_dir=base_dir, timeframe=timeframe
                )
                if task.result_callback:
                    task.result_callback({
                        "status": "saved" if success else "error",
                        "date": date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                    })
            else:
                print("[FileWorker._execute_task] Ошибка: data пустые или date не указана")
                if task.result_callback:
                    task.result_callback({"status": "error"})
        
        else:
            msg = f"[FileWorker._execute_task] Неизвестный тип задачи: {t_type}"
            print(msg)
            if task.error_callback:
                print(f"[FileWorker._execute_task] Вызов error_callback с сообщением: {msg}")
                task.error_callback(ValueError(msg))
                print("[FileWorker._execute_task] error_callback выполнен")
        
        print(f"[FileWorker._execute_task] Завершение выполнения задачи: {t_type}")

    def wait_for_ready(self, timeout=10) -> bool:
        print("[FileWorker.wait_for_ready] Ожидание готовности FileWorker...")
        if self._ready.wait(timeout=timeout):
            print("[FileWorker.wait_for_ready] FileWorker готов")
            return True
        print("[FileWorker.wait_for_ready] Таймаут")
        return False