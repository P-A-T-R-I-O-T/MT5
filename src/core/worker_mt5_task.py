# src/core/worker_mt5_task.py

import threading
import time
import queue
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field

from src.core.mt5_connection import MT5Connection
from src.core.realtime_collector import RealtimeCollector
from src.utils.history_manager import HistoryManager
from src.utils.json_manager import JsonManager


@dataclass
class MT5Task:
    """Тип задачи для MT5-воркера."""
    task_type: str  # 'connect', 'get_terminal_info', 'get_account_info',
                   # 'get_symbols', 'download_history', 'start_realtime', 
                   # 'stop_realtime', 'get_realtime_stats'
    payload: Optional[Dict[str, Any]] = field(default_factory=dict)
    result_callback: Optional[callable] = None
    error_callback: Optional[callable] = None


class WorkerMT5Task:
    def __init__(self, json_manager: JsonManager):
        self.json_manager = json_manager
        self.mt5_conn = MT5Connection(json_manager)
        self.realtime_collector: Optional[RealtimeCollector] = None
        self._queue: queue.Queue[MT5Task] = queue.Queue()
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

    def start(self):
        if self._is_running:
            return
        self._stop_flag = False
        self._is_running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print("[WorkerMT5Task] Запущен")

    def stop(self):
        """Корректная остановка с освобождением всех ресурсов."""
        print("[WorkerMT5Task] Начало остановки...")
        self._stop_flag = True
        self._is_running = False
        
        # Останавливаем сбор реального времени
        if self.realtime_collector:
            print("[WorkerMT5Task] Остановка сбора реальных данных...")
            self.realtime_collector.stop()
            self.realtime_collector = None
        
        # Ожидаем завершения потока
        if self._thread:
            print("[WorkerMT5Task] Ожидание завершения потока...")
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                print("[WorkerMT5Task] Поток не завершился, продолжаем...")
        
        # Отключаемся от MT5
        self.mt5_conn.disconnect()
        
        print("[WorkerMT5Task] Остановлен")

    def submit(self, task: MT5Task):
        """Добавляет задачу в очередь."""
        self._queue.put(task)

    def _run_loop(self):
        """Основной цикл обработки задач."""
        while not self._stop_flag:
            try:
                task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                # Проверяем флаг остановки перед выполнением задачи
                if self._stop_flag:
                    print("[WorkerMT5Task] Пропуск задачи из-за флага остановки")
                    # Выходим из цикла, не вызывая task_done(),
                    # так как задача уже извлечена из очереди
                    break
                    
                self._execute_task(task)
            except Exception as e:
                if task.error_callback:
                    task.error_callback(e)
                else:
                    print(f"[WorkerMT5Task] Необработанная ошибка задачи: {e}")
            finally:
                # Вызываем task_done() только если задача была обработана
                # Проверяем, что мы не вышли из-за stop_flag
                if not self._stop_flag:
                    self._queue.task_done()
                else:
                    # Если мы вышли из-за stop_flag, задача уже не нужна
                    # и task_done() вызывать не нужно, так как мы не будем 
                    # дожидаться завершения всех задач
                    pass
        
        # Очищаем очередь при выходе
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        
        print("[WorkerMT5Task] Цикл обработки задач завершен")

    def _execute_task(self, task: MT5Task):
        """Выполняет конкретную задачу."""
        t_type = task.task_type

        if t_type == "connect":
            ok = self.mt5_conn.connect()
            if ok:
                print("[WorkerMT5Task] Подключение к MT5 успешно.")
                # Запускаем мониторинг
                self.mt5_conn._start_connection_monitor()
            else:
                print("[WorkerMT5Task] Ошибка подключения к MT5.")
                if task.error_callback:
                    task.error_callback(RuntimeError("Не удалось подключиться к MT5"))
            if task.result_callback:
                task.result_callback({"connected": ok})

        elif t_type == "get_terminal_info":
            info = self.mt5_conn.get_terminal_info()
            if task.result_callback:
                task.result_callback({"terminal_info": info})

        elif t_type == "get_account_info":
            acc = self.mt5_conn.get_account_info()
            if task.result_callback:
                task.result_callback({"account_info": acc})

        elif t_type == "get_symbols":
            symbols = self.mt5_conn.symbols_get()
            if task.result_callback:
                task.result_callback({"symbols": symbols})

        elif t_type == "download_history":
            history_settings = task.payload.get("history_settings", {})
            HistoryManager.download_and_save_history(self.mt5_conn, history_settings)
            if task.result_callback:
                task.result_callback({"status": "history_downloaded"})

        elif t_type == "start_realtime":
            # Проверяем настройки перед запуском
            settings = self.json_manager.load_settings()
            history_settings = settings.get('history', {})
            realtime_settings = history_settings.get('realtime', {})
            
            if not realtime_settings.get('enabled', False):
                print("[WorkerMT5Task] Сбор реальных данных отключен в настройках")
                if task.result_callback:
                    task.result_callback({"status": "realtime_disabled"})
                return
            
            if not self.realtime_collector:
                self.realtime_collector = RealtimeCollector(self.mt5_conn, self.json_manager)
            self.realtime_collector.start()
            if task.result_callback:
                task.result_callback({"status": "realtime_started"})
                
        elif t_type == "stop_realtime":
            if self.realtime_collector:
                self.realtime_collector.stop()
                self.realtime_collector = None
            if task.result_callback:
                task.result_callback({"status": "realtime_stopped"})
        
        elif t_type == "get_realtime_stats":
            if self.realtime_collector:
                stats = self.realtime_collector.get_stats()
            else:
                stats = {"status": "not_running"}
            if task.result_callback:
                task.result_callback({"stats": stats})

        else:
            msg = f"[WorkerMT5Task] Неизвестный тип задачи: {t_type}"
            print(msg)
            if task.error_callback:
                task.error_callback(ValueError(msg))