# src/core/file_worker.py
import threading
import queue
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from pathlib import Path
from src.utils.history_manager import HistoryManager
from src.utils.json_manager import JsonManager

@dataclass
class FileTask:
    """Тип задачи для файлового воркера."""
    task_type: str  # 'check_settings', 'check_history_dir', 'save_avro', 
                   # 'prepare_real_csv', 'save_to_real_csv', 'save_daily_avro'
    payload: Optional[Dict[str, Any]] = field(default_factory=dict)
    result_callback: Optional[callable] = None
    error_callback: Optional[callable] = None

class FileWorker:
    def __init__(self):

        print("1 [FileWorker.__init__] Инициализация FileWorker...")

        print("2 [FileWorker.__init__] инициализация _json_manager_ready")
        self._json_manager_ready = threading.Event()
        print("2 [FileWorker.__init__] инициализация _json_manager_ready завершилась")
  
        self._queue: queue.Queue[FileTask] = queue.Queue()
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

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

        # Создаем JsonManager внутри потока
        print("2 [FileWorker._run_loop] Инициализация JsonManager...")
        self.json_manager = JsonManager()
        print("3 [FileWorker._run_loop] JsonManager создан")

        # Устанавливаем событие готовности
        self._json_manager_ready.set()
        print("4 [FileWorker._run_loop] JsonManager готов к работе")

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

        if t_type == "load_settings":
            print("[FileWorker._execute_task] Загрузка настроек...")
            settings = self.json_manager.load_settings()
            
            if task.result_callback:
                task.result_callback({"settings": settings})

        elif t_type == "load_credentials":  # ДОБАВИТЬ ЭТОТ БЛОК
            print("[FileWorker._execute_task] Загрузка учетных данных...")
            credentials = self.json_manager.load_credentials()
            
            if task.result_callback:
                task.result_callback({"credentials": credentials})
        
        elif t_type == "check_integrity":
            print("[FileWorker._execute_task] Проверка целостности файлов...")
            results = self.json_manager.check_all_files_integrity()
            
            if task.result_callback:
                task.result_callback(results)
        
        elif t_type == "check_settings":
            print("[FileWorker._execute_task] Проверка настроек...")
            settings = task.payload.get('settings', {})
            print(f"[FileWorker._execute_task] Получены настройки: {settings}")
            
            valid, errors = HistoryManager.validate_settings(settings)
            print(f"[FileWorker._execute_task] Результат валидации: valid={valid}, errors={errors}")
            
            if task.result_callback:
                print("[FileWorker._execute_task] Вызов result_callback для check_settings")
                task.result_callback({
                    "valid": valid,
                    "errors": errors
                })
                print("[FileWorker._execute_task] result_callback выполнен")
        
        elif t_type == "check_history_dir":
            print("[FileWorker._execute_task] Проверка директории history...")
            base_dir = task.payload.get('base_dir', self.base_dir)
            print(f"[FileWorker._execute_task] Проверяемая директория: {base_dir}")
            
            created = HistoryManager.check_and_create_history_dir(base_dir)
            print(f"[FileWorker._execute_task] Результат проверки: created={created}")
            
            if task.result_callback:
                print("[FileWorker._execute_task] Вызов result_callback для check_history_dir")
                task.result_callback({"created": created, "path": str(base_dir)})
                print("[FileWorker._execute_task] result_callback выполнен")
        
        elif t_type == "prepare_real_csv":
            print("[FileWorker._execute_task] Подготовка real.csv...")
            filename = task.payload.get('filename')
            print(f"[FileWorker._execute_task] Файл для подготовки: {filename}")
            
            if filename:
                print("[FileWorker._execute_task] Вызов HistoryManager.save_real_csv_header")
                HistoryManager.save_real_csv_header(filename)
                
                if task.result_callback:
                    print("[FileWorker._execute_task] Вызов result_callback для prepare_real_csv")
                    task.result_callback({"status": "prepared", "filename": str(filename)})
                    print("[FileWorker._execute_task] result_callback выполнен")
            else:
                print("[FileWorker._execute_task] Ошибка: filename не указан")
        
        elif t_type == "save_to_real_csv":
            print("[FileWorker._execute_task] Сохранение данных в real.csv...")
            data = task.payload.get('data')
            filename = task.payload.get('filename')
            print(f"[FileWorker._execute_task] Файл: {filename}")
            print(f"[FileWorker._execute_task] Данные: {data.shape if data is not None else 'None'}")
            
            if data is not None and filename:
                print("[FileWorker._execute_task] Вызов HistoryManager.append_to_real_csv")
                success = HistoryManager.append_to_real_csv(data, filename)
                print(f"[FileWorker._execute_task] Результат сохранения: {success}")
                
                if task.result_callback:
                    print("[FileWorker._execute_task] Вызов result_callback для save_to_real_csv")
                    task.result_callback({"status": "saved" if success else "error"})
                    print("[FileWorker._execute_task] result_callback выполнен")
            else:
                print("[FileWorker._execute_task] Ошибка: data или filename не указаны")
        
        elif t_type == "save_avro":
            print("[FileWorker._execute_task] Сохранение в AVRO...")
            data = task.payload.get('data')
            date = task.payload.get('date')
            base_dir = task.payload.get('base_dir', self.base_dir)
            print(f"[FileWorker._execute_task] Дата: {date}")
            print(f"[FileWorker._execute_task] Директория: {base_dir}")
            print(f"[FileWorker._execute_task] Данные: {data.shape if data is not None else 'None'}")
            
            if data is not None and not data.empty and date:
                print("[FileWorker._execute_task] Вызов HistoryManager.save_daily_data_avro")
                HistoryManager.save_daily_data_avro(data, date, base_dir)
                
                if task.result_callback:
                    print("[FileWorker._execute_task] Вызов result_callback для save_avro")
                    task.result_callback({"status": "saved", "date": date.strftime('%d-%m-%Y')})
                    print("[FileWorker._execute_task] result_callback выполнен")
            else:
                print("[FileWorker._execute_task] Ошибка: data пустые или date не указана")
        
        elif t_type == "save_symbols":
            print("[FileWorker._execute_task] Сохранение списка символов...")
            symbols = task.payload.get('symbols', [])
            print(f"[FileWorker._execute_task] Символы: {symbols}")
            print(f"[FileWorker._execute_task] Количество символов: {len(symbols)}")
            
            if symbols:
                print("[FileWorker._execute_task] Вызов JsonManager.save_symbols_to_json")
                success = self.json_manager.save_symbols_to_json(symbols)
                print(f"[FileWorker._execute_task] Результат сохранения: {success}")
                
                if task.result_callback:
                    print("[FileWorker._execute_task] Вызов result_callback для save_symbols")
                    task.result_callback({"status": "saved" if success else "error", "count": len(symbols)})
                    print("[FileWorker._execute_task] result_callback выполнен")
            else:
                print("[FileWorker._execute_task] Ошибка: список символов пуст")
        
        else:
            msg = f"[FileWorker._execute_task] Неизвестный тип задачи: {t_type}"
            print(msg)
            if task.error_callback:
                print(f"[FileWorker._execute_task] Вызов error_callback с сообщением: {msg}")
                task.error_callback(ValueError(msg))
                print("[FileWorker._execute_task] error_callback выполнен")
        
        print(f"[FileWorker._execute_task] Завершение выполнения задачи: {t_type}")


    def wait_for_json_manager(self, timeout=10):
        """Ожидает готовности JsonManager."""
        print("[FileWorker.wait_for_json_manager] Ожидание готовности JsonManager...")
        if self._json_manager_ready.wait(timeout=timeout):
            print("[FileWorker.wait_for_json_manager] JsonManager готов")
            return True
        else:
            print("[FileWorker.wait_for_json_manager] Таймаут ожидания JsonManager")
            return False