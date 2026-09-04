# json_manager.py

import json
import pathlib
from typing import Dict, Any, Optional, List
import time
from datetime import datetime, timedelta, timezone


class JsonManager:
    """Единый класс для работы с JSON‑конфигурациями и данными."""

    def __init__(self, base_path: str = 'F:/MT5-soft'):
        print("1 [JsonManager.__init__] Инициализация JsonManager...")
        
        self.base_path = pathlib.Path(base_path)
        print(f"2 [JsonManager.__init__] Базовый путь: {base_path}")

        self._settings_cache = None
        print("3 [JsonManager.__init__] Кеш настройки созданы / пустые")
        self._credentials_cache = None
        print("4 [JsonManager.__init__] Кеш учётных данных созданы / пустые")

        # Добавить пути к файлам
        self.settings_path = self.base_path / 'config' / 'settings.json'
        self.credentials_path = self.base_path / 'config' / 'credentials.json'
        self.symbols_list_path = self.base_path / 'config' / 'symbols_list.json'
        self.history_dir = self.base_path / 'history'
        self.real_csv_path = self.history_dir / 'real.csv'
        print("5 [JsonManager.__init__] Пути к файлам инициализированы")
        print("6 [JsonManager.__init__] Инициализация завершена")

    def _load_json_file(self, file_path: pathlib.Path) -> Dict[str, Any]:
        """Загружает JSON‑файл с обработкой ошибок."""
        print(f"[JsonManager._load_json_file] Загрузка файла: {file_path}")
        
        try:
            if not file_path.exists():
                print(f"[JsonManager._load_json_file] Файл {file_path} не найден.")
                return {}

            print(f"[JsonManager._load_json_file] Файл существует, чтение...")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"[JsonManager._load_json_file] Данные загружены: {data}")
            return data

        except json.JSONDecodeError as e:
            print(f"[JsonManager._load_json_file] Ошибка чтения JSON в файле {file_path}: {e}")
            return {}
        except Exception as e:
            print(f"[JsonManager._load_json_file] Неожиданная ошибка при чтении файла {file_path}: {e}")
            return {}

    def _save_json_file(self, data: Any, file_path: pathlib.Path) -> bool:
        """Сохраняет данные в JSON‑файл с обработкой ошибок."""
        print(f"[JsonManager._save_json_file] Сохранение данных в файл: {file_path}")
        print(f"[JsonManager._save_json_file] Данные: {data}")
        
        try:
            # Создаём директорию, если её нет
            print(f"[JsonManager._save_json_file] Создание директории: {file_path.parent}")
            file_path.parent.mkdir(parents=True, exist_ok=True)

            print("[JsonManager._save_json_file] Запись данных...")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"[JsonManager._save_json_file] Данные сохранены в: {file_path}")
            return True
        except Exception as e:
            print(f"[JsonManager._save_json_file] Ошибка при сохранении в файл {file_path}: {str(e)}")
            return False

    # --- МЕТОДЫ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ ---

    def get_system_settings(self) -> Dict[str, Any]:
        """Возвращает системные настройки."""
        print("[JsonManager.get_system_settings] Получение системных настроек...")
        settings = self.load_settings()
        system_settings = settings.get('system', {})
        print(f"[JsonManager.get_system_settings] Системные настройки: {system_settings}")
        return system_settings

    def get_shutdown_command(self) -> str:
        """Возвращает команду отключения системы."""
        print("[JsonManager.get_shutdown_command] Получение команды отключения...")
        system_settings = self.get_system_settings()
        command = system_settings.get('shutdown_command', 'all off')
        print(f"[JsonManager.get_shutdown_command] Команда: {command}")
        return command

    def get_monitor_interval(self) -> int:
        """Возвращает интервал мониторинга подключения."""
        print("[JsonManager.get_monitor_interval] Получение интервала мониторинга...")
        system_settings = self.get_system_settings()
        interval = system_settings.get('connection_monitor_interval', 30)
        print(f"[JsonManager.get_monitor_interval] Интервал: {interval}")
        return interval

    def get_reconnection_delays(self) -> List[int]:
        """Возвращает задержки для переподключения."""
        print("[JsonManager.get_reconnection_delays] Получение задержек переподключения...")
        system_settings = self.get_system_settings()
        delays = system_settings.get('reconnection_delays', [0, 10, 30, 60, 300])
        print(f"[JsonManager.get_reconnection_delays] Задержки: {delays}")
        return delays

    def get_symbols_directories(self) -> Any:
        """Возвращает настройку symbols_directories из history."""
        print("[JsonManager.get_symbols_directories] Получение настройки symbols_directories...")
        settings = self.load_settings()
        directories = settings.get('history', {}).get('symbols_directories', 'all')
        print(f"[JsonManager.get_symbols_directories] Настройка: {directories}")
        return directories

    def load_settings(self) -> Dict[str, Any]:
        """Загружает настройки из settings.json."""
        print("[JsonManager.load_settings] Загрузка настроек...")
        
        if self._settings_cache is not None:
            print("[JsonManager.load_settings] Настройки загружены из кеша")
            return self._settings_cache

        print("[JsonManager.load_settings] Кеш пуст, загрузка из файла...")
        settings_path = self.base_path / 'config' / 'settings.json'
        print(f"[JsonManager.load_settings] Путь к файлу: {settings_path}")
        
        self._settings_cache = self._load_json_file(settings_path)
        print(f"[JsonManager.load_settings] Настройки загружены в кеш: {self._settings_cache}")
        print("\n" *5)
        return self._settings_cache

    def get_display_settings(self) -> Dict[str, bool]:
        """Возвращает настройки отображения."""
        print("[JsonManager.get_display_settings] Получение настроек отображения...")
        settings = self.load_settings()
        display_settings = settings.get('display', {})
        print(f"[JsonManager.get_display_settings] Настройки отображения: {display_settings}")
        return display_settings

    def get_history_settings(self) -> Dict[str, Any]:
        """Возвращает настройки загрузки истории."""
        print("[JsonManager.get_history_settings] Получение настроек истории...")
        settings = self.load_settings()
        history_settings = settings.get('history', {})
        print(f"[JsonManager.get_history_settings] Настройки истории: {history_settings}")
        return history_settings

    # --- МЕТОДЫ ДЛЯ РАБОТЫ С УЧЁТНЫМИ ДАННЫМИ ---

    def load_credentials(self) -> Dict[str, Any]:
        """Загружает учётные данные из credentials.json."""
        print("[JsonManager.load_credentials] Загрузка учетных данных...")
        
        if self._credentials_cache is not None:
            print("[JsonManager.load_credentials] Учетные данные загружены из кеша")
            return self._credentials_cache

        print("[JsonManager.load_credentials] Кеш пуст, загрузка из файла...")
        credentials_path = self.base_path / 'config' / 'credentials.json'
        print(f"[JsonManager.load_credentials] Путь к файлу: {credentials_path}")
        
        self._credentials_cache = self._load_json_file(credentials_path)
        print(f"[JsonManager.load_credentials] Учетные данные загружены: {self._credentials_cache}")
        return self._credentials_cache

    # --- МЕТОДЫ ДЛЯ РАБОТЫ СО СПИСКОМ СИМВОЛОВ ---

    def load_symbols_list(self) -> List[str]:
        """Загружает список символов из symbols_list.json."""
        print("[JsonManager.load_symbols_list] Загрузка списка символов...")
        symbols_path = self.base_path / 'config' / 'symbols_list.json'
        print(f"[JsonManager.load_symbols_list] Путь к файлу: {symbols_path}")
        
        data = self._load_json_file(symbols_path)
        if isinstance(data, list):
            print(f"[JsonManager.load_symbols_list] Загружено символов: {len(data)}")
            return data
        else:
            print("[JsonManager.load_symbols_list] Ошибка: symbols_list.json должен содержать массив строк.")
            return []

    def save_symbols_to_json(self, symbols: List[Any], json_file_path: Optional[str] = None) -> bool:
        """Сохраняет список названий символов в JSON."""
        print("[JsonManager.save_symbols_to_json] Сохранение списка символов...")
        print(f"[JsonManager.save_symbols_to_json] Количество символов: {len(symbols)}")
        print(f"[JsonManager.save_symbols_to_json] Символы: {symbols}")
        
        if not symbols:
            print("[JsonManager.save_symbols_to_json] Нет символов для сохранения в JSON")
            return False

        # Если путь не указан, используем стандартный
        if json_file_path is None:
            json_file_path = str(self.base_path / 'config' / 'symbols_list.json')
            print(f"[JsonManager.save_symbols_to_json] Используем стандартный путь: {json_file_path}")

        json_path = pathlib.Path(json_file_path)
        print(f"[JsonManager.save_symbols_to_json] Путь для сохранения: {json_path}")

        # Извлекаем только названия символов (поле 'name')
        symbols_data = [symbol.name for symbol in symbols]
        print(f"[JsonManager.save_symbols_to_json] Извлечены имена: {symbols_data}")

        result = self._save_json_file(symbols_data, json_path)
        print(f"[JsonManager.save_symbols_to_json] Результат сохранения: {result}")
        return result

    # --- РАБОТА С ВРЕМЕННЫМИ КОНСТАНТАМИ ---

    def _get_local_timezone_offset(self) -> timedelta:
        """Получает смещение локальной таймзоны относительно UTC."""
        print("[JsonManager._get_local_timezone_offset] Получение смещения локальной таймзоны...")
        local_time = time.localtime()
        offset = timedelta(seconds=local_time.tm_gmtoff)
        print(f"[JsonManager._get_local_timezone_offset] Смещение: {offset}")
        return offset
  
    def parse_utc_date_string(self, date_string: str) -> datetime:
        """
        Парсит строку даты в формате YYYY-MM-DD как UTC дату.
        Время устанавливается в 00:00:00 UTC.
        Никаких конвертаций из локального времени нет.
        """
        print(f"[JsonManager.parse_utc_date_string] Парсинг даты: {date_string}")
        
        try:
            # Парсим как naive datetime (без таймзоны)
            naive_dt = datetime.strptime(date_string, '%Y-%m-%d')
            print(f"[JsonManager.parse_utc_date_string] Naive datetime: {naive_dt}")
            
            # Сразу ставим UTC
            utc_dt = naive_dt.replace(tzinfo=timezone.utc)
            print(f"[JsonManager.parse_utc_date_string] UTC datetime: {utc_dt}")
            
            return utc_dt
        except ValueError as e:
            print(f"[JsonManager.parse_utc_date_string] Ошибка парсинга: {e}")
            raise ValueError(f"Неверный формат даты '{date_string}': {e}")

        # === МЕТОДЫ ПРОВЕРКИ ЦЕЛОСТНОСТИ ===
    
    def check_all_files_integrity(self) -> Dict[str, Any]:
        """Проверяет целостность всех файлов и каталогов."""
        print("[JsonManager.check_all_files_integrity] Начало проверки целостности...")
        
        results = {
            'history_dir': self.check_history_dir(),
            'settings_file': self.check_settings_file(),
            'credentials_file': self.check_credentials_file(),
            'symbols_list_file': self.check_symbols_list_file(),
            'real_csv': self.check_real_csv(),
        }
        
        print(f"[JsonManager.check_all_files_integrity] Результаты: {results}")
        
        # Проверяем, все ли True
        all_ok = all(results.values())
        print(f"[JsonManager.check_all_files_integrity] Все проверки пройдены: {all_ok}")
        
        return results
    
    def check_history_dir(self) -> bool:
        """Проверяет и создает директорию history."""
        print(f"[JsonManager.check_history_dir] Проверка директории: {self.history_dir}")
        
        if self.history_dir.exists():
            print(f"[JsonManager.check_history_dir] Директория {self.history_dir} существует")
            return True
        
        try:
            self.history_dir.mkdir(parents=True, exist_ok=True)
            print(f"[JsonManager.check_history_dir] Директория {self.history_dir} создана")
            return True
        except Exception as e:
            print(f"[JsonManager.check_history_dir] Ошибка: {e}")
            return False
    
    def check_settings_file(self) -> bool:
        """Проверяет settings.json. Если нет - создает с настройками по умолчанию."""
        print(f"[JsonManager.check_settings_file] Проверка: {self.settings_path}")
        
        if self.settings_path.exists():
            print(f"[JsonManager.check_settings_file] Файл {self.settings_path} существует")
            return True
        
        default_settings = {
            "system": {
                "shutdown_command": "0",
                "connection_monitor_interval": 30,
                "reconnection_delays": [0, 10, 30, 60, 300]
            },
            "display": {
                "terminal_info": True,
                "account_info": True,
                "all_symbols": True,
                "symbol_details": False,
                "grouped_symbols": False
            },
            "history": {
                "enabled": True,
                "start_date": "2026-08-10",
                "timeframe": "M1",
                "symbols": ["GBPUSD"],
                "mode": "custom",
                "symbols_directories": "off"
            }
        }
        
        print(f"[JsonManager.check_settings_file] Создание settings.json с настройками по умолчанию")
        return self._save_json_file(default_settings, self.settings_path)
    
    def check_credentials_file(self) -> bool:
        """Проверяет credentials.json. Если нет - создает шаблон."""
        print(f"[JsonManager.check_credentials_file] Проверка: {self.credentials_path}")
        
        if self.credentials_path.exists():
            print(f"[JsonManager.check_credentials_file] Файл {self.credentials_path} существует")
            return True
        
        template = {
            "login": "",
            "password": "",
            "server": ""
        }
        
        print(f"[JsonManager.check_credentials_file] Создание credentials.json с шаблоном")
        return self._save_json_file(template, self.credentials_path)
    
    def check_symbols_list_file(self) -> bool:
        """Проверяет symbols_list.json. Если нет - создает пустой список."""
        print(f"[JsonManager.check_symbols_list_file] Проверка: {self.symbols_list_path}")
        
        if self.symbols_list_path.exists():
            print(f"[JsonManager.check_symbols_list_file] Файл {self.symbols_list_path} существует")
            return True
        
        print(f"[JsonManager.check_symbols_list_file] Создание symbols_list.json с пустым списком")
        return self._save_json_file([], self.symbols_list_path)
    
    def check_real_csv(self) -> bool:
        """Проверяет real.csv. Если нет - создает с заголовками."""
        print(f"[JsonManager.check_real_csv] Проверка: {self.real_csv_path}")
        
        if self.real_csv_path.exists():
            print(f"[JsonManager.check_real_csv] Файл {self.real_csv_path} существует")
            return True
        
        required_columns = ['time', 'open', 'high', 'low', 'close', 
                        'tick_volume', 'spread', 'real_volume', 'symbol']
        
        try:
            self.real_csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.real_csv_path, 'w', newline='', encoding='utf-8') as f:
                f.write(','.join(required_columns) + '\n')
            print(f"[JsonManager.check_real_csv] Файл {self.real_csv_path} создан")
            return True
        except Exception as e:
            print(f"[JsonManager.check_real_csv] Ошибка: {e}")
            return False