# json_manager.py

import json
import pathlib
from typing import Dict, Any, Optional, List, Tuple
import time
from datetime import datetime, timedelta, timezone

# Импортируем функции проверки из file_checker
from src.utils.file_checker import (TIMEFRAME_MAPPING)


class JsonManager:
    """Единый класс для работы с JSON-конфигурациями и данными."""

    # Используем TIMEFRAME_MAPPING из file_checker
    TIMEFRAME_MAPPING = TIMEFRAME_MAPPING

    def __init__(self, base_path: str = 'F:/MT5-soft'):
        """Инициализация JsonManager."""
        print("1 [JsonManager.__init__] Инициализация JsonManager...")
        
        self.base_path = pathlib.Path(base_path)
        print(f"2 [JsonManager.__init__] Базовый путь: {base_path}")

        self._settings_cache = None
        print("3 [JsonManager.__init__] Кеш настройки созданы / пустые")
        self._credentials_cache = None
        print("4 [JsonManager.__init__] Кеш учётных данных созданы / пустые")

        # Пути к файлам
        self.settings_path = self.base_path / 'config' / 'settings.json'
        self.credentials_path = self.base_path / 'config' / 'credentials.json'
        self.symbols_list_path = self.base_path / 'config' / 'symbols_list.json'
        self.history_dir = self.base_path / 'history'
        self.real_csv_path = self.history_dir / 'real.csv'
        print("5 [JsonManager.__init__] Пути к файлам инициализированы")
        print("6 [JsonManager.__init__] Инициализация завершена")

    def _load_json_file(self, file_path: pathlib.Path) -> Dict[str, Any]:
        """Загружает JSON-файл с обработкой ошибок."""
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
        """Сохраняет данные в JSON-файл с обработкой ошибок."""
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
        print(f"[JsonManager.load_settings] Путь к файлу: {self.settings_path}")
        
        self._settings_cache = self._load_json_file(self.settings_path)
        print(f"[JsonManager.load_settings] Настройки загружены в кеш: {self._settings_cache}")
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
        print(f"[JsonManager.load_credentials] Путь к файлу: {self.credentials_path}")
        
        self._credentials_cache = self._load_json_file(self.credentials_path)
        print(f"[JsonManager.load_credentials] Учетные данные загружены: {self._credentials_cache}")
        return self._credentials_cache

    # --- МЕТОДЫ ДЛЯ РАБОТЫ СО СПИСКОМ СИМВОЛОВ ---

    def load_symbols_list(self) -> List[str]:
        """Загружает список символов из symbols_list.json."""
        print("[JsonManager.load_symbols_list] Загрузка списка символов...")
        print(f"[JsonManager.load_symbols_list] Путь к файлу: {self.symbols_list_path}")
        
        data = self._load_json_file(self.symbols_list_path)
        if isinstance(data, list):
            print(f"[JsonManager.load_symbols_list] Загружено символов: {len(data)}")
            return data
        else:
            print("[JsonManager.load_symbols_list] Ошибка: symbols_list.json должен содержать массив строк.")
            return []

    def compare_symbols(self, new_symbols: List[str]) -> dict:
        """Сравнивает текущий symbols_list.json с новым списком."""
        print("[JsonManager.compare_symbols] Сравнение символов...")
        old_symbols = self.load_symbols_list()
        old_set = set(old_symbols)
        new_set = set(new_symbols)

        result = {
            'added': sorted(new_set - old_set),      # появились в MT5
            'removed': sorted(old_set - new_set),    # исчезли из MT5
            'unchanged': sorted(old_set & new_set),  # остались
            'old_count': len(old_set),
            'new_count': len(new_set),
        }

        print(f"[JsonManager.compare_symbols] old={result['old_count']}, "
            f"new={result['new_count']}, "
            f"added={len(result['added'])}, "
            f"removed={len(result['removed'])}, "
            f"unchanged={len(result['unchanged'])}")

        return result

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
            json_file_path = str(self.symbols_list_path)
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
        Парсит строку даты в формате DD-MM-YYYY как UTC дату.
        Время устанавливается в 00:00:00 UTC.
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

