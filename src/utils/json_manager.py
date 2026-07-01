# json_manager.py

import json
import pathlib
from typing import Dict, Any, Optional, List
import time
from datetime import datetime, timedelta, timezone


class JsonManager:
    """Единый класс для работы с JSON‑конфигурациями и данными."""

    def __init__(self, base_path: str = 'F:/MT5-soft'):
        self.base_path = pathlib.Path(base_path)
        self._settings_cache: Optional[Dict[str, Any]] = None
        self._credentials_cache: Optional[Dict[str, Any]] = None

    def _load_json_file(self, file_path: pathlib.Path) -> Dict[str, Any]:
        """Загружает JSON‑файл с обработкой ошибок."""
        try:
            if not file_path.exists():
                print(f"Файл {file_path} не найден.")
                return {}


            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            print(f"Ошибка чтения JSON в файле {file_path}: {e}")
            return {}
        except Exception as e:
            print(f"Неожиданная ошибка при чтении файла {file_path}: {e}")
            return {}

    def _save_json_file(self, data: Any, file_path: pathlib.Path) -> bool:
        """Сохраняет данные в JSON‑файл с обработкой ошибок."""
        try:
            # Создаём директорию, если её нет
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Данные сохранены в: {file_path}")
            return True
        except Exception as e:
            print(f"Ошибка при сохранении в файл {file_path}: {str(e)}")
            return False

    # --- МЕТОДЫ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ ---

    def get_system_settings(self) -> Dict[str, Any]:
        """Возвращает системные настройки."""
        settings = self.load_settings()
        return settings.get('system', {})

    def get_shutdown_command(self) -> str:
        """Возвращает команду отключения системы."""
        system_settings = self.get_system_settings()
        return system_settings.get('shutdown_command', 'all off')

    def get_monitor_interval(self) -> int:
        """Возвращает интервал мониторинга подключения."""
        system_settings = self.get_system_settings()
        return system_settings.get('connection_monitor_interval', 30)

    def get_reconnection_delays(self) -> List[int]:
        """Возвращает задержки для переподключения."""
        system_settings = self.get_system_settings()
        return system_settings.get('reconnection_delays', [0, 10, 30, 60, 300])

    def get_symbols_directories(self) -> Any:
        """Возвращает настройку symbols_directories из history."""
        settings = self.load_settings()
        return settings.get('history', {}).get('symbols_directories', 'all')

    def load_settings(self) -> Dict[str, Any]:
        """Загружает настройки из settings.json."""
        if self._settings_cache is not None:
            return self._settings_cache

        settings_path = self.base_path / 'config' / 'settings.json'
        self._settings_cache = self._load_json_file(settings_path)
        return self._settings_cache

    def get_display_settings(self) -> Dict[str, bool]:
        """Возвращает настройки отображения."""
        settings = self.load_settings()
        return settings.get('display', {})

    def get_history_settings(self) -> Dict[str, Any]:
        """Возвращает настройки загрузки истории."""
        settings = self.load_settings()
        return settings.get('history', {})

    # --- МЕТОДЫ ДЛЯ РАБОТЫ С УЧЁТНЫМИ ДАННЫМИ ---

    def load_credentials(self) -> Dict[str, Any]:
        """Загружает учётные данные из credentials.json."""
        if self._credentials_cache is not None:
            return self._credentials_cache

        credentials_path = self.base_path / 'config' / 'credentials.json'
        self._credentials_cache = self._load_json_file(credentials_path)
        return self._credentials_cache

    # --- МЕТОДЫ ДЛЯ РАБОТЫ СО СПИСКОМ СИМВОЛОВ ---

    def load_symbols_list(self) -> List[str]:
        """Загружает список символов из symbols_list.json."""
        symbols_path = self.base_path / 'config' / 'symbols_list.json'
        data = self._load_json_file(symbols_path)
        if isinstance(data, list):
            return data
        else:
            print("Ошибка: symbols_list.json должен содержать массив строк.")
            return []

    def save_symbols_to_json(self, symbols: List[Any], json_file_path: Optional[str] = None) -> bool:
        """Сохраняет список названий символов в JSON."""
        if not symbols:
            print("Нет символов для сохранения в JSON")
            return False

        # Если путь не указан, используем стандартный
        if json_file_path is None:
            json_file_path = str(self.base_path / 'config' / 'symbols_list.json')

        json_path = pathlib.Path(json_file_path)

        # Извлекаем только названия символов (поле 'name')
        symbols_data = [symbol.name for symbol in symbols]

        return self._save_json_file(symbols_data, json_path)

    # --- РАБОТА С ВРЕМЕННЫМИ КОНСТАНТАМИ ---

    def _get_local_timezone_offset(self) -> timedelta:
        """Получает смещение локальной таймзоны относительно UTC."""
        local_time = time.localtime()
        return timedelta(seconds=local_time.tm_gmtoff)

        
    def parse_utc_date_string(self, date_string: str) -> datetime:
        """
        Парсит строку даты в формате YYYY-MM-DD как UTC дату.
        Время устанавливается в 00:00:00 UTC.
        Никаких конвертаций из локального времени нет.
        """
        try:
            # Парсим как naive datetime (без таймзоны)
            naive_dt = datetime.strptime(date_string, '%Y-%m-%d')
            # Сразу ставим UTC
            utc_dt = naive_dt.replace(tzinfo=timezone.utc)
            return utc_dt
        except ValueError as e:
            raise ValueError(f"Неверный формат даты '{date_string}': {e}")

        

    



        


