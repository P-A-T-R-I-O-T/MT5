# src/utils/file_checker.py

"""
Модуль для проверки целостности файлов и настроек проекта MT5-soft.

Содержит три уровня проверок:

1. Файловый уровень:
   - check_and_create_directory
   - check_settings_file
   - check_credentials_file
   - check_symbols_list_file
   - check_real_csv

2. Структурный уровень (валидаторы):
   - validate_settings_system
   - validate_settings_display
   - validate_settings_history
   - validate_settings (агрегатор)
   - validate_credentials
   - validate_symbols_list

3. Семантический уровень (логические противоречия):
   - validate_settings_semantics
   - validate_credentials_semantics

Агрегатор:
   - check_all_files_integrity
"""

import json
import pathlib
from datetime import datetime
from typing import Dict, Any, Tuple, List


# ============================================================================
# КОНСТАНТЫ
# ============================================================================

# Константы для таймфреймов
TIMEFRAME_MAPPING = {
    'M1': 1,
    'M5': 5,
    'M15': 15,
    'H1': 60,
    'D1': 1440
}

# Допустимые значения для настроек
VALID_MODES = ['all', 'custom']
VALID_SYMBOLS_DIRECTORIES = ['on', 'off', 'all']
VALID_TIMEFRAMES = list(TIMEFRAME_MAPPING.keys())

# Заголовки real.csv
REAL_CSV_COLUMNS = [
    'time', 'open', 'high', 'low', 'close',
    'tick_volume', 'spread', 'real_volume', 'symbol'
]


# ============================================================================
# ШАБЛОНЫ
# ============================================================================

def get_default_settings() -> Dict[str, Any]:
    """Возвращает настройки по умолчанию."""
    print("[file_checker.get_default_settings] Создание настроек по умолчанию...")

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

    print("[file_checker.get_default_settings] Настройки по умолчанию созданы")
    return default_settings


def get_credentials_template() -> Dict[str, str]:
    """Возвращает шаблон для credentials.json."""
    print("[file_checker.get_credentials_template] Создание шаблона credentials...")

    template = {
        "login": "",
        "password": "",
        "server": ""
    }

    print("[file_checker.get_credentials_template] Шаблон credentials создан")
    return template


# ============================================================================
# УРОВЕНЬ 2: СТРУКТУРНЫЕ ВАЛИДАТОРЫ — settings.json
# ============================================================================

def validate_settings_system(system: Any) -> List[str]:
    """
    Проверяет секцию 'system' настроек.

    :param system: значение settings['system'] (или отсутствующее -> {})
    :return: список ошибок (пустой, если всё ок)
    """
    print("[file_checker.validate_settings_system] Валидация секции system...")
    errors: List[str] = []

    if not isinstance(system, dict):
        errors.append("Секция 'system' должна быть словарём")
        return errors

    # shutdown_command
    if 'shutdown_command' not in system:
        errors.append("Отсутствует параметр 'shutdown_command' в system")
    elif not isinstance(system['shutdown_command'], str):
        errors.append("Параметр 'shutdown_command' должен быть строкой")
    elif not system['shutdown_command']:
        errors.append("Параметр 'shutdown_command' не должен быть пустым")

    # connection_monitor_interval
    if 'connection_monitor_interval' not in system:
        errors.append("Отсутствует параметр 'connection_monitor_interval' в system")
    elif not isinstance(system['connection_monitor_interval'], int) \
            or isinstance(system['connection_monitor_interval'], bool):
        errors.append("Параметр 'connection_monitor_interval' должен быть целым числом")
    elif system['connection_monitor_interval'] <= 0:
        errors.append("Параметр 'connection_monitor_interval' должен быть > 0")

    # reconnection_delays
    if 'reconnection_delays' not in system:
        errors.append("Отсутствует параметр 'reconnection_delays' в system")
    elif not isinstance(system['reconnection_delays'], list):
        errors.append("Параметр 'reconnection_delays' должен быть списком")
    elif len(system['reconnection_delays']) == 0:
        errors.append("Параметр 'reconnection_delays' не должен быть пустым")
    else:
        for i, delay in enumerate(system['reconnection_delays']):
            if not isinstance(delay, int) or isinstance(delay, bool):
                errors.append(
                    f"reconnection_delays[{i}] должен быть целым числом"
                )
            elif delay < 0:
                errors.append(
                    f"reconnection_delays[{i}] не может быть отрицательным"
                )

    print(f"[file_checker.validate_settings_system] Ошибок: {len(errors)}")
    return errors


def validate_settings_display(display: Any) -> List[str]:
    """
    Проверяет секцию 'display' настроек.

    :param display: значение settings['display'] (или отсутствующее -> {})
    :return: список ошибок
    """
    print("[file_checker.validate_settings_display] Валидация секции display...")
    errors: List[str] = []

    if not isinstance(display, dict):
        errors.append("Секция 'display' должна быть словарём")
        return errors

    bool_keys = [
        'terminal_info',
        'account_info',
        'all_symbols',
        'symbol_details',
        'grouped_symbols'
    ]

    for key in bool_keys:
        if key not in display:
            errors.append(f"Отсутствует параметр '{key}' в display")
        elif not isinstance(display[key], bool):
            errors.append(f"Параметр 'display.{key}' должен быть boolean")

    print(f"[file_checker.validate_settings_display] Ошибок: {len(errors)}")
    return errors


def validate_settings_history(history: Any) -> List[str]:
    """
    Проверяет секцию 'history' настроек.

    :param history: значение settings['history']
    :return: список ошибок
    """
    print("[file_checker.validate_settings_history] Валидация секции history...")
    errors: List[str] = []

    if not isinstance(history, dict):
        errors.append("Секция 'history' должна быть словарём")
        return errors

    # enabled
    if 'enabled' not in history:
        errors.append("Отсутствует параметр 'enabled' в history")
    elif not isinstance(history['enabled'], bool):
        errors.append("Параметр 'enabled' должен быть boolean")

    # symbols
    if 'symbols' not in history:
        errors.append("Отсутствует параметр 'symbols' в history")
    elif not isinstance(history['symbols'], list):
        errors.append("Параметр 'symbols' должен быть списком")
    elif len(history['symbols']) == 0:
        errors.append("Список символов пуст")
    else:
        for i, sym in enumerate(history['symbols']):
            if not isinstance(sym, str) or not sym:
                errors.append(f"symbols[{i}] должен быть непустой строкой")

    # timeframe
    if 'timeframe' not in history:
        errors.append("Отсутствует параметр 'timeframe' в history")
    elif history['timeframe'] not in VALID_TIMEFRAMES:
        errors.append(f"Неподдерживаемый таймфрейм: {history['timeframe']}")

    # start_date
    if 'start_date' not in history:
        errors.append("Отсутствует параметр 'start_date' в history")
    elif not isinstance(history['start_date'], str):
        errors.append("Параметр 'start_date' должен быть строкой")
    else:
        try:
            datetime.strptime(history['start_date'], '%Y-%m-%d')
        except ValueError:
            errors.append(f"Неверный формат даты: {history['start_date']}")

    # mode
    if 'mode' not in history:
        errors.append("Отсутствует параметр 'mode' в history")
    elif history['mode'] not in VALID_MODES:
        errors.append(f"Неподдерживаемый режим: {history['mode']}")

    # symbols_directories
    if 'symbols_directories' not in history:
        errors.append("Отсутствует параметр 'symbols_directories' в history")
    elif history['symbols_directories'] not in VALID_SYMBOLS_DIRECTORIES:
        errors.append(
            f"Неподдерживаемое значение: {history['symbols_directories']}"
        )

    print(f"[file_checker.validate_settings_history] Ошибок: {len(errors)}")
    return errors


# ============================================================================
# УРОВЕНЬ 3: СЕМАНТИЧЕСКИЕ ПРОВЕРКИ — settings.json
# ============================================================================

def validate_settings_semantics(settings: Dict[str, Any]) -> List[str]:
    """
    Проверяет логические противоречия в settings.

    :param settings: весь словарь настроек
    :return: список предупреждений/ошибок
    """
    print("[file_checker.validate_settings_semantics] Семантическая валидация...")
    warnings: List[str] = []

    history = settings.get('history', {})
    if not isinstance(history, dict):
        return warnings

    enabled = history.get('enabled')
    symbols = history.get('symbols')
    mode = history.get('mode')
    symbols_directories = history.get('symbols_directories')

    # enabled=false, но symbols непустой
    if enabled is False and isinstance(symbols, list) and len(symbols) > 0:
        warnings.append(
            "history.enabled = false, но history.symbols непустой — "
            "символы будут проигнорированы"
        )

    # mode='custom', но symbols пустой
    if mode == 'custom' and isinstance(symbols, list) and len(symbols) == 0:
        warnings.append(
            "history.mode = 'custom', но history.symbols пуст"
        )

    # symbols_directories='on'/'all', но mode не 'custom'
    if symbols_directories in ('on', 'all') and mode != 'custom':
        warnings.append(
            f"history.symbols_directories = '{symbols_directories}', "
            f"но history.mode = '{mode}' — директории будут созданы для всех символов"
        )

    print(f"[file_checker.validate_settings_semantics] Предупреждений: {len(warnings)}")
    return warnings


# ============================================================================
# УРОВЕНЬ 2: СТРУКТУРНЫЙ ВАЛИДАТОР-АГРЕГАТОР — settings.json
# ============================================================================

def validate_settings(settings: Any) -> Tuple[bool, List[str]]:
    """
    Полная валидация settings.json: структурная + семантическая.

    :param settings: словарь настроек
    :return: (успех, список ошибок/предупреждений)
    """
    print("[file_checker.validate_settings] Начало валидации настроек...")
    errors: List[str] = []

    if not isinstance(settings, dict):
        return False, ["Настройки должны быть словарём"]

    # Проверяем наличие всех трёх секций
    for section in ('system', 'display', 'history'):
        if section not in settings:
            errors.append(f"Отсутствует секция '{section}' в настройках")

    # Структурная валидация секций
    errors += validate_settings_system(settings.get('system', {}))
    errors += validate_settings_display(settings.get('display', {}))
    errors += validate_settings_history(settings.get('history', {}))

    # Семантическая валидация (только если структурных ошибок нет)
    if not errors:
        errors += validate_settings_semantics(settings)

    if errors:
        for error in errors:
            print(f"[file_checker.validate_settings] Ошибка: {error}")
        return False, errors

    print("[file_checker.validate_settings] Настройки корректны")
    return True, []


# ============================================================================
# УРОВЕНЬ 2: СТРУКТУРНЫЙ ВАЛИДАТОР — credentials.json
# ============================================================================

def validate_credentials(credentials: Any) -> Tuple[bool, List[str]]:
    """
    Полная валидация credentials.json: структурная + семантическая.

    :param credentials: словарь учётных данных
    :return: (успех, список ошибок)
    """
    print("[file_checker.validate_credentials] Начало валидации credentials...")
    errors: List[str] = []

    if not isinstance(credentials, dict):
        return False, ["credentials.json должен содержать словарь"]

    # login
    if 'login' not in credentials:
        errors.append("Отсутствует поле 'login'")
    else:
        login = credentials['login']
        if login in (None, "", 0):
            errors.append("Поле 'login' пустое")
        else:
            try:
                login_int = int(login)
                if login_int <= 0:
                    errors.append("Поле 'login' должно быть > 0")
            except (ValueError, TypeError):
                errors.append(f"Поле 'login' должно быть числом, получено: {login!r}")

    # password
    if 'password' not in credentials:
        errors.append("Отсутствует поле 'password'")
    else:
        password = credentials['password']
        if not isinstance(password, str):
            errors.append("Поле 'password' должно быть строкой")
        elif not password:
            errors.append("Поле 'password' пустое")

    # server
    if 'server' not in credentials:
        errors.append("Отсутствует поле 'server'")
    else:
        server = credentials['server']
        if not isinstance(server, str):
            errors.append("Поле 'server' должно быть строкой")
        elif not server:
            errors.append("Поле 'server' пустое")

    # Семантические проверки
    if not errors:
        errors += validate_credentials_semantics(credentials)

    if errors:
        for error in errors:
            print(f"[file_checker.validate_credentials] Ошибка: {error}")
        return False, errors

    print("[file_checker.validate_credentials] Credentials корректны")
    return True, []


def validate_credentials_semantics(credentials: Dict[str, Any]) -> List[str]:
    """
    Семантические проверки credentials.

    :param credentials: словарь учётных данных (уже структурно валидный)
    :return: список предупреждений
    """
    print("[file_checker.validate_credentials_semantics] Семантическая валидация...")
    warnings: List[str] = []

    login = credentials.get('login')
    server = credentials.get('server')

    # login слишком маленький / большой
    try:
        login_int = int(login)
        if login_int < 1000:
            warnings.append(f"login = {login_int} подозрительно мал")
        if login_int > 10**12:
            warnings.append(f"login = {login_int} подозрительно велик")
    except (ValueError, TypeError):
        pass

    # server не содержит дефис (обычно формат "Broker-Server")
    if isinstance(server, str) and '-' not in server:
        warnings.append(
            f"server = '{server}' не содержит дефис — "
            f"обычно формат 'Broker-Server'"
        )

    print(f"[file_checker.validate_credentials_semantics] Предупреждений: {len(warnings)}")
    return warnings


# ============================================================================
# УРОВЕНЬ 2: СТРУКТУРНЫЙ ВАЛИДАТОР — symbols_list.json
# ============================================================================

def validate_symbols_list(symbols: Any) -> Tuple[bool, List[str]]:
    """
    Проверяет symbols_list.json.

    :param symbols: список символов
    :return: (успех, список ошибок)
    """
    print("[file_checker.validate_symbols_list] Валидация symbols_list...")
    errors: List[str] = []

    if not isinstance(symbols, list):
        return False, ["symbols_list.json должен содержать массив строк"]

    if len(symbols) == 0:
        errors.append("Список символов пуст")

    seen = set()
    for i, sym in enumerate(symbols):
        if not isinstance(sym, str) or not sym:
            errors.append(f"symbols_list[{i}] должен быть непустой строкой")
            continue
        if sym in seen:
            errors.append(f"symbols_list[{i}] дубликат: '{sym}'")
        seen.add(sym)

    if errors:
        for error in errors:
            print(f"[file_checker.validate_symbols_list] Ошибка: {error}")
        return False, errors

    print("[file_checker.validate_symbols_list] Список символов корректен")
    return True, []


# ============================================================================
# УРОВЕНЬ 1: ФАЙЛОВЫЕ ПРОВЕРКИ
# ============================================================================

def check_and_create_directory(dir_path: pathlib.Path) -> bool:
    """Проверяет существование директории и создаёт её при необходимости."""
    print(f"[file_checker.check_and_create_directory] Проверка директории: {dir_path}")

    try:
        if dir_path.exists():
            print("[file_checker.check_and_create_directory] Директория существует")
            return True

        dir_path.mkdir(parents=True, exist_ok=True)
        print("[file_checker.check_and_create_directory] Директория создана")
        return True
    except Exception as e:
        print(f"[file_checker.check_and_create_directory] Ошибка: {e}")
        return False


def check_settings_file(settings_path: pathlib.Path) -> Tuple[bool, List[str]]:
    """
    Проверяет settings.json.

    - Если файл не существует — создаёт с настройками по умолчанию.
    - Если существует — читает JSON и вызывает validate_settings.

    :param settings_path: путь к settings.json
    :return: (успех, список ошибок)
    """
    print(f"[file_checker.check_settings_file] Проверка: {settings_path}")

    # Файл не существует — создаём с дефолтами
    if not settings_path.exists():
        print("[file_checker.check_settings_file] Файл не существует, создание...")
        default_settings = get_default_settings()

        try:
            settings_path.parent.mkdir(parents=True, exist_ok=True)
            with open(settings_path, 'w', encoding='utf-8') as f:
                json.dump(default_settings, f, ensure_ascii=False, indent=2)
            print("[file_checker.check_settings_file] Файл создан с настройками по умолчанию")
            return True, []
        except Exception as e:
            print(f"[file_checker.check_settings_file] Ошибка при создании файла: {e}")
            return False, [f"Не удалось создать settings.json: {e}"]

    # Файл существует — читаем и валидируем
    print("[file_checker.check_settings_file] Файл существует, проверка...")

    try:
        with open(settings_path, 'r', encoding='utf-8') as f:
            settings = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[file_checker.check_settings_file] Ошибка JSON: {e}")
        return False, [f"Ошибка чтения JSON: {e}"]
    except Exception as e:
        print(f"[file_checker.check_settings_file] Ошибка: {e}")
        return False, [f"Ошибка при чтении файла: {e}"]

    return validate_settings(settings)


def check_credentials_file(credentials_path: pathlib.Path) -> Tuple[bool, List[str]]:
    """
    Проверяет credentials.json.

    - Если файл не существует — создаёт шаблон.
    - Если существует — читает JSON и вызывает validate_credentials.

    :param credentials_path: путь к credentials.json
    :return: (успех, список ошибок)
    """
    print(f"[file_checker.check_credentials_file] Проверка: {credentials_path}")

    # Файл не существует — создаём шаблон
    if not credentials_path.exists():
        print("[file_checker.check_credentials_file] Файл не существует, создание шаблона...")
        template = get_credentials_template()

        try:
            credentials_path.parent.mkdir(parents=True, exist_ok=True)
            with open(credentials_path, 'w', encoding='utf-8') as f:
                json.dump(template, f, ensure_ascii=False, indent=2)
            print("[file_checker.check_credentials_file] Шаблон создан")
            # Шаблон пустой — это ожидаемое состояние, возвращаем True
            return True, []
        except Exception as e:
            print(f"[file_checker.check_credentials_file] Ошибка: {e}")
            return False, [f"Не удалось создать credentials.json: {e}"]

    # Файл существует — читаем и валидируем
    print("[file_checker.check_credentials_file] Файл существует, проверка...")

    try:
        with open(credentials_path, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[file_checker.check_credentials_file] Ошибка JSON: {e}")
        return False, [f"Ошибка чтения JSON: {e}"]
    except Exception as e:
        print(f"[file_checker.check_credentials_file] Ошибка: {e}")
        return False, [f"Ошибка при чтении файла: {e}"]

    return validate_credentials(credentials)


def check_symbols_list_file(symbols_list_path: pathlib.Path) -> Tuple[bool, List[str]]:
    """
    Проверяет symbols_list.json.

    - Если файл не существует — создаёт с пустым списком.
    - Если существует — читает JSON и вызывает validate_symbols_list.

    :param symbols_list_path: путь к symbols_list.json
    :return: (успех, список ошибок)
    """
    print(f"[file_checker.check_symbols_list_file] Проверка: {symbols_list_path}")

    if not symbols_list_path.exists():
        print("[file_checker.check_symbols_list_file] Файл не существует, создание...")
        try:
            symbols_list_path.parent.mkdir(parents=True, exist_ok=True)
            with open(symbols_list_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            print("[file_checker.check_symbols_list_file] Файл создан")
            return True, []
        except Exception as e:
            print(f"[file_checker.check_symbols_list_file] Ошибка: {e}")
            return False, [f"Не удалось создать symbols_list.json: {e}"]

    try:
        with open(symbols_list_path, 'r', encoding='utf-8') as f:
            symbols = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[file_checker.check_symbols_list_file] Ошибка JSON: {e}")
        return False, [f"Ошибка чтения JSON: {e}"]
    except Exception as e:
        print(f"[file_checker.check_symbols_list_file] Ошибка: {e}")
        return False, [f"Ошибка при чтении файла: {e}"]

    # Пустой список — допустимо (файл может быть ещё не заполнен)
    if isinstance(symbols, list) and len(symbols) == 0:
        print("[file_checker.check_symbols_list_file] Список пуст — это допустимо")
        return True, []

    return validate_symbols_list(symbols)


def check_real_csv(real_csv_path: pathlib.Path) -> Tuple[bool, List[str]]:
    """
    Проверяет real.csv.

    - Если файл не существует — создаёт с заголовками.
    - Если существует — проверяет наличие заголовков.

    :param real_csv_path: путь к real.csv
    :return: (успех, список ошибок)
    """
    print(f"[file_checker.check_real_csv] Проверка: {real_csv_path}")

    if not real_csv_path.exists():
        print("[file_checker.check_real_csv] Файл не существует, создание...")
        try:
            real_csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(real_csv_path, 'w', newline='', encoding='utf-8') as f:
                f.write(','.join(REAL_CSV_COLUMNS) + '\n')
            print("[file_checker.check_real_csv] Файл создан")
            return True, []
        except Exception as e:
            print(f"[file_checker.check_real_csv] Ошибка: {e}")
            return False, [f"Не удалось создать real.csv: {e}"]

    # Проверяем заголовки
    try:
        with open(real_csv_path, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()

        if not header_line:
            return False, ["real.csv пуст (нет заголовков)"]

        actual_columns = header_line.split(',')
        if actual_columns != REAL_CSV_COLUMNS:
            return False, [
                f"Неверные заголовки real.csv. Ожидалось: {REAL_CSV_COLUMNS}, "
                f"получено: {actual_columns}"
            ]

        print("[file_checker.check_real_csv] Заголовки корректны")
        return True, []
    except Exception as e:
        print(f"[file_checker.check_real_csv] Ошибка: {e}")
        return False, [f"Ошибка при чтении real.csv: {e}"]


# ============================================================================
# УРОВЕНЬ 4: АГРЕГАТОР
# ============================================================================

def check_all_files_integrity(base_path: pathlib.Path) -> Dict[str, Dict[str, Any]]:
    """
    Проверяет целостность всех файлов и каталогов проекта.

    :param base_path: базовый путь к директории проекта
    :return: словарь вида
        {
            'history_dir':      {'ok': bool, 'errors': [...]},
            'settings_file':    {'ok': bool, 'errors': [...]},
            'credentials_file': {'ok': bool, 'errors': [...]},
            'symbols_list_file':{'ok': bool, 'errors': [...]},
            'real_csv':         {'ok': bool, 'errors': [...]},
        }
    """
    print("[file_checker.check_all_files_integrity] Начало проверки целостности...")

    # Определяем пути
    history_dir = base_path / 'history'
    settings_path = base_path / 'config' / 'settings.json'
    credentials_path = base_path / 'config' / 'credentials.json'
    symbols_list_path = base_path / 'config' / 'symbols_list.json'
    real_csv_path = history_dir / 'real.csv'

    # Директория
    history_ok = check_and_create_directory(history_dir)
    history_errors = [] if history_ok else ["Не удалось создать директорию history"]

    # settings.json
    settings_ok, settings_errors = check_settings_file(settings_path)

    # credentials.json
    credentials_ok, credentials_errors = check_credentials_file(credentials_path)

    # symbols_list.json
    symbols_ok, symbols_errors = check_symbols_list_file(symbols_list_path)

    # real.csv
    real_csv_ok, real_csv_errors = check_real_csv(real_csv_path)

    results = {
        'history_dir':       {'ok': history_ok,       'errors': history_errors},
        'settings_file':     {'ok': settings_ok,      'errors': settings_errors},
        'credentials_file':  {'ok': credentials_ok,   'errors': credentials_errors},
        'symbols_list_file': {'ok': symbols_ok,       'errors': symbols_errors},
        'real_csv':          {'ok': real_csv_ok,      'errors': real_csv_errors},
    }

    # Выводим сводку
    print("[file_checker.check_all_files_integrity] Результаты проверки:")
    for component, result in results.items():
        status_text = "OK" if result['ok'] else "FAIL"
        print(f"  {component}: {status_text}")
        for err in result['errors']:
            print(f"      - {err}")

    all_ok = all(r['ok'] for r in results.values())
    print(f"[file_checker.check_all_files_integrity] Все проверки пройдены: {all_ok}")

    return results


def is_all_files_integrity_ok(results: Dict[str, Dict[str, Any]]) -> bool:
    """
    Вспомогательная функция: проверяет, все ли компоненты прошли проверку.

    :param results: результат check_all_files_integrity
    :return: True, если все ok
    """
    return all(r.get('ok', False) for r in results.values())


def collect_all_errors(results: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Вспомогательная функция: собирает все ошибки в один плоский список.

    :param results: результат check_all_files_integrity
    :return: список строк вида "component: error"
    """
    all_errors: List[str] = []
    for component, result in results.items():
        for err in result.get('errors', []):
            all_errors.append(f"{component}: {err}")
    return all_errors