# src/utils/file_checker.py

"""
Модуль для проверки целостности файлов и настроек проекта MT5-soft.

Содержит три уровня проверок:

1. Файловый уровень:
   - check_and_create_directory
   - check_real_csv


Агрегатор:
   - check_all_files_integrity
"""

import pathlib
from datetime import datetime
from typing import Dict, Any, Tuple, List
import MetaTrader5 as mt5


# ============================================================================
# КОНСТАНТЫ
# ============================================================================

# ВАЖНО: значения должны быть реальными константами MT5,
# иначе copy_rates_range вернёт пустоту для H1/D1.
TIMEFRAME_MAPPING = {
    'M1':  mt5.TIMEFRAME_M1,     # 1
    'M5':  mt5.TIMEFRAME_M5,     # 5
    'M15': mt5.TIMEFRAME_M15,    # 15
    'H1':  mt5.TIMEFRAME_H1,     # 16385
    'D1':  mt5.TIMEFRAME_D1,     # 16408
}

# Длина окна в минутах — нужна планировщику сбора
TIMEFRAME_MINUTES = {
    'M1': 1, 'M5': 5, 'M15': 15, 'H1': 60, 'D1': 1440,
}

# Заголовки real.csv — добавлена колонка timeframe
REAL_CSV_COLUMNS = [
    'time', 'timeframe', 'open', 'high', 'low', 'close',
    'tick_volume', 'spread', 'real_volume', 'symbol'
]

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

def check_symbols_list_txt(symbols_path: pathlib.Path) -> Tuple[bool, List[str]]:
    """Проверяет symbols_list.txt. Если файла нет — создаёт пустой."""
    print(f"[file_checker.check_symbols_list_txt] Проверка: {symbols_path}")

    if not symbols_path.exists():
        try:
            symbols_path.parent.mkdir(parents=True, exist_ok=True)
            symbols_path.write_text('', encoding='utf-8')
            print("[file_checker.check_symbols_list_txt] Файл создан (пустой)")
            return True, []
        except Exception as e:
            return False, [f"Не удалось создать symbols_list.txt: {e}"]

    try:
        content = symbols_path.read_text(encoding='utf-8')
    except Exception as e:
        return False, [f"Ошибка чтения symbols_list.txt: {e}"]

    if not content.strip():
        return True, []

    errors = []
    seen = set()
    for i, line in enumerate(content.splitlines(), 1):
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        if ' ' in s or '\t' in s:
            errors.append(f"Строка {i}: символ содержит пробелы: {s!r}")
        if s in seen:
            errors.append(f"Строка {i}: дубликат: {s}")
        seen.add(s)

    if errors:
        for e in errors:
            print(f"[file_checker.check_symbols_list_txt] Ошибка: {e}")
        return False, errors

    print("[file_checker.check_symbols_list_txt] Список символов корректен")
    return True, []

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
            if set(actual_columns) == set(REAL_CSV_COLUMNS):
                print("[file_checker.check_real_csv] Порядок колонок не совпадает — "
                      "пересоздаём файл с заголовками")
                try:
                    with open(real_csv_path, 'w', newline='', encoding='utf-8') as f:
                        f.write(','.join(REAL_CSV_COLUMNS) + '\n')
                    return True, []
                except Exception as e:
                    return False, [f"Не удалось пересоздать real.csv: {e}"]
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
    print("[file_checker.check_all_files_integrity] Начало проверки целостности...")

    history_dir = base_path / 'history'
    symbols_list_path = base_path / 'config' / 'symbols_list.txt'
    real_csv_path = history_dir / 'real.csv'

    history_ok = check_and_create_directory(history_dir)
    history_errors = [] if history_ok else ["Не удалось создать директорию history"]

    symbols_ok, symbols_errors = check_symbols_list_txt(symbols_list_path)
    real_csv_ok, real_csv_errors = check_real_csv(real_csv_path)

    results = {
        'history_dir':       {'ok': history_ok,      'errors': history_errors},
        'symbols_list_file': {'ok': symbols_ok,      'errors': symbols_errors},
        'real_csv':          {'ok': real_csv_ok,     'errors': real_csv_errors},
    }

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

