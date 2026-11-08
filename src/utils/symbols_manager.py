# src/utils/symbols_manager.py
"""Работа со списком символов в текстовом формате (одна строка = один символ)."""

import os
from pathlib import Path
from typing import List, Dict


def load_symbols(path: Path) -> List[str]:
    """Читает symbols_list.txt. Пустые строки и '#'-комментарии игнорируются."""
    print(f"[SymbolsManager.load_symbols] Чтение: {path}")
    if not path.exists():
        print("[SymbolsManager.load_symbols] Файл не найден, возвращаем []")
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            symbols = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith('#')
            ]
        print(f"[SymbolsManager.load_symbols] Загружено: {len(symbols)} символов")
        return symbols
    except Exception as e:
        print(f"[SymbolsManager.load_symbols] Ошибка: {e}")
        return []


def save_symbols(path: Path, symbols: List[str]) -> bool:
    """Атомарная запись списка символов (tmp + os.replace)."""
    print(f"[SymbolsManager.save_symbols] Сохранение {len(symbols)} символов в {path}")
    tmp = path.with_suffix(path.suffix + '.tmp')
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write('\n'.join(symbols))
            if symbols:
                f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        print("[SymbolsManager.save_symbols] Сохранено")
        return True
    except Exception as e:
        print(f"[SymbolsManager.save_symbols] Ошибка: {e}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def compare_symbols(old_symbols: List[str], new_symbols: List[str]) -> Dict:
    """Сравнивает список из файла с новым списком из MT5."""
    old_set = set(old_symbols)
    new_set = set(new_symbols)
    result = {
        'added': sorted(new_set - old_set),
        'removed': sorted(old_set - new_set),
        'unchanged': sorted(old_set & new_set),
        'old_count': len(old_set),
        'new_count': len(new_set),
        'all_new': sorted(new_set),          # полный новый список — для сохранения
    }
    print(
        f"[SymbolsManager.compare_symbols] old={result['old_count']}, "
        f"new={result['new_count']}, added={len(result['added'])}, "
        f"removed={len(result['removed'])}"
    )
    return result