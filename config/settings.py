# config/settings.py
"""Настройки проекта. Единый источник истины для путей и параметров."""

from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
HISTORY_DIR = BASE_DIR / "history"
REAL_CSV_PATH = HISTORY_DIR / "real.csv"
SYMBOLS_LIST_PATH = CONFIG_DIR / "symbols_list.txt"
D1_PARQUET_PATH = HISTORY_DIR / "D1" / "D1.parquet"   # D1 — один файл


@dataclass
class SystemSettings:
    shutdown_command: str = "0"
    connection_monitor_interval: int = 30
    reconnection_delays: list = field(
        default_factory=lambda: [0, 10, 30, 60, 300]
    )


@dataclass
class DisplaySettings:
    terminal_info: bool = True
    account_info: bool = True
    all_symbols: bool = True
    symbol_details: bool = False
    grouped_symbols: bool = False


@dataclass
class HistorySettings:
    enabled: bool = True
    start_date: datetime = datetime(2026, 10, 14)
    symbols: list = field(
        default_factory=lambda: ["GBPUSD"]  # ["GBPUSD", "BTCUSD"]
    )
    mode: str = "custom"
    symbols_directories: str = "off"

    # --- НОВОЕ ---
    # Список ТФ, для которых качаем историю и ведём live-сбор.
    timeframes: list = field(
        default_factory=lambda: ["M1", "M5", "M15", "H1", "D1"]
    )
    # Для этих ТФ храним ОДИН общий файл (не parquet по дням).
    # D1 — потому что данных мало: достаточно history/D1/D1.parquet.
    # ВАЖНО: такие ТФ НЕ пишутся в real.csv.
    single_file_timeframes: list = field(default_factory=lambda: ["D1"])
    # Сколько секунд подождать после ожидаемого закрытия бара,
    # прежде чем запрашивать его у MT5.
    close_offset_seconds: int = 2


@dataclass
class AISettings:

    enabled: bool = True  # False
    seq_len: int = 60
    horizon: int = 15
    stride: int = 5
    confidence_threshold: float = 0.0
    timeframes: list = field(
        default_factory=lambda: ["M1", "M5", "M15", "H1", "D1"]
    )


@dataclass
class RunSettings:
    mode: str = "live"   # 'live' | 'train'


SYSTEM = SystemSettings()
DISPLAY = DisplaySettings()
HISTORY = HistorySettings()
AI = AISettings()
RUN = RunSettings()