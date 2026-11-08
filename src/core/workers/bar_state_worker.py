# src/core/workers/bar_state_worker.py

"""
BarStateWorker — агрегирует закрытые M1 в неполные старшие ТФ (M5/M15/H1/D1).

Логика:
- Подписывается на поток закрытых M1 (submit_m1).
- Держит self.current_bars = {symbol: {tf: bar}}.
- По каждой M1 обновляет неполный бар каждого ТФ.
- Когда окно закрывается — формирует закрытый бар и отправляет его
  во внешний колбэк (on_tf_close), который обычно пишет в TFArchiveWorker.

Изоляция:
- Отдельный поток.
- Очередь ограничена maxsize, при переполнении дропаем старую M1.
- Ошибки внутри воркера не роняют SystemOrchestrator.
"""

import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

import pandas as pd


# ============================================================================
# Константы: сколько минут в каждом ТФ
# ============================================================================
TF_MINUTES = {
    "M5": 5,
    "M15": 15,
    "H1": 60,
    "D1": 1440,
}

# Порядок от младшего к старшему — важно для проверки «закрылось ли окно»
TF_ORDER = ["M5", "M15", "H1", "D1"]


class BarStateWorker:
    """
    Воркер состояния старших таймфреймов.

    Жизненный цикл:
        w = BarStateWorker(on_tf_close=callback, queue_maxsize=100)
        w.start()
        w.submit_m1(symbol, bar_dict)   # bar_dict — одна M1
        ...
        w.stop()
    """

    def __init__(
        self,
        on_tf_close: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
        queue_maxsize: int = 100,
    ):
        print("[BarStateWorker.__init__] Инициализация...")

        # Колбэк, вызываемый при закрытии старшего ТФ:
        #   on_tf_close(symbol, timeframe, bar_dict)
        self._on_tf_close = on_tf_close

        # --- Очередь входных M1 ---
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=queue_maxsize)
        self._dropped_m1 = 0

        # --- Состояние ---
        # {symbol: {tf: bar_dict}}
        self.current_bars: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Для каждого (symbol, tf) храним время начала окна,
        # чтобы понимать, когда окно закрывается.
        # {symbol: {tf: datetime}}
        self._window_start: Dict[str, Dict[str, datetime]] = {}

        # --- Поток ---
        self._thread: Optional[threading.Thread] = None
        self._stop_flag = False
        self._is_running = False

        # --- Защита current_bars от одновременного чтения/записи ---
        self._lock = threading.Lock()

        print("[BarStateWorker.__init__] Инициализация завершена")

    # ------------------------------------------------------------------ #
    # Управление жизненным циклом
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        print("[BarStateWorker.start] Запуск...")
        if self._is_running:
            print("[BarStateWorker.start] Уже запущен, пропуск")
            return
        self._stop_flag = False
        self._is_running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"[BarStateWorker.start] Поток запущен с ID: {self._thread.ident}")

    def stop(self) -> None:
        print("[BarStateWorker.stop] Остановка...")
        self._stop_flag = True
        self._is_running = False
        if self._thread:
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                print("[BarStateWorker.stop] Поток не завершился за 3 сек")
            else:
                print("[BarStateWorker.stop] Поток завершён")
        print(f"[BarStateWorker.stop] Дропнуто M1 из-за переполнения: {self._dropped_m1}")

    # ------------------------------------------------------------------ #
    # Публичный API
    # ------------------------------------------------------------------ #

    def submit_m1(self, symbol: str, bar: Dict[str, Any]) -> None:
        """
        Добавляет закрытую M1 в очередь.

        bar — dict с ключами:
            time (pd.Timestamp или datetime, naive UTC),
            open, high, low, close, tick_volume
        """
        item = {"symbol": symbol, "bar": bar}
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            # Дропаем самую старую M1, кладём новую
            try:
                dropped = self._queue.get_nowait()
                self._dropped_m1 += 1
                print(f"[BarStateWorker.submit_m1] Очередь переполнена, "
                      f"дропнута M1 {dropped['symbol']} @ "
                      f"{dropped['bar'].get('time')}")
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                # Крайне маловероятно, но на всякий случай
                self._dropped_m1 += 1

    def get_snapshot(self, symbol: str) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Возвращает копию текущего состояния старших ТФ по символу.
        Безопасно для чтения из других потоков.
        """
        with self._lock:
            if symbol not in self.current_bars:
                return None
            # Глубокая копия, чтобы читатель не видел мутаций
            return {
                tf: dict(bar)
                for tf, bar in self.current_bars[symbol].items()
            }

    # ------------------------------------------------------------------ #
    # Основной цикл
    # ------------------------------------------------------------------ #

    def _run_loop(self) -> None:
        print("[BarStateWorker._run_loop] Цикл запущен")

        while not self._stop_flag:
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                symbol = item["symbol"]
                bar = item["bar"]
                self._process_m1(symbol, bar)
            except Exception as e:
                # Никогда не роняем поток из-за одной плохой M1
                print(f"[BarStateWorker._run_loop] Ошибка обработки M1: {e}")
            finally:
                self._queue.task_done()

        print("[BarStateWorker._run_loop] Цикл завершён")

    # ------------------------------------------------------------------ #
    # Обработка одной M1
    # ------------------------------------------------------------------ #

    def _process_m1(self, symbol: str, m1: Dict[str, Any]) -> None:
        """
        Обновляет неполные бары всех ТФ и, если окно закрылось,
        эмитит закрытый бар через on_tf_close.
        """
        t: datetime = m1["time"]
        if isinstance(t, pd.Timestamp):
            t = t.to_pydatetime()
        if t.tzinfo is not None:
            # Приводим к naive UTC — так же, как в real.csv
            t = t.astimezone(timezone.utc).replace(tzinfo=None)

        with self._lock:
            if symbol not in self.current_bars:
                self.current_bars[symbol] = {}
                self._window_start[symbol] = {}

            for tf in TF_ORDER:
                self._update_tf(symbol, tf, m1, t)

    def _update_tf(
        self,
        symbol: str,
        tf: str,
        m1: Dict[str, Any],
        t: datetime,
    ) -> None:
        """
        Обновляет бар указанного ТФ. Если окно закрылось —
        эмитит закрытый бар и открывает новое окно.
        """
        minutes = TF_MINUTES[tf]
        window_start = self._window_start[symbol].get(tf)

        # Определяем начало окна для текущей M1
        current_window_start = self._floor_to_window(t, minutes)

        if window_start is None:
            # Первая M1 по этому ТФ — открываем новое окно
            self._window_start[symbol][tf] = current_window_start
            self.current_bars[symbol][tf] = self._make_new_bar(m1)
            return

        if current_window_start == window_start:
            # M1 внутри того же окна — обновляем неполный бар
            bar = self.current_bars[symbol][tf]
            bar["high"] = max(bar["high"], m1["high"])
            bar["low"] = min(bar["low"], m1["low"])
            bar["close"] = m1["close"]
            bar["tick_volume"] += m1.get("tick_volume", 0)
            return

        # Окно закрылось: эмитим закрытый бар и открываем новое окно
        closed_bar = dict(self.current_bars[symbol][tf])
        closed_bar["time"] = window_start  # время начала окна
        closed_bar["symbol"] = symbol
        closed_bar["timeframe"] = tf

        # Новое окно
        self._window_start[symbol][tf] = current_window_start
        self.current_bars[symbol][tf] = self._make_new_bar(m1)

        # Эмитим закрытый бар — вне лока, чтобы не блокировать
        # (но мы внутри with self._lock из _process_m1,
        #  поэтому вызываем колбэк после выхода — см. ниже)
        self._emit_closed_bar(symbol, tf, closed_bar)

    def _emit_closed_bar(
        self,
        symbol: str,
        tf: str,
        closed_bar: Dict[str, Any],
    ) -> None:
        """Безопасно вызывает on_tf_close, если он задан."""
        if self._on_tf_close is None:
            return
        try:
            self._on_tf_close(symbol, tf, closed_bar)
        except Exception as e:
            print(f"[BarStateWorker._emit_closed_bar] Ошибка в колбэке "
                  f"({symbol}, {tf}): {e}")

    # ------------------------------------------------------------------ #
    # Вспомогательное
    # ------------------------------------------------------------------ #

    @staticmethod
    def _floor_to_window(t: datetime, minutes: int) -> datetime:
        """
        Округляет время вниз до начала окна нужной длины.
        Для D1 — начало суток (00:00).
        Для H1 — начало часа.
        Для M5/M15 — стандартное округление минут.
        """
        if minutes == 1440:
            return t.replace(hour=0, minute=0, second=0, microsecond=0)

        # Общее правило: минуты от начала суток // minutes * minutes
        total_minutes = t.hour * 60 + t.minute
        floored = (total_minutes // minutes) * minutes
        new_hour = floored // 60
        new_minute = floored % 60
        return t.replace(hour=new_hour, minute=new_minute, second=0, microsecond=0)

    @staticmethod
    def _make_new_bar(m1: Dict[str, Any]) -> Dict[str, Any]:
        """Создаёт новый бар из первой M1 окна."""
        return {
            "open": m1["open"],
            "high": m1["high"],
            "low": m1["low"],
            "close": m1["close"],
            "tick_volume": m1.get("tick_volume", 0),
        }