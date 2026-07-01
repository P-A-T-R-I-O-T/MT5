# mt5_connection.py

import MetaTrader5 as mt5
import json
import os
import pathlib
import pandas as pd
from src.utils.json_manager import JsonManager
import threading
import time 

class MT5Connection: # Упрощённый класс для подключения к MetaTrader 5

    def __init__(self, json_manager: JsonManager): # Получаем абсолютный путь к файлу
        self.json_manager = json_manager
        self._is_connected = False
        self._monitor_thread = None
        self._monitoring_active = False
        self._load_credentials()

    def _start_connection_monitor(self):
        """Запускает фоновый мониторинг подключения."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return

        self._monitoring_active = True
        self._monitor_thread = threading.Thread(
            target=self._connection_monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()

    def _connection_monitor_loop(self):
        """Цикл мониторинга подключения."""
        interval = self.json_manager.get_monitor_interval()
        while self._monitoring_active:
            time.sleep(interval)
            if not self.is_connected():
                print("Обнаружено отключение от MT5. Попытка переподключения...")
                self.reconnect_with_strategy()

    def reconnect_with_strategy(self) -> bool:
        """
        Переподключение с использованием стратегии задержек.
        :return: успех подключения
        """
        delays = self.json_manager.get_reconnection_delays()

        for attempt, delay in enumerate(delays, 1):
            if delay > 0:
                print(f"Ожидание {delay} секунд перед попыткой {attempt}...")
                time.sleep(delay)

            print(f"Попытка переподключения {attempt}/{len(delays)}...")
            if self.connect():
                print("Переподключение успешно!")
                return True
            else:
                print(f"Попытка {attempt} не удалась")

        print("Все попытки переподключения исчерпаны")
        return False

    def stop_monitoring(self):
        """Останавливает мониторинг подключения."""
        self._monitoring_active = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)

    def _load_credentials(self):
        """Загрузка учётных данных через ConfigManager."""
        credentials = self.json_manager.load_credentials()
        if not credentials:
            print("Файл credentials.json не найден или пуст. Будет использован демо‑режим.")
            return

        try:
            self.login = int(credentials.get('login')) if credentials.get('login') else None
            self.password = credentials.get('password')
            self.server = credentials.get('server')


            if not all([self.login, self.password, self.server]):
                print("Ошибка: в файле отсутствуют необходимые данные")
                self.login = None
                self.password = None
                self.server = None
        except ValueError:
            print("Ошибка: некорректный формат логина (должен быть числом)")

    def connect(self): # Подключение к MT5
        try:
            # Проверяем наличие всех необходимых параметров
            if self.login and self.password and self.server:
                # Передаём именованные аргументы
                if not mt5.initialize(
                    login=self.login,
                    server=self.server,
                    password=self.password
                ):
                    print("Ошибка подключения с учетными данными")
                    error_code, error_desc = mt5.last_error()
                    print(f"Код ошибки: {error_code}, Описание: {error_desc}")
                    return False
                            
                self._is_connected = True
                return True

        except Exception as e:
            print(f"Произошла ошибка при подключении: {str(e)}")
            return False

    def disconnect(self): # Отключение от MT5
        """Отключение от MT5 с остановкой мониторинга."""
        self.stop_monitoring()
        if self._is_connected:
            try:
                mt5.shutdown()
                self._is_connected = False
                print("Отключение от MT5 выполнено")
            except Exception as e:
                print(f"Ошибка при отключении: {str(e)}")

    def get_terminal_info(self): # Получение информации о терминале MT5

        if not self._is_connected:
            return None
        try:
            info = mt5.terminal_info()
            return {
                'build': info.build,
                'name': info.name
            } if info else None
        except Exception as e:
            print(f"Ошибка получения информации о терминале: {e}")
            return None

    def get_account_info(self): # Получение информации о торговом счёте

        if not self._is_connected:
            return None
        try:
            account_info = mt5.account_info()
            return {
                'login': account_info.login,
                'balance': account_info.balance,
                'equity': account_info.equity,
                'currency': account_info.currency
            } if account_info else None
        except Exception as e:
            print(f"Ошибка получения информации о счёте: {e}")
            return None

    def is_connected(self) -> bool: # Проверка состояния подключения
        if not self._is_connected:
            return False
        try:
            info = mt5.terminal_info()
            return info is not None
        except:
            self._is_connected = False

    def symbols_get(self): # Получает все доступные символы из MT5

        try:
            symbols = mt5.symbols_get()
            print(f'Всего символов: {len(symbols)}\n')
            return symbols
        except Exception as e:
            print(f"Ошибка при получении символов: {str(e)}")
            return []

    def simboi_info(self): # показ информации символа символа GBPUSD в MarketWatch

        selected=mt5.symbol_select("GBPUSD",True)
        if not selected:
            print("Failed to select GBPUSD")
            mt5.shutdown()
            quit()
        # выведем свойства по символу GBPUSD 
        symbol_info=mt5.symbol_info("GBPUSD")
        if symbol_info!=None:
            # выведем данные о терминале как есть    
            print(symbol_info)
            print("GBPUSD: spread =",symbol_info.spread,"  digits =",symbol_info.digits)
            # выведем свойства символа в виде списка
            print("Show symbol_info(\"GBPUSD\")._asdict():")
            symbol_info_dict = mt5.symbol_info("GBPUSD")._asdict()
            for prop in symbol_info_dict:
                print("  {}={}".format(prop, symbol_info_dict[prop]))
        
    def get_historical_data(self, symbol_name, timeframe, start_date, end_date):
        """
        :param start_date: datetime объект с tzinfo (UTC)
        :param end_date: datetime объект с tzinfo (UTC)
        """
        if not self._is_connected:
            print("Не подключено к MT5")
            return None

        try:
            # Преобразуем UTC datetime в naive datetime для MT5 (MT5 ожидает naive в UTC)
            if start_date.tzinfo:
                start_date_naive = start_date.replace(tzinfo=None)
            else:
                start_date_naive = start_date

            if end_date.tzinfo:
                end_date_naive = end_date.replace(tzinfo=None)
            else:
                end_date_naive = end_date

            rates = mt5.copy_rates_range(
                symbol_name,
                timeframe,
                start_date_naive,
                end_date_naive
            )

            if rates is None or len(rates) == 0:
                print(f"Нет исторических данных для {symbol_name}")
                return None

            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            # Устанавливаем UTC для временных меток
            df['time'] = df['time'].dt.tz_localize('UTC')
            return df

        except Exception as e:
            print(f"Ошибка при получении исторических данных для {symbol_name}: {str(e)}")
            return None