# mt5_connection.py

import MetaTrader5 as mt5
import pandas as pd
from src.utils.json_manager import JsonManager
import threading
import time 

class MT5Connection: # Упрощённый класс для подключения к MetaTrader 5

    def __init__(self, json_manager: JsonManager): # Получаем абсолютный путь к файлу
        print("[MT5Connection.__init__] Инициализация MT5Connection...")
        self.json_manager = json_manager
        self._is_connected = False
        self._monitor_thread = None
        self._monitoring_active = False
        print("[MT5Connection.__init__] Вызов _load_credentials()...")
        self._load_credentials()
        print("[MT5Connection.__init__] Инициализация завершена")

    def _start_connection_monitor(self):
        print("[MT5Connection._start_connection_monitor] Запуск мониторинга подключения...")
        if self._monitor_thread and self._monitor_thread.is_alive():
            print("[MT5Connection._start_connection_monitor] Мониторинг уже запущен, пропускаем")
            return

        self._monitoring_active = True
        self._monitor_thread = threading.Thread(
            target=self._connection_monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()
        print(f"[MT5Connection._start_connection_monitor] Поток мониторинга запущен с ID: {self._monitor_thread.ident}")

    def _connection_monitor_loop(self):
        print("[MT5Connection._connection_monitor_loop] Запуск цикла мониторинга...")
        interval = self.json_manager.get_monitor_interval()
        print(f"[MT5Connection._connection_monitor_loop] Интервал проверки: {interval} секунд")
        
        while self._monitoring_active:
            time.sleep(interval)
            print("[MT5Connection._connection_monitor_loop] Проверка подключения...")
            if not self.is_connected():
                print("[MT5Connection._connection_monitor_loop] Обнаружено отключение от MT5. Попытка переподключения...")
                self.reconnect_with_strategy()
            else:
                print("[MT5Connection._connection_monitor_loop] Подключение активно")

    def reconnect_with_strategy(self) -> bool:
        """
        Переподключение с использованием стратегии задержек.
        :return: успех подключения
        """
        print("[MT5Connection.reconnect_with_strategy] Начало переподключения...")
        delays = self.json_manager.get_reconnection_delays()
        print(f"[MT5Connection.reconnect_with_strategy] Задержки: {delays}")

        for attempt, delay in enumerate(delays, 1):
            if delay > 0:
                print(f"[MT5Connection.reconnect_with_strategy] Ожидание {delay} секунд перед попыткой {attempt}...")
                time.sleep(delay)

            print(f"[MT5Connection.reconnect_with_strategy] Попытка переподключения {attempt}/{len(delays)}...")
            if self.connect():
                print("[MT5Connection.reconnect_with_strategy] Переподключение успешно!")
                return True
            else:
                print(f"[MT5Connection.reconnect_with_strategy] Попытка {attempt} не удалась")

        print("[MT5Connection.reconnect_with_strategy] Все попытки переподключения исчерпаны")
        return False

    def stop_monitoring(self):
        print("[MT5Connection.stop_monitoring] Остановка мониторинга подключения...")
        self._monitoring_active = False
        if self._monitor_thread:
            print(f"[MT5Connection.stop_monitoring] Ожидание завершения потока {self._monitor_thread.ident}...")
            self._monitor_thread.join(timeout=2)
            if self._monitor_thread.is_alive():
                print("[MT5Connection.stop_monitoring] Поток не завершился за 2 секунды")
            else:
                print("[MT5Connection.stop_monitoring] Поток мониторинга успешно завершен")
        print("[MT5Connection.stop_monitoring] Мониторинг остановлен")

    def _load_credentials(self):
        print("[MT5Connection._load_credentials] Загрузка учетных данных...")
        credentials = self.json_manager.load_credentials()
        print(f"[MT5Connection._load_credentials] Загруженные данные: {credentials}")

        if not credentials:
            print("[MT5Connection._load_credentials] Файл credentials.json не найден или пуст.")
            self.login = None
            self.password = None
            self.server = None
            return

        try:
            self.login = int(credentials.get('login')) if credentials.get('login') else None
            self.password = credentials.get('password')
            self.server = credentials.get('server')

            print(f"[MT5Connection._load_credentials] Логин: {self.login}")
            print(f"[MT5Connection._load_credentials] Сервер: {self.server}")
            print(f"[MT5Connection._load_credentials] Пароль: {'*' * len(str(self.password)) if self.password else 'None'}")

            if not all([self.login, self.password, self.server]):
                print("[MT5Connection._load_credentials] Ошибка: не все данные указаны")
                self.login = None
                self.password = None
                self.server = None
            else:
                print("[MT5Connection._load_credentials] Все учетные данные загружены корректно")
                
        except ValueError:
            print("[MT5Connection._load_credentials] Ошибка: некорректный формат логина")
            self.login = None
            self.password = None
            self.server = None
            
    def connect(self): # Подключение к MT5
        print("[MT5Connection.connect] Попытка подключения к MT5...")
        print(f"[MT5Connection.connect] Логин: {self.login}")
        print(f"[MT5Connection.connect] Сервер: {self.server}")
        print(f"[MT5Connection.connect] Пароль: {'*' * len(str(self.password)) if self.password else 'None'}")
        
        try:
            # Проверяем наличие всех необходимых параметров
            if not all([self.login, self.password, self.server]):
                print("[MT5Connection.connect] Ошибка: не все учетные данные указаны")
                return False
            
            # Передаём именованные аргументы
            print("[MT5Connection.connect] Вызов mt5.initialize()...")
            if not mt5.initialize(
                login=self.login,
                server=self.server,
                password=self.password
            ):
                print("[MT5Connection.connect] Ошибка подключения с учетными данными")
                error_code, error_desc = mt5.last_error()
                print(f"[MT5Connection.connect] Код ошибки: {error_code}, Описание: {error_desc}")
                return False
            
            self._is_connected = True
            print("[MT5Connection.connect] Подключение успешно!")
            return True

        except Exception as e:
            print(f"[MT5Connection.connect] Произошла ошибка при подключении: {str(e)}")
            return False

    def disconnect(self): # Отключение от MT5
        print("[MT5Connection.disconnect] Отключение от MT5...")
        self.stop_monitoring()
        if self._is_connected:
            try:
                print("[MT5Connection.disconnect] Вызов mt5.shutdown()...")
                mt5.shutdown()
                self._is_connected = False
                print("[MT5Connection.disconnect] Отключение от MT5 выполнено")
            except Exception as e:
                print(f"[MT5Connection.disconnect] Ошибка при отключении: {str(e)}")
        else:
            print("[MT5Connection.disconnect] Не было подключено к MT5")

    def get_terminal_info(self): # Получение информации о терминале MT5
        print("[MT5Connection.get_terminal_info] Получение информации о терминале...")
        
        if not self._is_connected:
            print("[MT5Connection.get_terminal_info] Не подключено к MT5")
            return None
            
        try:
            info = mt5.terminal_info()
            if info:
                print(f"[MT5Connection.get_terminal_info] Получена информация: build={info.build}, name={info.name}")
                return {
                    'build': info.build,
                    'name': info.name
                }
            else:
                print("[MT5Connection.get_terminal_info] Информация о терминале не получена")
                return None
        except Exception as e:
            print(f"[MT5Connection.get_terminal_info] Ошибка получения информации о терминале: {e}")
            return None

    def get_account_info(self): # Получение информации о торговом счёте
        print("[MT5Connection.get_account_info] Получение информации о счете...")
        
        if not self._is_connected:
            print("[MT5Connection.get_account_info] Не подключено к MT5")
            return None
            
        try:
            account_info = mt5.account_info()
            if account_info:
                print(f"[MT5Connection.get_account_info] Получена информация: login={account_info.login}, balance={account_info.balance}")
                return {
                    'login': account_info.login,
                    'balance': account_info.balance,
                    'equity': account_info.equity,
                    'currency': account_info.currency
                }
            else:
                print("[MT5Connection.get_account_info] Информация о счете не получена")
                return None
        except Exception as e:
            print(f"[MT5Connection.get_account_info] Ошибка получения информации о счёте: {e}")
            return None

    def is_connected(self) -> bool: # Проверка состояния подключения
        print("[MT5Connection.is_connected] Проверка состояния подключения...")
        
        if not self._is_connected:
            print("[MT5Connection.is_connected] Не подключено (флаг _is_connected = False)")
            return False
            
        try:
            info = mt5.terminal_info()
            if info is not None:
                print("[MT5Connection.is_connected] Подключение активно")
                return True
            else:
                print("[MT5Connection.is_connected] Подключение потеряно")
                self._is_connected = False
                return False
        except Exception as e:
            print(f"[MT5Connection.is_connected] Ошибка проверки: {e}")
            self._is_connected = False
            return False

    def symbols_get(self): # Получает все доступные символы из MT5
        print("[MT5Connection.symbols_get] Получение списка символов...")
        
        try:
            symbols = mt5.symbols_get()
            if symbols:
                print(f"[MT5Connection.symbols_get] Всего символов: {len(symbols)}")
            else:
                print("[MT5Connection.symbols_get] Символы не получены")
            return symbols
        except Exception as e:
            print(f"[MT5Connection.symbols_get] Ошибка при получении символов: {str(e)}")
            return []

    def simboi_info(self): # показ информации символа символа GBPUSD в MarketWatch
        print("[MT5Connection.simboi_info] Получение информации о символе GBPUSD...")
        
        selected=mt5.symbol_select("GBPUSD",True)
        if not selected:
            print("[MT5Connection.simboi_info] Failed to select GBPUSD")
            mt5.shutdown()
            quit()
        else:
            print("[MT5Connection.simboi_info] Символ GBPUSD выбран")
            
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
        else:
            print("[MT5Connection.simboi_info] Информация о символе GBPUSD не получена")
        
    def get_historical_data(self, symbol_name, timeframe, start_date, end_date):
        """
        :param start_date: datetime объект с tzinfo (UTC)
        :param end_date: datetime объект с tzinfo (UTC)
        """
        print(f"[MT5Connection.get_historical_data] Получение исторических данных для {symbol_name}...")
        print(f"[MT5Connection.get_historical_data] Timeframe: {timeframe}")
        print(f"[MT5Connection.get_historical_data] Начало: {start_date}")
        print(f"[MT5Connection.get_historical_data] Конец: {end_date}")
        
        if not self._is_connected:
            print("[MT5Connection.get_historical_data] Не подключено к MT5")
            return None

        try:
            # Преобразуем UTC datetime в naive datetime для MT5 (MT5 ожидает naive в UTC)
            if start_date.tzinfo:
                start_date_naive = start_date.replace(tzinfo=None)
                print(f"[MT5Connection.get_historical_data] start_date преобразован в naive: {start_date_naive}")
            else:
                start_date_naive = start_date
                print(f"[MT5Connection.get_historical_data] start_date уже naive: {start_date_naive}")

            if end_date.tzinfo:
                end_date_naive = end_date.replace(tzinfo=None)
                print(f"[MT5Connection.get_historical_data] end_date преобразован в naive: {end_date_naive}")
            else:
                end_date_naive = end_date
                print(f"[MT5Connection.get_historical_data] end_date уже naive: {end_date_naive}")

            print("[MT5Connection.get_historical_data] Вызов mt5.copy_rates_range()...")
            rates = mt5.copy_rates_range(
                symbol_name,
                timeframe,
                start_date_naive,
                end_date_naive
            )

            if rates is None or len(rates) == 0:
                print(f"[MT5Connection.get_historical_data] Нет исторических данных для {symbol_name}")
                return None

            print(f"[MT5Connection.get_historical_data] Получено {len(rates)} записей")
            df = pd.DataFrame(rates)
            # В примере используется utc=True, что эквивалентно
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
            print(f"[MT5Connection.get_historical_data] DataFrame создан: {df.shape}")
            return df

        except Exception as e:
            print(f"[MT5Connection.get_historical_data] Ошибка при получении исторических данных для {symbol_name}: {str(e)}")
            return None