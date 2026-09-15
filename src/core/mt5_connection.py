# mt5_connection.py

import MetaTrader5 as mt5
import pandas as pd
import threading
import time
from datetime import datetime, timezone

class MT5Connection: # Упрощённый класс для подключения к MetaTrader 5

    def __init__(self, credentials: dict, monitor_interval: int = 30, 
                 reconnection_delays: list = None): # Получаем абсолютный путь к файлу
        print("[MT5Connection.__init__] Инициализация MT5Connection...")

        self.credentials = credentials
        self.monitor_interval = monitor_interval
        self.reconnection_delays = reconnection_delays or [0, 10, 30, 60, 300]

        self._is_connected = False


        # Инициализируем атрибуты учётных данных
        self.login = None
        self.password = None
        self.server = None
        
        # Разбираем переданный словарь credentials
        self._load_credentials()
        print("[MT5Connection.__init__] Инициализация завершена")

    def _load_credentials(self):
        """
        Разбирает словарь self.credentials на отдельные атрибуты:
        self.login, self.password, self.server.
        
        Данные приходят из JsonManager._credentials_cache через
        SystemOrchestrator, поэтому повторное чтение файла не требуется.
        """
        print("[MT5Connection._load_credentials] Разбор credentials...")
        credentials = self.credentials
        print(f"[MT5Connection._load_credentials] Полученные данные: {credentials}")

        if not credentials:
            print("[MT5Connection._load_credentials] credentials пуст или None")
            self.login = None
            self.password = None
            self.server = None
            return

        try:
            # login приводим к int, т.к. в JSON он хранится строкой
            raw_login = credentials.get('login')
            self.login = int(raw_login) if raw_login else None
            self.password = credentials.get('password')
            self.server = credentials.get('server')

            print(f"[MT5Connection._load_credentials] Логин: {self.login}")
            print(f"[MT5Connection._load_credentials] Сервер: {self.server}")
            print(
                f"[MT5Connection._load_credentials] Пароль: "
                f"{'*' * len(str(self.password)) if self.password else 'None'}"
            )

            # Проверяем, что все три поля заполнены
            if not all([self.login, self.password, self.server]):
                print("[MT5Connection._load_credentials] Ошибка: не все данные указаны")
                self.login = None
                self.password = None
                self.server = None
            else:
                print("[MT5Connection._load_credentials] Все данные корректны")

        except (ValueError, TypeError) as e:
            print(f"[MT5Connection._load_credentials] Ошибка формата login: {e}")
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
    #    self.stop_monitoring()
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

    def get_server_time(self):
        """
        Возвращает текущее время сервера MT5 (naive datetime).
        Используется для расчёта диапазонов запросов.
        """
        print("[MT5Connection.get_server_time] Получение серверного времени...")

        if not self._is_connected:
            print("[MT5Connection.get_server_time] Не подключено к MT5")
            return None

        try:
            # Берём последний тик любого доступного символа
            # либо terminal_info().time — но надёжнее через тик
            tick = mt5.symbol_info_tick("GBPUSD")
            print(f"[MT5Connection.get_server_time] {tick}")
            if tick is None or tick.time == 0:
                # fallback — время терминала
                info = mt5.terminal_info()
                if info is None:
                    print("[MT5Connection.get_server_time] Не удалось получить время")
                    return None
                server_time = datetime.fromtimestamp(info.time, tz=timezone.utc).replace(tzinfo=None)

            else:
                server_time = datetime.fromtimestamp(tick.time, tz=timezone.utc).replace(tzinfo=None)




            print(f"\n\n\n\n[MT5Connection.get_server_time] Серверное время: server_time {server_time}")
            return server_time

        except Exception as e:
            print(f"[MT5Connection.get_server_time] Ошибка: {e}")
            return None

    def warmup_symbol(self, symbol: str, timeframe: int, count: int = 2000) -> bool:
        """
        «Прогревает» историю символа в MT5.
        1. Добавляет символ в Market Watch (symbol_select).
        2. Запрашивает последние `count` баров через copy_rates_from_pos —
           это заставляет MT5 подгрузить историю в терминал.

        :param symbol: имя символа
        :param timeframe: таймфрейм MT5
        :param count: сколько последних баров запросить
        :return: True, если прогрев успешен
        """
        print(f"[MT5Connection.warmup_symbol] Прогрев {symbol} (timeframe={timeframe}, count={count})...")

        if not self._is_connected:
            print("[MT5Connection.warmup_symbol] Не подключено к MT5")
            return False

        try:
            # 1. Добавляем символ в Market Watch
            selected = mt5.symbol_select(symbol, True)
            if not selected:
                print(f"[MT5Connection.warmup_symbol] Не удалось выбрать {symbol} в Market Watch")
                return False
            print(f"[MT5Connection.warmup_symbol] {symbol} добавлен в Market Watch")

            # 2. Запрашиваем последние бары — это заставит MT5 подгрузить историю
            rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
            if rates is None or len(rates) == 0:
                print(f"[MT5Connection.warmup_symbol] {symbol}: не удалось получить бары для прогрева")
                return False

            print(f"[MT5Connection.warmup_symbol] {symbol}: прогрето {len(rates)} баров")
            return True

        except Exception as e:
            print(f"[MT5Connection.warmup_symbol] Ошибка прогрева {symbol}: {e}")
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

    
        
    def get_historical_data(self, symbol_name, timeframe, start_date, end_date):
        """
        Получение исторических данных из MT5 с полной обработкой.
        
        :param symbol_name: название символа
        :param timeframe: таймфрейм MT5 (mt5.TIMEFRAME_M1 и т.д.)
        :param start_date: datetime объект с tzinfo (UTC)
        :param end_date: datetime объект с tzinfo (UTC)
        :return: DataFrame с историческими данными или None
        """
        print(f"[MT5Connection.get_historical_data] Получение исторических данных для {symbol_name}...")
        print(f"[MT5Connection.get_historical_data] Timeframe: {timeframe}")
        print(f"[MT5Connection.get_historical_data] Начало: {start_date}")
        print(f"[MT5Connection.get_historical_data] Конец: {end_date}")
        
        # 1. Проверка подключения
        if not self._is_connected:
            print("[MT5Connection.get_historical_data] Не подключено к MT5")
            return None
        
        try:
            # 2. Обработка datetime — оставляем naive, MT5 работает в серверном времени
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

            # 3. Убираем секунды и микросекунды для корректной работы MT5
            start_date_naive = start_date_naive.replace(second=0, microsecond=0)
            end_date_naive = end_date_naive.replace(second=0, microsecond=0)
            print(f"[MT5Connection.get_historical_data] start_date после обработки: {start_date_naive}")
            print(f"[MT5Connection.get_historical_data] end_date после обработки: {end_date_naive}")

            # 4. Вызов MT5 API
            print("[MT5Connection.get_historical_data] Вызов mt5.copy_rates_range()...")
            rates = mt5.copy_rates_range(
                symbol_name,
                timeframe,
                start_date_naive,
                end_date_naive
            )

            # 4.1. Fallback: если диапазон пустой — пробуем через copy_rates_from_pos
            if rates is None or len(rates) == 0:
                print(f"[MT5Connection.get_historical_data] copy_rates_range пуст, "
                      f"пробуем copy_rates_from_pos (fallback)...")

                # Запрашиваем N последних баров и фильтруем по диапазону
                fallback_count = 10000  # достаточно для ~7 дней M1
                rates_all = mt5.copy_rates_from_pos(symbol_name, timeframe, 0, fallback_count)

                if rates_all is not None and len(rates_all) > 0:
                    # Фильтруем по диапазону (time — в секундах, Unix timestamp)
                    start_ts = int((start_date_naive - datetime(1970, 1, 1)).total_seconds())
                    end_ts = int((end_date_naive - datetime(1970, 1, 1)).total_seconds())
                    mask = (rates_all['time'] >= start_ts) & (rates_all['time'] <= end_ts)
                    rates = rates_all[mask]
                    print(f"[MT5Connection.get_historical_data] Fallback: "
                          f"из {len(rates_all)} баров отфильтровано {len(rates)}")
                else:
                    print(f"[MT5Connection.get_historical_data] Fallback тоже пуст")

            # 5. Проверка результата
            if rates is None or len(rates) == 0:
                print(f"[MT5Connection.get_historical_data] Нет исторических данных для {symbol_name}")
                return None

            print(f"[MT5Connection.get_historical_data] Получено {len(rates)} записей")

            # 6. Создание DataFrame
            df = pd.DataFrame(rates)

            # 7. Конвертация времени — БЕЗ utc=True, оставляем серверное время MT5
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_localize(None)        
            # 8. Сортировка колонок в нужном порядке
            cols_order = ['time', 'open', 'high', 'low', 'close', 
                        'tick_volume', 'spread', 'real_volume']
            
            # Проверяем наличие всех колонок
            available_cols = [col for col in cols_order if col in df.columns]
            if available_cols:
                df = df[available_cols]
                print(f"[MT5Connection.get_historical_data] Колонки отсортированы: {available_cols}")
            else:
                print("[MT5Connection.get_historical_data] Предупреждение: не все ожидаемые колонки найдены")
            
            # 9. Добавляем колонку symbol
            df['symbol'] = symbol_name
            print(f"[MT5Connection.get_historical_data] Добавлена колонка symbol: {symbol_name}")
            
            # 10. Сортировка данных по времени
            df = df.sort_values('time')
            print("[MT5Connection.get_historical_data] Данные отсортированы по времени")
            
            print(f"[MT5Connection.get_historical_data] Итоговый DataFrame: {df.shape}")
            print(f"[MT5Connection.get_historical_data] Колонки: {df.columns.tolist()}")
            
            return df
        
        except Exception as e:
            print(f"[MT5Connection.get_historical_data] Ошибка при получении исторических данных для {symbol_name}: {str(e)}")
            return None

    def get_last_closed_bar(self, symbol_name, timeframe):
        """
        Возвращает последний ЗАКРЫТЫЙ бар по символу.
        Использует copy_rates_from_pos(symbol, timeframe, 1, 1) —
        позиция 1 = предыдущий бар (не текущий формирующийся).

        :param symbol_name: имя символа
        :param timeframe: таймфрейм MT5
        :return: DataFrame с 1 баром или None
        """
        print(f"[MT5Connection.get_last_closed_bar] Получение последнего закрытого бара {symbol_name}...")

        if not self._is_connected:
            print("[MT5Connection.get_last_closed_bar] Не подключено к MT5")
            return None

        try:
            rates = mt5.copy_rates_from_pos(symbol_name, timeframe, 1, 1)

            if rates is None or len(rates) == 0:
                print(f"[MT5Connection.get_last_closed_bar] Нет бара для {symbol_name}")
                return None

            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_localize(None)
            cols_order = ['time', 'open', 'high', 'low', 'close',
                        'tick_volume', 'spread', 'real_volume']
            available_cols = [col for col in cols_order if col in df.columns]
            df = df[available_cols]

            df['symbol'] = symbol_name

            print(f"[MT5Connection.get_last_closed_bar] Получен бар: {df['time'].iloc[0]}")
            return df

        except Exception as e:
            print(f"[MT5Connection.get_last_closed_bar] Ошибка: {e}")
            return None