import os
import pathlib

class MT5Symbol:
    def __init__(self, symbol_info, history_dir='F:/MT5-soft/history'):
        print("[MT5Symbol.__init__] Инициализация MT5Symbol...")
        print(f"[MT5Symbol.__init__] Symbol info: {symbol_info}")
        
        self.name = symbol_info.name
        print(f"[MT5Symbol.__init__] Имя символа: {self.name}")
        
        self.description = symbol_info.description
        print(f"[MT5Symbol.__init__] Описание: {self.description}")
        
        self.digits = symbol_info.digits
        print(f"[MT5Symbol.__init__] Digits: {self.digits}")
        
        self.spread = symbol_info.spread
        print(f"[MT5Symbol.__init__] Spread: {self.spread}")
        
        self.trade_mode = symbol_info.trade_mode
        print(f"[MT5Symbol.__init__] Trade mode: {self.trade_mode}")
        
        self.volume_min = symbol_info.volume_min
        print(f"[MT5Symbol.__init__] Volume min: {self.volume_min}")
        
        self.volume_step = symbol_info.volume_step
        print(f"[MT5Symbol.__init__] Volume step: {self.volume_step}")

        # Вычисляем lot_size через point_value
        self.lot_size = symbol_info.point_value if hasattr(symbol_info, 'point_value') else 1.0
        print(f"[MT5Symbol.__init__] Lot size: {self.lot_size}")

        # Для margin_required используем margin_initial или 0
        self.margin_required = (
            symbol_info.margin_initial
            if hasattr(symbol_info, 'margin_initial')
            else 0.0
        )
        print(f"[MT5Symbol.__init__] Margin required: {self.margin_required}")

        self.history_dir = pathlib.Path(history_dir)
        print(f"[MT5Symbol.__init__] History dir: {self.history_dir}")
        print("[MT5Symbol.__init__] Инициализация завершена")


    def _is_valid_filename(self, name): # Проверка, допустимо ли имя для использования в качестве имени директории
        print(f"[MT5Symbol._is_valid_filename] Проверка имени: {name}")
        invalid_chars = set('<>:"/|?\\*')
        result = not any(char in invalid_chars for char in name)
        print(f"[MT5Symbol._is_valid_filename] Результат проверки: {result}")
        return result

    def check_directory_exists(self): # Проверка существование директории для конкретного символа
        """
        Возвращает True/False, обрабатывает возможные ошибки.
        """
        print(f"[MT5Symbol.check_directory_exists] Проверка существования директории для {self.name}...")
        symbol_path = self.history_dir / self.name
        print(f"[MT5Symbol.check_directory_exists] Путь: {symbol_path}")
        
        try:
            exists = symbol_path.exists()
            print(f"[MT5Symbol.check_directory_exists] Директория существует: {exists}")
            return exists
        except OSError as e:
            print(f"[MT5Symbol.check_directory_exists] Ошибка при проверке существования директории {symbol_path}: {e}")
            return False

    def create_directory(self): # Создание директории для конкретного символа
        """
        Возвращает:
            True — если директория создана,
            False — если уже существовала или произошла ошибка.
        """
        print(f"[MT5Symbol.create_directory] Создание директории для {self.name}...")
        
        if not self._is_valid_filename(self.name):
            print(f"[MT5Symbol.create_directory] Недопустимое имя символа для создания директории: '{self.name}'")
            return False

        symbol_path = self.history_dir / self.name
        print(f"[MT5Symbol.create_directory] Путь для создания: {symbol_path}")

        try:
            symbol_path.mkdir(parents=True, exist_ok=True)
            print(f"[MT5Symbol.create_directory] Создана директория: {symbol_path}")
            return True
        except (OSError, PermissionError) as e:
            print(f"[MT5Symbol.create_directory] Ошибка создания директории {symbol_path}: {str(e)}")
            return False

    @classmethod
    def create_all_directories(cls, symbols, symbols_directories_setting):
        """
        Создаёт директории для символов согласно настройке symbols_directories.
        :param symbols: список объектов MT5Symbol
        :param symbols_directories_setting: значение из settings.json ('all' или список символов)
        :return: словарь с результатами (создано/уже было/ошибка)
        """
        print("[MT5Symbol.create_all_directories] Начало создания директорий...")
        print(f"[MT5Symbol.create_all_directories] Настройка: {symbols_directories_setting}")
        print(f"[MT5Symbol.create_all_directories] Количество символов: {len(symbols)}")

        # Если настройка 'off', сразу возвращаем пустые результаты — директории не создаём
        if symbols_directories_setting == 'off':
            print("[MT5Symbol.create_all_directories] Создание директорий отключено (symbols_directories = 'off')")
            return {
                'created': [],
                'already_existed': [],
                'errors': []
            }

        results = {
            'created': [],
            'already_existed': [],
            'errors': []
        }

        # Определяем, какие символы обрабатывать
        if symbols_directories_setting == 'all':
            symbols_to_process = symbols
            print(f"[MT5Symbol.create_all_directories] Обработка всех символов: {len(symbols_to_process)}")
        else:
            # Фильтруем символы по списку из настройки
            symbol_names = {symbol.name for symbol in symbols}
            print(f"[MT5Symbol.create_all_directories] Доступные символы: {symbol_names}")
            
            symbols_to_process = [
                symbol for symbol in symbols
                if symbol.name in symbols_directories_setting
            ]
            print(f"[MT5Symbol.create_all_directories] Отфильтровано символов: {len(symbols_to_process)}")
            
            # Проверяем, все ли указанные символы существуют в MT5
            requested_names = set(symbols_directories_setting)
            missing = requested_names - symbol_names
            if missing:
                print(f"[MT5Symbol.create_all_directories] Предупреждение: следующие символы не найдены в MT5: {missing}")

        print(f"[MT5Symbol.create_all_directories] Начало обработки {len(symbols_to_process)} символов...")
        
        for symbol in symbols_to_process:
            print(f"[MT5Symbol.create_all_directories] Обработка символа: {symbol.name}")
            
            if symbol.check_directory_exists():
                print(f"[MT5Symbol.create_all_directories] Директория уже существует для {symbol.name}")
                results['already_existed'].append(symbol.name)
            else:
                print(f"[MT5Symbol.create_all_directories] Директория не существует, создаем для {symbol.name}")
                success = symbol.create_directory()
                if success:
                    print(f"[MT5Symbol.create_all_directories] Директория успешно создана для {symbol.name}")
                    results['created'].append(symbol.name)
                else:
                    print(f"[MT5Symbol.create_all_directories] Ошибка создания директории для {symbol.name}")
                    results['errors'].append(symbol.name)

        print(f"[MT5Symbol.create_all_directories] Итоги: создано={len(results['created'])}, уже существовало={len(results['already_existed'])}, ошибок={len(results['errors'])}")
        return results