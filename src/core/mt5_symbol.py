import os
import pathlib

class MT5Symbol:
    def __init__(self, symbol_info, history_dir='F:/MT5-soft/history'):
        self.name = symbol_info.name
        self.description = symbol_info.description
        self.digits = symbol_info.digits
        self.spread = symbol_info.spread
        self.trade_mode = symbol_info.trade_mode
        self.volume_min = symbol_info.volume_min
        self.volume_step = symbol_info.volume_step

        # Вычисляем lot_size через point_value
        self.lot_size = symbol_info.point_value if hasattr(symbol_info, 'point_value') else 1.0

        # Для margin_required используем margin_initial или 0
        self.margin_required = (
            symbol_info.margin_initial
            if hasattr(symbol_info, 'margin_initial')
            else 0.0
        )

        self.history_dir = pathlib.Path(history_dir)


    def _is_valid_filename(self, name): # Проверка, допустимо ли имя для использования в качестве имени директории
        invalid_chars = set('<>:"/|?\\*')
        return not any(char in invalid_chars for char in name)

    def check_directory_exists(self): # Проверка существование директории для конкретного символа
        """
        Возвращает True/False, обрабатывает возможные ошибки.
        """
        symbol_path = self.history_dir / self.name
        try:
            return symbol_path.exists()
        except OSError as e:
            print(f"Ошибка при проверке существования директории {symbol_path}: {e}")
            return False

    def create_directory(self): # Создание директории для конкретного символа
        """
        Возвращает:
            True — если директория создана,
            False — если уже существовала или произошла ошибка.
        """
        if not self._is_valid_filename(self.name):
            print(f"Недопустимое имя символа для создания директории: '{self.name}'")
            return False

        symbol_path = self.history_dir / self.name

        try:
            symbol_path.mkdir(parents=True, exist_ok=True)
            print(f"Создана директория: {symbol_path}")
            return True
        except (OSError, PermissionError) as e:
            print(f"Ошибка создания директории {symbol_path}: {str(e)}")
            return False

    @classmethod
    def create_all_directories(cls, symbols, symbols_directories_setting):
        """
        Создаёт директории для символов согласно настройке symbols_directories.
        :param symbols: список объектов MT5Symbol
        :param symbols_directories_setting: значение из settings.json ('all' или список символов)
        :return: словарь с результатами (создано/уже было/ошибка)
        """

        # Если настройка 'off', сразу возвращаем пустые результаты — директории не создаём
        if symbols_directories_setting == 'off':
            print("Создание директорий отключено (symbols_directories = 'off')")
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
        else:
            # Фильтруем символы по списку из настройки
            symbol_names = {symbol.name for symbol in symbols}
            symbols_to_process = [
                symbol for symbol in symbols
                if symbol.name in symbols_directories_setting
            ]
            # Проверяем, все ли указанные символы существуют в MT5
            requested_names = set(symbols_directories_setting)
            missing = requested_names - symbol_names
            if missing:
                print(f"Предупреждение: следующие символы не найдены в MT5: {missing}")

        for symbol in symbols_to_process:
            if symbol.check_directory_exists():
                results['already_existed'].append(symbol.name)
            else:
                success = symbol.create_directory()
                if success:
                    results['created'].append(symbol.name)
                else:
                    results['errors'].append(symbol.name)

        return results

    

    
