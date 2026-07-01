# main.py


from src.core.mt5_connection import MT5Connection
from src.utils.symbol_utils import group_and_display_symbols
from src.core.mt5_symbol import MT5Symbol
from src.utils.history_manager import HistoryManager
from src.utils.json_manager import JsonManager



def main():
    json_manager = JsonManager()
    settings = json_manager.load_settings()
    display_settings = json_manager.get_display_settings()
    history_settings = json_manager.get_history_settings()
    symbols_directories = json_manager.get_symbols_directories()
    shutdown_command = json_manager.get_shutdown_command()

    mt5_conn = MT5Connection(json_manager)

    try:
        if mt5_conn.connect():
            print("Успешно подключились к MT5")
            mt5_conn._start_connection_monitor()  # Запускаем мониторинг подключения
        else:
            print("Не удалось подключиться к MT5")
            return

        # Основной цикл с проверкой команды отключения
        print(f"\nСистема запущена. Для отключения введите: '{shutdown_command}'")

        while True:
            try:
                # Выполняем все операции
                _execute_trading_operations(
                    mt5_conn,
                    json_manager,
            display_settings,
            history_settings,
            symbols_directories
        )

                # Проверяем команду отключения (ввод пользователя)
                user_input = input("\nВведите команду (или нажмите Enter для продолжения): ").strip()
                if user_input.lower() == shutdown_command.lower():
                    print("Получена команда отключения системы...")
                    break

                # Если пользователь ничего не ввёл, просто продолжаем цикл
                if not user_input:
                    continue

                # Здесь можно добавить обработку других команд в будущем
                if user_input:
                    print(f"Неизвестная команда: {user_input}. Для отключения используйте: '{shutdown_command}'")

            except KeyboardInterrupt:
                print("\nПрервано пользователем")
                break
            except Exception as e:
                print(f"Ошибка в операционном цикле: {str(e)}")
                # Продолжаем работу — мониторинг и переподключение продолжат работать

    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    except Exception as e:
        print(f"Критическая ошибка в основном цикле: {str(e)}")
    finally:
        mt5_conn.disconnect()  # Гарантированное отключение и остановка мониторинга
        print("Система корректно завершена.")

def _execute_trading_operations(mt5_conn, json_manager, display_settings, history_settings, symbols_directories):
    """Выполняет все торговые операции."""
    # Информация о терминале
    if display_settings.get('terminal_info', True):
        terminal_info = mt5_conn.get_terminal_info()
        if terminal_info:
            print("\nИнформация о терминале:")
            print(f"  Версия терминала: {terminal_info['build']}")
            print(f"  Название: {terminal_info['name']}")

    # Информация о счёте
    if display_settings.get('account_info', True):
        account_info = mt5_conn.get_account_info()
        if account_info:
            print("\nИнформация о счёте:")
            print(f"  Логин: {account_info['login']}")
            print(f"  Баланс: {account_info['balance']} {account_info['currency']}")
            print(f"  Эквити: {account_info['equity']} {account_info['currency']}")

    # Работа с символами
    if display_settings.get('all_symbols', True):
        symbols = mt5_conn.symbols_get()
        if symbols:
            json_manager.save_symbols_to_json(symbols)
            if display_settings.get('grouped_symbols', True):
                group_and_display_symbols(symbols)
            mt5_symbols = [MT5Symbol(symbol) for symbol in symbols]
            results = MT5Symbol.create_all_directories(mt5_symbols, symbols_directories)
            print(f"Создано директорий: {len(results['created'])}")
            print(f"Уже существовали: {len(results['already_existed'])}")
            if results['errors']:
                print(f"Ошибки при создании: {len(results['errors'])}")
        else:
            print("Список символов пуст")

    # Проверка согласованности настроек символов
    history_symbols = history_settings.get('symbols', [])
    if symbols_directories != 'all' and set(history_symbols) != set(symbols_directories):
        print("ПРЕДУПРЕЖДЕНИЕ: Настройки 'symbols' и 'symbols_directories' не согласованы!")
        print(f"  'symbols': {history_symbols}")
        print(f"  'symbols_directories': {symbols_directories}")
        print("  Рекомендуется привести их к одинаковым значениям.")

    # Загрузка исторических данных (если включено в настройках)
    if history_settings.get('enabled', False):
        print("\n" + "="*60)
        print("ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ")
        print("="*60)
        HistoryManager.download_and_save_history(mt5_conn, history_settings)


if __name__ == "__main__":
    main()