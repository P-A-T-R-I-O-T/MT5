# main.py

import threading
import time
from typing import Callable, Any, Dict

from src.core.worker_mt5_task import WorkerMT5Task, MT5Task
from src.utils.symbol_utils import group_and_display_symbols
from src.core.mt5_symbol import MT5Symbol
from src.utils.json_manager import JsonManager


def main():
    json_manager = JsonManager()
    settings = json_manager.load_settings()
    display_settings = json_manager.get_display_settings()
    history_settings = json_manager.get_history_settings()
    symbols_directories = json_manager.get_symbols_directories()
    shutdown_command = json_manager.get_shutdown_command()
    
    # Проверяем настройки реального времени
    realtime_settings = history_settings.get('realtime', {})
    realtime_enabled = realtime_settings.get('enabled', False)

    # Инициализируем воркер
    worker = WorkerMT5Task(json_manager)
    worker.start()

    print("\n[Main] Воркер MT5 запущен. Поток задач активен.")

    # Флаг для управления главным циклом
    system_running = True

    # 1) Сначала отправляем задачу на подключение
    def on_connect_result(res: Dict[str, Any]):
        nonlocal system_running
        connected = res.get("connected", False)
        if connected:
            print("[Main] MT5 подключён. Начинаем выполнение операций.")
            _schedule_initial_operations(worker, display_settings, history_settings, symbols_directories)
            
            # Запускаем сбор данных в реальном времени, если включен
            if realtime_enabled:
                print("[Main] Запуск сбора данных в реальном времени...")
                worker.submit(MT5Task(
                    task_type="start_realtime",
                    result_callback=lambda res: print("[Main] Сбор реальных данных запущен"),
                    error_callback=lambda e: print(f"[Main] Ошибка запуска сбора: {e}")
                ))
            else:
                print("[Main] Сбор реальных данных отключен в настройках")
        else:
            print("[Main] Не удалось подключиться к MT5. Остановка.")
            system_running = False
            worker.stop()

    worker.submit(MT5Task(
        task_type="connect",
        result_callback=on_connect_result,
        error_callback=lambda e: print(f"[Main] Ошибка при подключении: {e}")
    ))

    # 2) Поток ввода команд пользователя
    input_thread = threading.Thread(
        target=_user_input_loop,
        args=(worker, shutdown_command),
        daemon=True
    )
    input_thread.start()

    try:
        # Держим главный поток живым, пока работает система
        while system_running and (worker._is_running or input_thread.is_alive()):
            time.sleep(1)
            
            # Проверяем, не был ли установлен флаг остановки в воркере
            if worker._stop_flag:
                print("[Main] Обнаружен флаг остановки, завершаем работу...")
                system_running = False
                break
            
            # Периодически выводим статистику сбора (для отладки)
            if realtime_enabled and worker.realtime_collector:
                # Раз в 30 секунд показываем статистику
                if int(time.time()) % 30 == 0:
                    stats = worker.realtime_collector.get_stats()
                    if stats.get('total_records', 0) > 0:
                        print(f"\n[Main] Статистика сбора: {stats['total_records']} записей, "
                              f"символы: {', '.join(stats['symbols'])}")
                        
    except KeyboardInterrupt:
        print("\n[Main] Прервано пользователем.")
    finally:
        print("[Main] Завершение работы...")
        
        # Останавливаем сбор реального времени
        if worker.realtime_collector:
            print("[Main] Остановка сбора реальных данных...")
            worker.submit(MT5Task(
                task_type="stop_realtime",
                result_callback=lambda res: print("[Main] Сбор реальных данных остановлен"),
                error_callback=lambda e: print(f"[Main] Ошибка остановки сбора: {e}")
            ))
            time.sleep(2)  # Даем время на сохранение буфера
        
        # Останавливаем воркер
        worker.stop()
        print("[Main] Система корректно завершена.")


def _schedule_initial_operations(
    worker: WorkerMT5Task,
    display_settings: dict,
    history_settings: dict,
    symbols_directories: Any
):
    """Планирует начальные операции после подключения."""

    # Терминал
    if display_settings.get('terminal_info', True):
        worker.submit(MT5Task(
            task_type="get_terminal_info",
            result_callback=_handle_terminal_info
        ))

    # Счёт
    if display_settings.get('account_info', True):
        worker.submit(MT5Task(
            task_type="get_account_info",
            result_callback=_handle_account_info
        ))

    # Символы
    if display_settings.get('all_symbols', True):
        worker.submit(MT5Task(
            task_type="get_symbols",
            result_callback=lambda res: _handle_symbols(
                res, display_settings, symbols_directories
            )
        ))

    # История (если включена)
    if history_settings.get('enabled', False):
        worker.submit(MT5Task(
            task_type="download_history",
            payload={"history_settings": history_settings},
            result_callback=lambda res: print("[Main] Загрузка истории завершена.")
        ))


def _handle_terminal_info(res: dict):
    info = res.get("terminal_info")
    if info:
        print("\nИнформация о терминале:")
        print(f" Версия терминала: {info['build']}")
        print(f" Название: {info['name']}")


def _handle_account_info(res: dict):
    acc = res.get("account_info")
    if acc:
        print("\nИнформация о счёте:")
        print(f" Логин: {acc['login']}")
        print(f" Баланс: {acc['balance']} {acc['currency']}")
        print(f" Эквити: {acc['equity']} {acc['currency']}")


def _handle_symbols(res: dict, display_settings: dict, symbols_directories: Any):
    symbols = res.get("symbols", [])
    if not symbols:
        print("\nСписок символов пуст")
        return

    json_manager = JsonManager()
    json_manager.save_symbols_to_json(symbols)

    if display_settings.get('grouped_symbols', True):
        group_and_display_symbols(symbols)
        mt5_symbols = [MT5Symbol(symbol) for symbol in symbols]
        results = MT5Symbol.create_all_directories(mt5_symbols, symbols_directories)

        print(f"\nСоздано директорий: {len(results['created'])}")
        print(f"Уже существовали: {len(results['already_existed'])}")
        if results['errors']:
            print(f"Ошибки при создании: {len(results['errors'])}")
    else:
        print("\nГруппировка и создание директорий отключены.")


def _user_input_loop(worker: WorkerMT5Task, shutdown_command: str):
    """Цикл обработки пользовательского ввода."""
    while True:
        try:
            user_input = input("\nВведите команду: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if user_input.lower() == shutdown_command.lower():
            print("Получена команда отключения системы...")
            # Устанавливаем флаг остановки в воркере
            worker._stop_flag = True
            # Ждем немного, чтобы дать возможность завершиться другим потокам
            time.sleep(0.5)
            break
        elif user_input == "stats":
            # Показываем статистику сбора
            if worker.realtime_collector:
                stats = worker.realtime_collector.get_stats()
                print(f"\nСтатистика сбора данных:")
                print(f"  Всего записей: {stats.get('total_records', 0)}")
                print(f"  Символы: {stats.get('symbols', [])}")
                print(f"  Начало: {stats.get('start_time', 'Нет данных')}")
                print(f"  Конец: {stats.get('end_time', 'Нет данных')}")
                print(f"  Буфер: {stats.get('buffer_size', 0)} записей")
            else:
                print("Сбор данных не запущен")
        elif user_input == "realtime stop":
            if worker.realtime_collector:
                # Останавливаем сбор напрямую, без отправки задачи в очередь
                worker.realtime_collector.stop()
                worker.realtime_collector = None
                print("Сбор данных остановлен")
            else:
                print("Сбор данных не запущен")
        elif user_input == "realtime start":
            # Проверяем настройки перед запуском
            settings = worker.json_manager.load_settings()
            history_settings = settings.get('history', {})
            realtime_settings = history_settings.get('realtime', {})
            
            if not realtime_settings.get('enabled', False):
                print("Сбор реальных данных отключен в настройках")
                continue
            
            worker.submit(MT5Task(
                task_type="start_realtime",
                result_callback=lambda res: print("Сбор данных запущен"),
                error_callback=lambda e: print(f"Ошибка запуска: {e}")
            ))
        elif user_input == "exit":
            print("Выход из программы...")
            worker._stop_flag = True
            time.sleep(0.5)
            break
        elif not user_input:
            continue
        else:
            print(f"Неизвестная команда: '{user_input}'. Доступные команды:")
            print(f"  {shutdown_command} - завершение работы")
            print("  stats - статистика сбора данных")
            print("  realtime start - запустить сбор данных")
            print("  realtime stop - остановить сбор данных")
            print("  exit - выход из программы")


if __name__ == "__main__":
    main()