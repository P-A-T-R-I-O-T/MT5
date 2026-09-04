# main.py

import time
from src.utils.json_manager import JsonManager
from src.core.system_orchestrator import SystemOrchestrator


def main():
    """Точка входа в программу."""
    print("1 [main] Запуск программы...")
    
    print("2 [main] Создание SystemOrchestrator...")
    orchestrator = SystemOrchestrator()
    print("3 [main] SystemOrchestrator создан")

    print("4 [main] Запускаем SystemOrchestrator.start()")
    orchestrator.start()
    
    try:
        print("[main] Запуск оркестратора...")
        orchestrator.start()
        print("[main] Оркестратор запущен")
        
        # Ждем завершения работы
        print("[main] Ожидание завершения работы системы...")
        while orchestrator.system_running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[main] Прервано пользователем (KeyboardInterrupt).")
    except Exception as e:
        print(f"[main] Неожиданная ошибка: {e}")
    finally:
        print("[main] Завершение работы...")
        orchestrator.stop()
        print("[main] Система корректно завершена.")


if __name__ == "__main__":
    main()
    print("[main.__main__] Программа завершена")