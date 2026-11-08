# main.py

import time
from src.core.system_orchestrator import SystemOrchestrator
from config.settings import RUN
from src.ai.training_orchestrator import TrainingOrchestrator


def main():
    """Точка входа в программу."""
    print("1 [main] Запуск программы...")

    
    if RUN.mode == "live":
        print("2 [main] Создание SystemOrchestrator...")
        orchestrator = SystemOrchestrator()
        print("3 [main] SystemOrchestrator создан")
    elif RUN.mode == "train":
        print("2.1 [main] Создание TrainingOrchestrator...")
        orchestrator = TrainingOrchestrator()
        print("3.1 [main] Создание TrainingOrchestrator создан")
    else: raise ValueError("Не корректные настройки RUN")
    


    try:
        print("4 [main] Запускаем SystemOrchestrator.start()")
        orchestrator.start()  # Только один вызов!
        print("[main] Оркестратор запущен")
        
        # Ждем завершения работы
        while orchestrator.system_running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[main] Прервано пользователем.")
    except Exception as e:
        print(f"[main] Неожиданная ошибка: {e}")
    finally:
        print("[main] Завершение работы...")
        orchestrator.stop()
        print("[main] Система корректно завершена.")


if __name__ == "__main__":
    main()
    print("[main.__main__] Программа завершена")