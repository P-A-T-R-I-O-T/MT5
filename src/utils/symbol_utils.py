# symbol_utils.py


def group_and_display_symbols(symbols): # Группирует и отображает символы по категориям в столбцах
    """
    Группирует и отображает символы по категориям в столбцах
    :param symbols: список объектов SymbolInfo
    """
    print("[symbol_utils.group_and_display_symbols] Начало группировки и отображения символов...")
    print(f"[symbol_utils.group_and_display_symbols] Количество символов: {len(symbols)}")
    
    if not symbols:
        print("[symbol_utils.group_and_display_symbols] Нет данных для отображения символов")
        return

    groups = {
        'Валютные пары': [],
        'Металлы': [],
        'Криптовалюты': [],
        'Индексы': [],
        'Сырьё/Товары': [],
        'Акции': [],
        'ETF/Фонды': [],
        'Волатильность': []
    }
    print("[symbol_utils.group_and_display_symbols] Созданы группы для категорий")

    print("[symbol_utils.group_and_display_symbols] Начало классификации символов...")
    for s in symbols:
        name = s.name
        print(f"[symbol_utils.group_and_display_symbols] Обработка символа: {name}")

        if name.startswith('X') and name.endswith('USD'):
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Металлы")
            groups['Металлы'].append(name)
        elif name in ['BTCUSD', 'ETHUSD', 'LTCUSD', 'XRPUSD'] or 'BTC' in name or 'ETH' in name:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Криптовалюты")
            groups['Криптовалюты'].append(name)
        elif name in ['ES', 'NQ', 'TA35', 'VIX', 'FDAX', 'FESX']:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Индексы")
            groups['Индексы'].append(name)
        elif name in ['CL', 'HO', 'NG', 'BRN', 'COCOA', 'COFFEE', 'CORN',
                     'SOYBEAN', 'SUGAR', 'WHEAT', 'COTTON']:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Сырьё/Товары")
            groups['Сырьё/Товары'].append(name)
        elif any(company in name for company in ['Tesla', 'Apple', 'Amazon',
                                                'Microsoft', 'Google', 'Netflix']):
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Акции")
            groups['Акции'].append(name)
        elif name in ['SPY', 'IJH', 'FXI', 'EWG', 'VGK', 'ILF', 'EWW',
                     'EWZ', 'AGG', 'EWU', 'USDX']:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> ETF/Фонды")
            groups['ETF/Фонды'].append(name)
        elif 'Vol_' in name or name in ['Boom', 'Crash']:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Волатильность")
            groups['Волатильность'].append(name)
        else:
            print(f"[symbol_utils.group_and_display_symbols] {name} -> Валютные пары")
            groups['Валютные пары'].append(name)
    
    print("[symbol_utils.group_and_display_symbols] Классификация завершена")
    print(f"[symbol_utils.group_and_display_symbols] Итоги группировки:")
    for group_name, symbols_list in groups.items():
        print(f"  {group_name}: {len(symbols_list)} символов")

    def print_group(group_name, symbols_list):
        print(f"[symbol_utils.print_group] Отображение группы: {group_name}")
        print(f"[symbol_utils.print_group] Количество символов: {len(symbols_list)}")
        
        if not symbols_list:
            print(f"[symbol_utils.print_group] Группа {group_name} пуста, пропускаем")
            return
            
        print(f'\n{"="*60}')
        print(f'{group_name} ({len(symbols_list)}):')
        print('='*60)

        columns = 3
        rows = (len(symbols_list) + columns - 1) // columns
        print(f"[symbol_utils.print_group] Отображение в {columns} колонках, {rows} строк")

        for row in range(rows):
            line = []
            for col in range(columns):
                idx = row + col * rows
                if idx < len(symbols_list):
                    line.append(f'{idx + 1:3}. {symbols_list[idx]:<12}')
            print('  '.join(line))

    print("[symbol_utils.group_and_display_symbols] Отображение групп...")
    for group_name, symbols_list in groups.items():
        print_group(group_name, symbols_list)
    
    print("[symbol_utils.group_and_display_symbols] Отображение завершено")