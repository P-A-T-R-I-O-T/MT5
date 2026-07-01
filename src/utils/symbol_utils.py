# symbol_utils.py


def group_and_display_symbols(symbols): # Группирует и отображает символы по категориям в столбцах
    """
    Группирует и отображает символы по категориям в столбцах
    :param symbols: список объектов SymbolInfo
    """
    if not symbols:
        print("Нет данных для отображения символов")
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

    for s in symbols:
        name = s.name

        if name.startswith('X') and name.endswith('USD'):
            groups['Металлы'].append(name)
        elif name in ['BTCUSD', 'ETHUSD', 'LTCUSD', 'XRPUSD'] or 'BTC' in name or 'ETH' in name:
            groups['Криптовалюты'].append(name)
        elif name in ['ES', 'NQ', 'TA35', 'VIX', 'FDAX', 'FESX']:
            groups['Индексы'].append(name)
        elif name in ['CL', 'HO', 'NG', 'BRN', 'COCOA', 'COFFEE', 'CORN',
                     'SOYBEAN', 'SUGAR', 'WHEAT', 'COTTON']:
            groups['Сырьё/Товары'].append(name)
        elif any(company in name for company in ['Tesla', 'Apple', 'Amazon',
                                                'Microsoft', 'Google', 'Netflix']):
            groups['Акции'].append(name)
        elif name in ['SPY', 'IJH', 'FXI', 'EWG', 'VGK', 'ILF', 'EWW',
                     'EWZ', 'AGG', 'EWU', 'USDX']:
            groups['ETF/Фонды'].append(name)
        elif 'Vol_' in name or name in ['Boom', 'Crash']:
            groups['Волатильность'].append(name)
        else:
            groups['Валютные пары'].append(name)

    def print_group(group_name, symbols_list):
        if not symbols_list:
            return
        print(f'\n{"="*60}')
        print(f'{group_name} ({len(symbols_list)}):')
        print('='*60)

        columns = 3
        rows = (len(symbols_list) + columns - 1) // columns

        for row in range(rows):
            line = []
            for col in range(columns):
                idx = row + col * rows
                if idx < len(symbols_list):
                    line.append(f'{idx + 1:3}. {symbols_list[idx]:<12}')
            print('  '.join(line))

    for group_name, symbols_list in groups.items():
        print_group(group_name, symbols_list)