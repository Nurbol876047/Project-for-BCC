#!/usr/bin/env python3
"""
Генератор тестовых данных для анализа банковских продуктов
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

def generate_sample_data():
    """Генерирует тестовые данные транзакций"""
    
    # Создаем папку project444 если её нет
    os.makedirs("project444", exist_ok=True)
    
    # Параметры генерации
    num_clients = 70  # Увеличиваем количество клиентов
    transactions_per_client = np.random.poisson(100, num_clients)  # 50-150 транзакций на клиента
    
    # Категории транзакций с большим разнообразием
    categories = [
        # Обычные траты
        'продукты', 'супермаркет', 'магазин', 'транспорт', 'бензин', 'автобус', 'метро',
        
        # Путешествия
        'ресторан', 'кафе', 'такси', 'uber', 'yandex', 'отель', 'hotel', 
        'путешествие', 'travel', 'авиабилет', 'билет', 'туризм', 'отдых',
        
        # Премиум траты
        'косметика', 'cosmetics', 'парфюм', 'perfume', 'ювелирные', 'jewelry',
        'золото', 'gold', 'украшения', 'люкс', 'luxury', 'премиум', 'premium',
        
        # Онлайн-сервисы
        'онлайн', 'online', 'подписка', 'subscription', 'стриминг', 'streaming',
        'игры', 'games', 'приложения', 'apps', 'софт', 'software',
        
        # Валютные операции
        'usd', 'eur', 'доллар', 'евро', 'валюта', 'fx_buy', 'fx_sell',
        'обмен валют', 'currency exchange', 'конвертация', 'conversion',
        
        # Разные категории для кредитных карт
        'топ-категории', 'основные траты', 'развлечения', 'entertainment',
        'спорт', 'sport', 'фитнес', 'fitness', 'здоровье', 'health',
        'образование', 'education', 'книги', 'books', 'музыка', 'music'
    ]
    
    all_transactions = []
    
    # Создаем разные типы клиентов для разнообразия продуктов
    client_types = ['travel', 'premium', 'credit', 'currency', 'deposits', 'investments', 'loan']
    
    for client_id in range(1, num_clients + 1):
        num_transactions = transactions_per_client[client_id - 1]
        
        # Определяем тип клиента (циклически)
        client_type = client_types[(client_id - 1) % len(client_types)]
        
        # Генерируем баланс в зависимости от типа клиента
        if client_type == 'premium':
            base_balance = np.random.uniform(200000, 800000)  # Высокий баланс
        elif client_type == 'investments':
            base_balance = np.random.uniform(300000, 1000000)  # Очень высокий баланс
        elif client_type == 'loan':
            base_balance = np.random.uniform(5000, 50000)  # Низкий баланс
        elif client_type == 'deposits':
            base_balance = np.random.uniform(80000, 200000)  # Средний-высокий баланс
        else:
            base_balance = np.random.uniform(20000, 300000)  # Обычный баланс
        
        for i in range(num_transactions):
            # Дата транзакции (последние 6 месяцев)
            days_ago = np.random.randint(0, 180)
            transaction_date = datetime.now() - timedelta(days=days_ago)
            
            # Сумма транзакции в зависимости от типа клиента
            if client_type == 'loan':
                # Клиенты с кредитами - больше трат чем доходов
                if random.random() < 0.7:  # 70% трат
                    amount = -np.random.uniform(5000, 30000)
                else:  # 30% доходов
                    amount = np.random.uniform(10000, 50000)
            elif client_type == 'investments':
                # Инвесторы - больше доходов, меньше трат
                if random.random() < 0.3:  # 30% трат
                    amount = -np.random.uniform(1000, 15000)
                else:  # 70% доходов
                    amount = np.random.uniform(20000, 100000)
            elif client_type == 'premium':
                # Премиум клиенты - большие траты и доходы
                if random.random() < 0.6:  # 60% трат
                    amount = -np.random.uniform(5000, 50000)
                else:  # 40% доходов
                    amount = np.random.uniform(30000, 150000)
            else:
                # Обычные клиенты
                amount = np.random.normal(0, 10000)
                if amount > 0:
                    amount = min(amount, 100000)
                else:
                    amount = max(amount, -50000)
            
            # Категория в зависимости от типа клиента
            if client_type == 'travel':
                travel_cats = ['такси', 'uber', 'yandex', 'отель', 'hotel', 'путешествие', 'travel', 'авиабилет', 'билет', 'туризм', 'отдых']
                if random.random() < 0.4:  # 40% трат на путешествия
                    category = random.choice(travel_cats)
                else:
                    category = random.choice(categories)
            elif client_type == 'premium':
                premium_cats = ['ресторан', 'кафе', 'косметика', 'cosmetics', 'парфюм', 'perfume', 'ювелирные', 'jewelry', 'золото', 'gold', 'украшения', 'люкс', 'luxury', 'премиум', 'premium']
                if random.random() < 0.3:  # 30% премиум трат
                    category = random.choice(premium_cats)
                else:
                    category = random.choice(categories)
            elif client_type == 'credit':
                online_cats = ['онлайн', 'online', 'подписка', 'subscription', 'стриминг', 'streaming', 'игры', 'games', 'приложения', 'apps', 'софт', 'software']
                if random.random() < 0.5:  # 50% онлайн трат
                    category = random.choice(online_cats)
                else:
                    category = random.choice(categories)
            elif client_type == 'currency':
                currency_cats = ['usd', 'eur', 'доллар', 'евро', 'валюта', 'fx_buy', 'fx_sell', 'обмен валют', 'currency exchange', 'конвертация', 'conversion']
                if random.random() < 0.3:  # 30% валютных операций
                    category = random.choice(currency_cats)
                else:
                    category = random.choice(categories)
            else:
                category = random.choice(categories)
            
            # Описание
            description = f"Транзакция {category} на сумму {abs(amount):.2f}"
            
            # Баланс после транзакции
            base_balance += amount
            balance = max(base_balance, 0)  # баланс не может быть отрицательным
            
            transaction = {
                'client_id': client_id,
                'transaction_date': transaction_date,
                'amount': amount,
                'category': category,
                'description': description,
                'balance': balance,
                'merchant': f"Merchant_{random.randint(1, 100)}",
                'type': 'debit' if amount < 0 else 'credit'
            }
            
            all_transactions.append(transaction)
    
    # Создаем DataFrame
    df = pd.DataFrame(all_transactions)
    
    # Сохраняем в CSV
    output_file = "project444/transactions.csv"
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Сгенерировано {len(df)} транзакций для {num_clients} клиентов")
    print(f"Данные сохранены в {output_file}")
    
    # Создаем дополнительный файл с информацией о клиентах
    clients_data = []
    for client_id in range(1, num_clients + 1):
        client_transactions = df[df['client_id'] == client_id]
        avg_balance = client_transactions['balance'].mean()
        total_spending = client_transactions[client_transactions['amount'] < 0]['amount'].abs().sum()
        
        client_info = {
            'client_id': client_id,
            'avg_balance': avg_balance,
            'total_spending': total_spending,
            'total_transactions': len(client_transactions),
            'registration_date': datetime.now() - timedelta(days=np.random.randint(30, 1000))
        }
        clients_data.append(client_info)
    
    clients_df = pd.DataFrame(clients_data)
    clients_file = "project444/clients.csv"
    clients_df.to_csv(clients_file, index=False, encoding='utf-8')
    
    print(f"Информация о клиентах сохранена в {clients_file}")
    
    return df, clients_df

if __name__ == "__main__":
    generate_sample_data()
