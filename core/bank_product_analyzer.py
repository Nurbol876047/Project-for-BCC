#!/usr/bin/env python3
"""
Анализатор банковских продуктов для клиентов
Загружает данные из project444/, очищает их и определяет лучшие продукты
"""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import re
import random
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class BankProductAnalyzer:
    def __init__(self, data_path: str = "project444/", output_path: str = "output/"):
        self.data_path = data_path
        self.output_path = output_path
        self.transactions = None
        self.clients = None
        self.products = None
        
        # Правила для определения продуктов
        self.product_rules = {
            'travel_card': {
                'taxi': ['такси', 'taxi', 'uber', 'yandex'],
                'hotels': ['отель', 'hotel', 'гостиница', 'бронирование'],
                'travel': ['путешествие', 'travel', 'авиабилет', 'билет'],
                'currency': ['usd', 'eur', 'доллар', 'евро', 'валюта']
            },
            'premium_card': {
                'high_balance': 100000,  # руб
                'restaurants': ['ресторан', 'restaurant', 'кафе', 'cafe'],
                'cosmetics': ['косметика', 'cosmetics', 'парфюм', 'perfume'],
                'jewelry': ['ювелирные', 'jewelry', 'золото', 'gold', 'украшения']
            },
            'credit_card': {
                'categories': ['топ-категории', 'основные траты'],
                'online_services': ['онлайн', 'online', 'подписка', 'subscription']
            },
            'currency_exchange': {
                'fx_operations': ['fx_buy', 'fx_sell', 'обмен валют', 'currency exchange'],
                'usd_eur_spending': ['usd', 'eur', 'доллар', 'евро']
            },
            'deposits': {
                'free_funds': 50000,  # руб
                'stable_balance': True
            },
            'investments': {
                'free_money': 100000,  # руб
                'savings_interest': True
            },
            'cash_loan': {
                'regular_loans': True,
                'outflows_greater': True
            }
        }
        
        # TOV правила для пуш-уведомлений
        self.tov_rules = {
            'travel_card': "Карта для путешествий с кэшбэком до 5% на отели и такси",
            'premium_card': "Премиальная карта с эксклюзивными привилегиями",
            'credit_card': "Кредитная карта с льготным периодом до 55 дней",
            'currency_exchange': "Выгодный обмен валют без комиссии",
            'deposits': "Накопительный счет с доходностью до 8% годовых",
            'investments': "Инвестиционный портфель с доходностью до 12%",
            'cash_loan': "Кредит наличными под 15.9% годовых"
        }

    def load_data(self) -> bool:
        """Загружает все файлы из project444/"""
        try:
            # Поиск всех CSV файлов в папке project444
            csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
            
            if not csv_files:
                print(f"Файлы не найдены в {self.data_path}")
                return False
            
            dataframes = []
            for file in csv_files:
                try:
                    df = pd.read_csv(file)
                    df['source_file'] = os.path.basename(file)
                    dataframes.append(df)
                    print(f"Загружен файл: {file} ({len(df)} записей)")
                except Exception as e:
                    print(f"Ошибка загрузки файла {file}: {e}")
            
            if dataframes:
                self.transactions = pd.concat(dataframes, ignore_index=True)
                print(f"Всего загружено {len(self.transactions)} транзакций")
                return True
            else:
                print("Не удалось загрузить ни одного файла")
                return False
                
        except Exception as e:
            print(f"Ошибка при загрузке данных: {e}")
            return False

    def clean_data(self) -> None:
        """Очищает данные: корректные даты, суммы, категории"""
        if self.transactions is None:
            print("Данные не загружены")
            return
        
        print("Начинаем очистку данных...")
        
        # Очистка дат
        date_columns = ['date', 'transaction_date', 'created_at', 'timestamp']
        for col in date_columns:
            if col in self.transactions.columns:
                self.transactions[col] = pd.to_datetime(self.transactions[col], errors='coerce')
                print(f"Очищена колонка дат: {col}")
        
        # Очистка сумм
        amount_columns = ['amount', 'sum', 'value', 'price', 'balance']
        for col in amount_columns:
            if col in self.transactions.columns:
                # Удаляем валютные символы и приводим к числовому формату
                self.transactions[col] = self.transactions[col].astype(str).str.replace(r'[^\d.,-]', '', regex=True)
                self.transactions[col] = self.transactions[col].str.replace(',', '.')
                self.transactions[col] = pd.to_numeric(self.transactions[col], errors='coerce')
                print(f"Очищена колонка сумм: {col}")
        
        # Очистка категорий
        category_columns = ['category', 'type', 'description', 'merchant']
        for col in category_columns:
            if col in self.transactions.columns:
                self.transactions[col] = self.transactions[col].astype(str).str.strip().str.lower()
                print(f"Очищена колонка категорий: {col}")
        
        # Удаляем строки с пустыми критически важными полями
        critical_columns = ['client_id', 'user_id', 'customer_id']
        for col in critical_columns:
            if col in self.transactions.columns:
                self.transactions = self.transactions.dropna(subset=[col])
                print(f"Удалены записи с пустым {col}")
        
        print(f"Очистка завершена. Осталось {len(self.transactions)} записей")

    def analyze_client_behavior(self, client_id: str) -> Dict:
        """Анализирует поведение клиента для определения лучшего продукта"""
        client_data = self.transactions[
            self.transactions['client_id'].astype(str) == str(client_id)
        ].copy()
        
        if len(client_data) == 0:
            return {}
        
        analysis = {
            'client_id': client_id,
            'total_transactions': len(client_data),
            'avg_balance': 0,
            'total_spending': 0,
            'categories': {},
            'currency_operations': 0,
            'online_services': 0,
            'regular_loans': 0,
            'outflows_vs_inflows': 0
        }
        
        # Анализ баланса
        if 'balance' in client_data.columns:
            analysis['avg_balance'] = client_data['balance'].mean()
        
        # Анализ трат
        if 'amount' in client_data.columns:
            spending = client_data[client_data['amount'] < 0]['amount'].abs().sum()
            income = client_data[client_data['amount'] > 0]['amount'].sum()
            analysis['total_spending'] = spending
            analysis['outflows_vs_inflows'] = spending / max(income, 1)
        
        # Анализ категорий
        if 'category' in client_data.columns:
            category_counts = client_data['category'].value_counts()
            analysis['categories'] = category_counts.to_dict()
        
        # Анализ валютных операций
        if 'description' in client_data.columns:
            desc_text = ' '.join(client_data['description'].astype(str))
            analysis['currency_operations'] = sum(1 for word in ['usd', 'eur', 'доллар', 'евро', 'fx_buy', 'fx_sell'] 
                                                if word in desc_text.lower())
        
        # Анализ онлайн-сервисов
        if 'description' in client_data.columns:
            desc_text = ' '.join(client_data['description'].astype(str))
            analysis['online_services'] = sum(1 for word in ['онлайн', 'online', 'подписка', 'subscription'] 
                                            if word in desc_text.lower())
        
        return analysis

    def determine_best_product(self, analysis: Dict) -> str:
        """Определяет лучший банковский продукт для клиента"""
        if not analysis:
            return 'deposits'  # по умолчанию
        
        scores = {
            'travel_card': 0,
            'premium_card': 0,
            'credit_card': 0,
            'currency_exchange': 0,
            'deposits': 0,
            'investments': 0,
            'cash_loan': 0
        }
        
        categories = analysis.get('categories', {})
        avg_balance = analysis.get('avg_balance', 0)
        total_spending = analysis.get('total_spending', 0)
        currency_ops = analysis.get('currency_operations', 0)
        online_services = analysis.get('online_services', 0)
        outflows_ratio = analysis.get('outflows_vs_inflows', 0)
        total_transactions = analysis.get('total_transactions', 0)
        
        # Карта для путешествий (приоритет: валютные операции + траты на путешествия)
        travel_keywords = ['такси', 'taxi', 'отель', 'hotel', 'путешествие', 'travel', 'авиабилет', 'билет']
        travel_score = 0
        for category, count in categories.items():
            if any(word in category.lower() for word in travel_keywords):
                travel_score += count * 3
        
        if currency_ops > 0:
            travel_score += currency_ops * 4
        if currency_ops > 3:  # много валютных операций
            travel_score += 10
        
        scores['travel_card'] = travel_score
        
        # Премиальная карта (приоритет: высокий баланс + премиум траты)
        premium_keywords = ['ресторан', 'restaurant', 'кафе', 'cafe', 'косметика', 'cosmetics', 'парфюм', 'perfume', 'ювелирные', 'jewelry', 'золото', 'gold', 'украшения']
        premium_score = 0
        
        if avg_balance > 200000:  # очень высокий баланс
            premium_score += 15
        elif avg_balance > 100000:
            premium_score += 8
        
        for category, count in categories.items():
            if any(word in category.lower() for word in premium_keywords):
                premium_score += count * 4
        
        scores['premium_card'] = premium_score
        
        # Кредитная карта (приоритет: онлайн-сервисы + разнообразие трат)
        credit_score = 0
        if online_services > 0:
            credit_score += online_services * 3
        if online_services > 5:  # много онлайн-сервисов
            credit_score += 8
        
        if len(categories) > 5:  # много разных категорий
            credit_score += 6
        elif len(categories) > 3:
            credit_score += 3
        
        if total_transactions > 100:  # активный пользователь
            credit_score += 5
        
        scores['credit_card'] = credit_score
        
        # Обмен валют (приоритет: частые валютные операции)
        currency_score = 0
        if currency_ops > 5:  # очень много валютных операций
            currency_score += 15
        elif currency_ops > 2:
            currency_score += currency_ops * 3
        
        # Дополнительные баллы за траты в валюте
        currency_categories = ['usd', 'eur', 'доллар', 'евро', 'валюта']
        for category, count in categories.items():
            if any(word in category.lower() for word in currency_categories):
                currency_score += count * 2
        
        scores['currency_exchange'] = currency_score
        
        # Депозиты (приоритет: стабильный баланс + свободные средства)
        deposits_score = 0
        if avg_balance > 100000 and outflows_ratio < 0.8:  # высокий баланс + низкие траты
            deposits_score += 12
        elif avg_balance > 50000 and outflows_ratio < 1.0:
            deposits_score += 8
        elif avg_balance > 30000:
            deposits_score += 4
        
        # Бонус за стабильность (много транзакций, но не большие траты)
        if total_transactions > 80 and outflows_ratio < 1.2:
            deposits_score += 5
        
        scores['deposits'] = deposits_score
        
        # Инвестиции (приоритет: очень высокий баланс + интерес к сбережениям)
        investments_score = 0
        if avg_balance > 300000:  # очень высокий баланс
            investments_score += 15
        elif avg_balance > 150000:
            investments_score += 8
        
        # Бонус за низкие траты относительно баланса
        if avg_balance > 100000 and outflows_ratio < 0.6:
            investments_score += 10
        
        # Бонус за активность в сбережениях
        if total_transactions > 100 and outflows_ratio < 1.0:
            investments_score += 5
        
        scores['investments'] = investments_score
        
        # Кредит наличными (приоритет: высокие траты относительно доходов)
        loan_score = 0
        if outflows_ratio > 2.0:  # траты в 2+ раза больше доходов
            loan_score += 15
        elif outflows_ratio > 1.5:
            loan_score += 10
        elif outflows_ratio > 1.2:
            loan_score += 5
        
        # Дополнительные баллы за регулярные займы
        if total_transactions > 100 and outflows_ratio > 1.3:
            loan_score += 8
        
        # Бонус за низкий баланс при высоких тратах
        if avg_balance < 50000 and outflows_ratio > 1.5:
            loan_score += 6
        
        scores['cash_loan'] = loan_score
        
        # Возвращаем продукт с максимальным счетом
        best_product = max(scores, key=scores.get)
        max_score = scores[best_product]
        
        # Если максимальный счет слишком низкий, выбираем депозиты по умолчанию
        if max_score < 2:
            return 'deposits'
        
        # Добавляем случайность для разнообразия, если счеты близки
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_scores) > 1 and sorted_scores[0][1] - sorted_scores[1][1] < 3:
            # Если разница между топ-2 продуктами небольшая, иногда выбираем второй
            if random.random() < 0.3:  # 30% шанс выбрать второй продукт
                return sorted_scores[1][0]
        
        return best_product

    def generate_push_notification(self, client_id: str, product: str) -> str:
        """Генерирует персонализированное пуш-уведомление"""
        base_message = self.tov_rules.get(product, "Специальное предложение для вас")
        
        # Персонализация на основе анализа
        analysis = self.analyze_client_behavior(client_id)
        
        if product == 'travel_card' and analysis.get('currency_operations', 0) > 0:
            return f"🌍 {base_message}. Кэшбэк 5% на покупки в валюте!"
        elif product == 'premium_card' and analysis.get('avg_balance', 0) > 100000:
            return f"💎 {base_message}. Доступ к VIP-залам и персональному менеджеру!"
        elif product == 'credit_card':
            return f"💳 {base_message}. Льготный период 55 дней без процентов!"
        elif product == 'currency_exchange':
            return f"💱 {base_message}. Курс выгоднее на 0.5%!"
        elif product == 'deposits':
            return f"💰 {base_message}. Гарантированная доходность!"
        elif product == 'investments':
            return f"📈 {base_message}. Портфель под ваши цели!"
        elif product == 'cash_loan':
            return f"💵 {base_message}. Одобрение за 5 минут!"
        else:
            return f"🎯 {base_message}"

    def process_all_clients(self) -> pd.DataFrame:
        """Обрабатывает всех клиентов и создает итоговый DataFrame"""
        if self.transactions is None:
            print("Данные не загружены")
            return pd.DataFrame()
        
        # Получаем уникальных клиентов
        client_columns = ['client_id', 'user_id', 'customer_id']
        client_col = None
        for col in client_columns:
            if col in self.transactions.columns:
                client_col = col
                break
        
        if client_col is None:
            print("Не найдена колонка с ID клиентов")
            return pd.DataFrame()
        
        unique_clients = self.transactions[client_col].unique()
        print(f"Найдено {len(unique_clients)} уникальных клиентов")
        
        results = []
        
        for client_id in unique_clients:
            print(f"Обрабатываем клиента: {client_id}")
            
            # Анализируем поведение клиента
            analysis = self.analyze_client_behavior(client_id)
            
            # Определяем лучший продукт
            best_product = self.determine_best_product(analysis)
            
            # Генерируем пуш-уведомление
            push_notification = self.generate_push_notification(client_id, best_product)
            
            # Создаем запись результата
            result = {
                'client_id': client_id,
                'best_product': best_product,
                'push_notification': push_notification,
                'total_transactions': analysis.get('total_transactions', 0),
                'avg_balance': analysis.get('avg_balance', 0),
                'total_spending': analysis.get('total_spending', 0),
                'currency_operations': analysis.get('currency_operations', 0),
                'online_services': analysis.get('online_services', 0),
                'outflows_vs_inflows': analysis.get('outflows_vs_inflows', 0)
            }
            
            results.append(result)
        
        return pd.DataFrame(results)

    def save_results(self, results_df: pd.DataFrame) -> None:
        """Сохраняет результаты в CSV файл"""
        os.makedirs(self.output_path, exist_ok=True)
        output_file = os.path.join(self.output_path, "result.csv")
        
        results_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Результаты сохранены в {output_file}")
        print(f"Обработано {len(results_df)} клиентов")

    def run_analysis(self) -> None:
        """Запускает полный анализ"""
        print("=== АНАЛИЗ БАНКОВСКИХ ПРОДУКТОВ ===")
        
        # Загружаем данные
        if not self.load_data():
            print("Не удалось загрузить данные")
            return
        
        # Очищаем данные
        self.clean_data()
        
        # Обрабатываем всех клиентов
        results = self.process_all_clients()
        
        if not results.empty:
            # Сохраняем результаты
            self.save_results(results)
            
            # Выводим статистику
            print("\n=== СТАТИСТИКА ===")
            print("Распределение продуктов:")
            print(results['best_product'].value_counts())
            
            print(f"\nВсего обработано клиентов: {len(results)}")
        else:
            print("Не удалось обработать клиентов")

def main():
    """Основная функция"""
    analyzer = BankProductAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
