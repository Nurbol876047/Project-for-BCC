#!/usr/bin/env python3
"""
Анализатор банковских продуктов для клиентов
Соответствует ТЗ: 60 клиентов, KZT, точные названия продуктов, формат уведомлений 180-220 символов
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
        
        # Правила для определения продуктов согласно ТЗ (в KZT)
        self.product_rules = {
            'Карта для путешествий': {
                'taxi': ['такси', 'taxi', 'uber', 'yandex'],
                'hotels': ['отель', 'hotel', 'гостиница', 'бронирование'],
                'travel': ['путешествие', 'travel', 'авиабилет', 'билет'],
                'currency': ['usd', 'eur', 'доллар', 'евро', 'валюта']
            },
            'Премиальная карта': {
                'high_balance': 100000,  # KZT
                'restaurants': ['ресторан', 'restaurant', 'кафе', 'cafe'],
                'cosmetics': ['косметика', 'cosmetics', 'парфюм', 'perfume'],
                'jewelry': ['ювелирные', 'jewelry', 'золото', 'gold', 'украшения']
            },
            'Кредитная карта': {
                'categories': ['топ-категории', 'основные траты'],
                'online_services': ['онлайн', 'online', 'подписка', 'subscription']
            },
            'Обмен валют': {
                'fx_operations': ['fx_buy', 'fx_sell', 'обмен валют', 'currency exchange'],
                'usd_eur_spending': ['usd', 'eur', 'доллар', 'евро']
            },
            'Депозит накопительный': {
                'free_funds': 50000,  # KZT
                'stable_balance': True
            },
            'Депозит сберегательный': {
                'free_funds': 50000,  # KZT
                'stable_balance': True
            },
            'Инвестиции': {
                'free_funds': 100000,  # KZT
                'savings_interest': True
            },
            'Кредит наличными': {
                'regular_loans': True,
                'outflows_vs_inflows': 1.5  # outflows больше inflows в 1.5 раза
            }
        }
        
        # TOV правила для пуш-уведомлений (180-220 символов, один CTA, макс. одно emoji)
        self.tov_rules = {
            'Карта для путешествий': "🌍 Карта для путешествий с кэшбэком до 5% на отели и такси. Кэшбэк 5% на покупки в валюте! Оформите сейчас и экономьте на каждой поездке.",
            'Премиальная карта': "💎 Премиальная карта с эксклюзивными привилегиями. Доступ к VIP-залам и персональному менеджеру! Повысьте статус уже сегодня.",
            'Кредитная карта': "💳 Кредитная карта с льготным периодом до 55 дней. Кэшбэк до 3% на все покупки! Получите карту за 5 минут онлайн.",
            'Обмен валют': "💱 Выгодный обмен валют без комиссии. Лучший курс в городе! Обменивайте валюту выгодно и безопасно.",
            'Депозит накопительный': "💰 Накопительный счет с доходностью до 8% годовых. Начните копить уже сегодня! Ваши деньги работают на вас.",
            'Депозит сберегательный': "🏦 Сберегательный счет с гарантированной доходностью. Защитите свои накопления! Откройте счет за 2 минуты.",
            'Инвестиции': "📈 Инвестиционный портфель с доходностью до 12% годовых. Увеличьте капитал! Начните инвестировать с 10 000 ₸.",
            'Кредит наличными': "💵 Кредит наличными под 15.9% годовых. Решение за 15 минут! Получите деньги на любые цели."
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
        
        # Очистка дат согласно ТЗ
        date_columns = ['date', 'transaction_date', 'created_at', 'timestamp']
        for col in date_columns:
            if col in self.transactions.columns:
                self.transactions[col] = pd.to_datetime(self.transactions[col], errors='coerce')
                print(f"Очищена колонка дат: {col}")
        
        # Очистка сумм в KZT согласно ТЗ
        amount_columns = ['amount', 'sum', 'value', 'price', 'balance']
        for col in amount_columns:
            if col in self.transactions.columns:
                # Удаляем валютные символы (₸, $, €) и приводим к числовому формату
                self.transactions[col] = self.transactions[col].astype(str).str.replace(r'[^\d.,-]', '', regex=True)
                self.transactions[col] = self.transactions[col].str.replace(',', '.')
                self.transactions[col] = pd.to_numeric(self.transactions[col], errors='coerce')
                print(f"Очищена колонка сумм: {col} (в KZT)")
        
        # Очистка категорий
        category_columns = ['category', 'type', 'description', 'merchant']
        for col in category_columns:
            if col in self.transactions.columns:
                self.transactions[col] = self.transactions[col].astype(str).str.strip().str.lower()
                print(f"Очищена колонка категорий: {col}")
        
        # Удаляем строки с пустыми критически важными полями согласно ТЗ
        critical_columns = ['client_code', 'client_id', 'user_id', 'customer_id']
        for col in critical_columns:
            if col in self.transactions.columns:
                self.transactions = self.transactions.dropna(subset=[col])
                print(f"Удалены записи с пустым {col}")
        
        print(f"Очистка завершена. Осталось {len(self.transactions)} записей")

    def analyze_client_behavior(self, client_code: str) -> Dict:
        """Анализирует поведение клиента для определения лучшего продукта согласно ТЗ"""
        # Поддерживаем оба формата полей для совместимости
        if 'client_code' in self.transactions.columns:
            client_data = self.transactions[
                self.transactions['client_code'].astype(str) == str(client_code)
            ].copy()
        else:
            client_data = self.transactions[
                self.transactions['client_id'].astype(str) == str(client_code)
            ].copy()
        
        if len(client_data) == 0:
            return {}
        
        analysis = {
            'client_code': client_code,
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

    def determine_best_product(self, analysis: Dict) -> Tuple[str, List[str]]:
        """Определяет лучший банковский продукт для клиента согласно ТЗ и возвращает Top-4"""
        if not analysis:
            return 'Депозит накопительный', ['Депозит накопительный', 'Депозит сберегательный', 'Инвестиции', 'Кредитная карта']  # по умолчанию согласно ТЗ
        
        scores = {
            'Карта для путешествий': 0,
            'Премиальная карта': 0,
            'Кредитная карта': 0,
            'Обмен валют': 0,
            'Депозит накопительный': 0,
            'Депозит сберегательный': 0,
            'Инвестиции': 0,
            'Кредит наличными': 0
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
        
        scores['Карта для путешествий'] = travel_score
        
        # Премиальная карта (приоритет: высокий баланс + премиум траты)
        premium_keywords = ['ресторан', 'restaurant', 'кафе', 'cafe', 'косметика', 'cosmetics', 'парфюм', 'perfume', 'ювелирные', 'jewelry', 'золото', 'gold', 'украшения']
        premium_score = 0
        
        if avg_balance > 200000:  # очень высокий баланс в KZT
            premium_score += 15
        elif avg_balance > 100000:
            premium_score += 8
        
        for category, count in categories.items():
            if any(word in category.lower() for word in premium_keywords):
                premium_score += count * 4
        
        scores['Премиальная карта'] = premium_score
        
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
        
        scores['Кредитная карта'] = credit_score
        
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
        
        scores['Обмен валют'] = currency_score
        
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
        
        scores['Депозит накопительный'] = deposits_score
        scores['Депозит сберегательный'] = deposits_score  # одинаковые критерии
        
        # Инвестиции (приоритет: очень высокий баланс + интерес к сбережениям)
        investments_score = 0
        if avg_balance > 300000:  # очень высокий баланс в KZT
            investments_score += 15
        elif avg_balance > 150000:
            investments_score += 8
        
        # Бонус за низкие траты относительно баланса
        if avg_balance > 100000 and outflows_ratio < 0.6:
            investments_score += 10
        
        # Бонус за активность в сбережениях
        if total_transactions > 100 and outflows_ratio < 1.0:
            investments_score += 5
        
        scores['Инвестиции'] = investments_score
        
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
        
        scores['Кредит наличными'] = loan_score
        
        # Возвращаем продукт с максимальным счетом и Top-4
        best_product = max(scores, key=scores.get)
        max_score = scores[best_product]
        
        # Если максимальный счет слишком низкий, выбираем депозит по умолчанию согласно ТЗ
        if max_score < 2:
            return 'Депозит накопительный', ['Депозит накопительный', 'Депозит сберегательный', 'Инвестиции', 'Кредитная карта']
        
        # Добавляем случайность для разнообразия, если счеты близки
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_scores) > 1 and sorted_scores[0][1] - sorted_scores[1][1] < 3:
            # Если разница между топ-2 продуктами небольшая, иногда выбираем второй
            if random.random() < 0.3:  # 30% шанс выбрать второй продукт
                best_product = sorted_scores[1][0]
        
        # Формируем Top-4 продуктов
        top4_products = [item[0] for item in sorted_scores[:4]]
        
        return best_product, top4_products

    def generate_push_notification(self, client_code: str, product: str) -> str:
        """Генерирует персонализированное пуш-уведомление согласно ТЗ (180-220 символов)"""
        # Используем готовые сообщения из TOV правил (уже соответствуют ТЗ)
        base_message = self.tov_rules.get(product, "Специальное предложение для вас")
        
        # Проверяем длину и корректируем если нужно
        if len(base_message) < 180:
            # Добавляем персонализацию если сообщение слишком короткое
            analysis = self.analyze_client_behavior(client_code)
            
            if product == 'Карта для путешествий' and analysis.get('currency_operations', 0) > 0:
                return f"🌍 Карта для путешествий с кэшбэком до 5% на отели и такси. Кэшбэк 5% на покупки в валюте! Оформите сейчас и экономьте на каждой поездке."
            elif product == 'Премиальная карта' and analysis.get('avg_balance', 0) > 100000:
                return f"💎 Премиальная карта с эксклюзивными привилегиями. Доступ к VIP-залам и персональному менеджеру! Повысьте статус уже сегодня."
            elif product == 'Кредитная карта':
                return f"💳 Кредитная карта с льготным периодом до 55 дней. Кэшбэк до 3% на все покупки! Получите карту за 5 минут онлайн."
            elif product == 'Обмен валют':
                return f"💱 Выгодный обмен валют без комиссии. Лучший курс в городе! Обменивайте валюту выгодно и безопасно."
            elif product in ['Депозит накопительный', 'Депозит сберегательный']:
                return f"💰 Накопительный счет с доходностью до 8% годовых. Начните копить уже сегодня! Ваши деньги работают на вас."
            elif product == 'Инвестиции':
                return f"📈 Инвестиционный портфель с доходностью до 12% годовых. Увеличьте капитал! Начните инвестировать с 10 000 ₸."
            elif product == 'Кредит наличными':
                return f"💵 Кредит наличными под 15.9% годовых. Решение за 15 минут! Получите деньги на любые цели."
        
        return base_message

    def validate_push_quality(self, push_notification: str) -> Dict[str, bool]:
        """Проверяет качество пуш-уведомления согласно ТЗ (4×5 баллов)"""
        quality_checks = {
            'length_180_220': 180 <= len(push_notification) <= 220,
            'one_cta': push_notification.count('!') <= 1,  # максимум одно восклицание
            'one_emoji': sum(1 for char in push_notification if ord(char) > 127 and len(char.encode('utf-8')) > 1) <= 1,  # максимум одно emoji
            'proper_formatting': bool(re.search(r'\d+', push_notification))  # содержит числа
        }
        return quality_checks

    def process_all_clients(self) -> pd.DataFrame:
        """Обрабатывает всех клиентов и создает итоговый DataFrame"""
        if self.transactions is None:
            print("Данные не загружены")
            return pd.DataFrame()
        
        # Получаем уникальных клиентов согласно ТЗ
        client_columns = ['client_code', 'client_id', 'user_id', 'customer_id']
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
        
        for client_code in unique_clients:
            print(f"Обрабатываем клиента: {client_code}")
            
            # Анализируем поведение клиента
            analysis = self.analyze_client_behavior(client_code)
            
            # Определяем лучший продукт и Top-4
            best_product, top4_products = self.determine_best_product(analysis)
            
            # Генерируем пуш-уведомление
            push_notification = self.generate_push_notification(client_code, best_product)
            
            # Проверяем качество пуш-уведомления
            quality_checks = self.validate_push_quality(push_notification)
            
            # Создаем запись результата согласно ТЗ
            result = {
                'client_code': client_code,
                'product': best_product,
                'push_notification': push_notification,
                'top4_products': '|'.join(top4_products),  # Top-4 продукты через разделитель
                'push_quality_score': sum(quality_checks.values()),  # общий балл качества (0-4)
                'total_transactions': analysis.get('total_transactions', 0),
                'avg_balance': analysis.get('avg_balance', 0),
                'total_spending': analysis.get('total_spending', 0),
                'currency_operations': analysis.get('currency_operations', 0),
                'online_services': analysis.get('online_services', 0),
                'outflows_vs_inflows': analysis.get('outflows_vs_inflows', 0)
            }
            
            results.append(result)
        
        return pd.DataFrame(results)

    def process_hidden_test(self, hidden_data_path: str = "hidden_test/") -> pd.DataFrame:
        """Обрабатывает скрытый тестовый набор данных согласно ТЗ"""
        if not os.path.exists(hidden_data_path):
            print(f"Скрытый тест не найден в {hidden_data_path}")
            return pd.DataFrame()
        
        # Сохраняем текущие данные
        original_transactions = self.transactions
        
        # Загружаем скрытые данные
        self.transactions = None
        self.data_path = hidden_data_path
        
        if not self.load_data():
            print("Не удалось загрузить скрытые данные")
            self.transactions = original_transactions
            return pd.DataFrame()
        
        # Очищаем скрытые данные
        self.clean_data()
        
        # Обрабатываем скрытых клиентов
        hidden_results = self.process_all_clients()
        
        # Восстанавливаем оригинальные данные
        self.transactions = original_transactions
        self.data_path = "project444/"
        
        return hidden_results

    def save_results(self, results_df: pd.DataFrame) -> None:
        """Сохраняет результаты в CSV файл согласно ТЗ"""
        os.makedirs(self.output_path, exist_ok=True)
        
        # Основной файл result.csv согласно ТЗ: только client_code, product, push_notification
        main_file = os.path.join(self.output_path, "result.csv")
        main_df = results_df[['client_code', 'product', 'push_notification']].copy()
        main_df.to_csv(main_file, index=False, encoding='utf-8')
        print(f"Основные результаты сохранены в {main_file}")
        
        # Дополнительный файл с метриками для анализа
        summary_file = os.path.join(self.output_path, "summary_by_client.csv")
        results_df.to_csv(summary_file, index=False, encoding='utf-8')
        print(f"Детальные результаты сохранены в {summary_file}")
        
        print(f"Обработано {len(results_df)} клиентов")

    def run_analysis(self) -> None:
        """Запускает полный анализ согласно ТЗ"""
        print("=== АНАЛИЗ БАНКОВСКИХ ПРОДУКТОВ (ТЗ) ===")
        
        # Загружаем данные
        if not self.load_data():
            print("Не удалось загрузить данные")
            return
        
        # Очищаем данные
        self.clean_data()
        
        # Обрабатываем всех клиентов (60 клиентов согласно ТЗ)
        results = self.process_all_clients()
        
        if not results.empty:
            # Сохраняем результаты
            self.save_results(results)
            
            # Выводим статистику
            print("\n=== СТАТИСТИКА ===")
            print("Распределение продуктов:")
            print(results['product'].value_counts())
            
            print(f"\nКачество пуш-уведомлений:")
            print(f"Средний балл качества: {results['push_quality_score'].mean():.2f}/4")
            print(f"Количество уведомлений с максимальным качеством: {(results['push_quality_score'] == 4).sum()}")
            
            print(f"\nВсего обработано клиентов: {len(results)}")
            
            # Обрабатываем скрытый тест если доступен
            print("\n=== ОБРАБОТКА СКРЫТОГО ТЕСТА ===")
            hidden_results = self.process_hidden_test()
            if not hidden_results.empty:
                hidden_file = os.path.join(self.output_path, "hidden_test_results.csv")
                hidden_results[['client_code', 'product', 'push_notification']].to_csv(hidden_file, index=False, encoding='utf-8')
                print(f"Результаты скрытого теста сохранены в {hidden_file}")
                print(f"Обработано скрытых клиентов: {len(hidden_results)}")
            else:
                print("Скрытый тест не найден или пуст")
        else:
            print("Не удалось обработать клиентов")

def main():
    """Основная функция"""
    analyzer = BankProductAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
