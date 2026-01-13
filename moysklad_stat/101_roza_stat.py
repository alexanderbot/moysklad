import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Tuple, List, Optional
from collections import defaultdict
from contextlib import contextmanager
import hashlib
import json

import requests
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ConversationHandler, \
    MessageHandler, filters, JobQueue

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
MOYSKLAD_TOKEN = os.getenv('MOYSKLAD_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID')  # Добавим переменную для админа
MOYSKLAD_BASE_URL = 'https://api.moysklad.ru/api/remap/1.2'

HEADERS = {
    'Authorization': f'Bearer {MOYSKLAD_TOKEN}',
    'Accept-Encoding': 'gzip'
}

# Определяем состояния для ConversationHandler <-- ДОБАВЬТЕ ЭТО
(
    PERIOD_START_DATE,
    PERIOD_END_DATE
) = range(2)

# ============================================================
# МЕНЕДЖЕР ДЛЯ ХРАНЕНИЯ ТОКЕНОВ В JSON ФАЙЛЕ
# ============================================================

USER_TOKENS_FILE = 'user_tokens.json'


def load_user_tokens() -> Dict:
    """Загрузка токенов из JSON файла"""
    if os.path.exists(USER_TOKENS_FILE):
        try:
            with open(USER_TOKENS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки токенов: {e}")
            return {}
    return {}


def save_user_tokens(tokens: Dict):
    """Сохранение токенов в JSON файл"""
    try:
        with open(USER_TOKENS_FILE, 'w', encoding='utf-8') as f:
            json.dump(tokens, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        logger.error(f"Ошибка сохранения токенов: {e}")


def get_user_token(user_id: int) -> Optional[str]:
    """Получение токена пользователя"""
    tokens = load_user_tokens()
    user_data = tokens.get(str(user_id), {})
    return user_data.get('moysklad_token')


def get_user_info(user_id: int) -> Optional[Dict]:
    """Получение информации о пользователе"""
    tokens = load_user_tokens()
    user_data = tokens.get(str(user_id), {})
    return user_data


def set_user_token(user_id: int, token: str, **kwargs):
    """Установка токена пользователя"""
    tokens = load_user_tokens()
    user_id_str = str(user_id)

    if user_id_str not in tokens:
        tokens[user_id_str] = {}

    # Обновляем токен
    tokens[user_id_str]['moysklad_token'] = token

    # Обновляем дополнительные данные
    for key, value in kwargs.items():
        if value:  # Сохраняем только не пустые значения
            tokens[user_id_str][key] = value

    # Добавляем метаданные
    tokens[user_id_str]['updated_at'] = datetime.now().isoformat()

    save_user_tokens(tokens)


def delete_user_token(user_id: int):
    """Удаление токена пользователя"""
    tokens = load_user_tokens()
    user_id_str = str(user_id)

    if user_id_str in tokens:
        # Удаляем только токен, сохраняя другую информацию
        if 'moysklad_token' in tokens[user_id_str]:
            del tokens[user_id_str]['moysklad_token']

        # Очищаем организацию, если она была
        for key in ['organization_name', 'organization_inn', 'organization_email']:
            if key in tokens[user_id_str]:
                del tokens[user_id_str][key]

        save_user_tokens(tokens)


def update_user_activity(user_id: int, username: str = None, first_name: str = None, last_name: str = None):
    """Обновление активности пользователя"""
    tokens = load_user_tokens()
    user_id_str = str(user_id)

    if user_id_str not in tokens:
        tokens[user_id_str] = {}

    # Обновляем информацию о пользователе
    if username:
        tokens[user_id_str]['username'] = username
    if first_name:
        tokens[user_id_str]['first_name'] = first_name
    if last_name:
        tokens[user_id_str]['last_name'] = last_name

    # Обновляем время последней активности
    tokens[user_id_str]['last_activity'] = datetime.now().isoformat()

    save_user_tokens(tokens)


def get_all_users_with_tokens() -> List[Dict]:
    """Получение всех пользователей с токенами"""
    tokens = load_user_tokens()
    users_with_tokens = []

    for user_id_str, user_data in tokens.items():
        if 'moysklad_token' in user_data:
            users_with_tokens.append({
                'user_id': user_id_str,
                'username': user_data.get('username'),
                'first_name': user_data.get('first_name'),
                'last_name': user_data.get('last_name'),
                'organization_name': user_data.get('organization_name'),
                'last_activity': user_data.get('last_activity')
            })

    return users_with_tokens


# ============================================================
# УНИВЕРСАЛЬНЫЙ КЛИЕНТ МОЙСКЛАД (РАБОЧАЯ ВЕРСИЯ)
# ============================================================


class DebugMoySkladClient:
    def __init__(self, user_id: int = None):
        self.base_url = MOYSKLAD_BASE_URL
        self.user_id = user_id

        # Получаем токен пользователя или используем глобальный
        user_token = get_user_token(user_id) if user_id else None
        self.token = user_token or MOYSKLAD_TOKEN

        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept-Encoding': 'gzip'
        }
        self.timeout = 30

    def is_token_valid(self) -> Tuple[bool, str]:
        """Проверяет валидность токена"""
        try:
            response = requests.get(
                f"{self.base_url}/entity/company",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                org_name = data.get('name', 'Неизвестно')
                return True, org_name
            else:
                return False, f"Ошибка {response.status_code}"

        except Exception as e:
            return False, f"Ошибка: {str(e)}"

    def get_organization_info(self) -> Dict:
        """Получает информацию об организации"""
        try:
            response = requests.get(
                f"{self.base_url}/entity/company",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    'name': data.get('name', 'Неизвестно'),
                    'inn': data.get('inn', 'Не указан'),
                    'email': data.get('email', 'Не указан'),
                    'phone': data.get('phone', 'Не указан')
                }
        except Exception:
            pass
        return {}

    def get_debug_sales_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Просто передаем данные как есть, без обработки"""
        try:
            # Используем только даты (без времени) для фильтра
            start_date_only = start_date.split()[0] if ' ' in start_date else start_date
            end_date_only = end_date.split()[0] if ' ' in end_date else end_date

            filter_params = {
                'filter': f'moment>={start_date_only} 00:00:00;moment<={end_date_only} 23:59:59',
                'limit': 100,
                'expand': 'agent'
            }

            logger.info(f"DEBUG ЗАПРОС ОПТОВЫХ ПРОДАЖ: {filter_params['filter']}")

            response = requests.get(
                f"{self.base_url}/entity/demand",
                headers=self.headers,
                params=filter_params,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()

            total_sales = Decimal('0')
            count = 0
            sales_data = []

            if 'rows' in data:
                logger.info(f"DEBUG: Получено {len(data['rows'])} строк")

                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        agent_info = None
                        if 'agent' in row and row['agent']:
                            agent = row['agent']

                            # СЫРЫЕ данные - логируем ВСЕ
                            raw_name = agent.get('name')
                            logger.info(f"DEBUG строка {i}: agent.get('name') = '{raw_name}' (тип: {type(raw_name)})")

                            # ПРОСТО берем как есть
                            agent_name = raw_name

                            # Если None - ставим 'Без имени', иначе оставляем как есть
                            if agent_name is None:
                                agent_name = 'Без имени'
                            else:
                                # НИКАКИХ преобразований!
                                agent_name = str(agent_name)

                            agent_info = {
                                'id': agent.get('meta', {}).get('href', '').split('/')[-1],
                                'name': agent_name,
                                'phone': agent.get('phone', 'Не указан'),
                                'email': agent.get('email', 'Не указан')
                            }

                        sale_amount = Decimal(str(row['sum'] / 100))
                        total_sales += sale_amount
                        count += 1

                        sales_data.append({
                            'id': row['id'],
                            'moment': row.get('moment', ''),
                            'sum': sale_amount,
                            'agent': agent_info
                        })

            logger.info(f"DEBUG ИТОГ: {count} продаж")
            if sales_data and sales_data[0]['agent']:
                logger.info(f"DEBUG первый агент: '{sales_data[0]['agent']['name']}'")

            return count, total_sales, sales_data

        except Exception as e:
            logger.error(f"DEBUG Ошибка: {e}", exc_info=True)
            return 0, Decimal('0'), []

    def get_debug_stats(self, start_date: str, end_date: str) -> Dict:
        """Простая статистика для отладки"""
        try:
            count, total_sales, sales = self.get_debug_sales_data(start_date, end_date)

            logger.info(f"=== DEBUG STATS для {start_date} - {end_date} ===")

            customers = {}

            for i, sale in enumerate(sales):
                if sale['agent']:
                    agent = sale['agent']
                    agent_id = agent['id']

                    logger.info(f"Продажа {i}: agent['name'] = '{agent['name']}'")

                    if agent_id not in customers:
                        customers[agent_id] = {
                            'id': agent_id,
                            'name': agent['name'],
                            'phone': agent['phone'],
                            'email': agent['email'],
                            'orders': 0,
                            'total': Decimal('0')
                        }

                    customers[agent_id]['orders'] += 1
                    customers[agent_id]['total'] += sale['sum']

            # Топ покупателей
            all_customers = list(customers.values())
            top_customers = sorted(all_customers, key=lambda x: x['total'], reverse=True)[:10]

            logger.info(f"DEBUG: всего покупателей {len(customers)}")
            if customers:
                for cust_id, cust in list(customers.items())[:3]:
                    logger.info(f"  • {cust['name']} - {cust['total']} ₽")

            return {
                'new_customers': 0,
                'returning_customers': 0,
                'customer_count': len(customers),
                'new_customers_list': [],
                'returning_customers_list': [],
                'top_customers': top_customers,
                'total_orders': count,
                'total_sales': total_sales
            }

        except Exception as e:
            logger.error(f"DEBUG Ошибка stats: {e}")
            return {
                'new_customers': 0, 'returning_customers': 0, 'customer_count': 0,
                'new_customers_list': [], 'returning_customers_list': [], 'top_customers': [],
                'total_orders': 0, 'total_sales': Decimal('0')
            }

    def get_incoming_payments_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о входящих платежах за период"""
        try:
            filter_params = {
                'filter': f'moment>={start_date};moment<={end_date}',
                'limit': 100,
                'expand': 'agent'
            }

            logger.info(f"DEBUG ЗАПРОС ВХОДЯЩИХ ПЛАТЕЖЕЙ: {start_date} - {end_date}")

            response = requests.get(
                f"{self.base_url}/entity/paymentin",
                headers=self.headers,
                params=filter_params,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API при запросе платежей: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()

            total_amount = Decimal('0')
            count = 0
            payments_data = []

            if 'rows' in data:
                logger.info(f"DEBUG: Получено {len(data['rows'])} платежей")

                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        agent_info = None
                        if 'agent' in row and row['agent']:
                            agent = row['agent']

                            raw_name = agent.get('name')
                            logger.info(f"DEBUG платеж {i}: agent.get('name') = '{raw_name}'")

                            agent_name = raw_name
                            if agent_name is None:
                                agent_name = 'Без имени'
                            else:
                                agent_name = str(agent_name)

                            agent_info = {
                                'id': agent.get('meta', {}).get('href', '').split('/')[-1],
                                'name': agent_name,
                                'phone': agent.get('phone', 'Не указан'),
                                'email': agent.get('email', 'Не указан')
                            }

                        payment_amount = Decimal(str(row['sum'] / 100))
                        total_amount += payment_amount
                        count += 1

                        payments_data.append({
                            'id': row['id'],
                            'moment': row.get('moment', ''),
                            'sum': payment_amount,
                            'agent': agent_info,
                            'payment_type': row.get('paymentType', {}).get('name', 'Не указан')
                        })

            logger.info(f"DEBUG ИТОГ ПЛАТЕЖЕЙ: {count} платежей на сумму {total_amount} ₽")
            return count, total_amount, payments_data

        except Exception as e:
            logger.error(f"DEBUG Ошибка при получении платежей: {e}", exc_info=True)
            return 0, Decimal('0'), []

    def get_incoming_payments_stats(self, start_date: str, end_date: str) -> Dict:
        """Получает статистику по входящим платежам"""
        try:
            count, total_amount, payments = self.get_incoming_payments_data(start_date, end_date)

            logger.info(f"=== DEBUG STATS ПЛАТЕЖИ для {start_date} - {end_date} ===")

            # Группировка по контрагентам
            customers = {}
            payment_types = defaultdict(Decimal)

            for payment in payments:
                if payment['agent']:
                    agent = payment['agent']
                    agent_id = agent['id']

                    if agent_id not in customers:
                        customers[agent_id] = {
                            'id': agent_id,
                            'name': agent['name'],
                            'phone': agent['phone'],
                            'email': agent['email'],
                            'payments': 0,
                            'total': Decimal('0')
                        }

                    customers[agent_id]['payments'] += 1
                    customers[agent_id]['total'] += payment['sum']

                # Суммирование по типам платежей
                payment_type = payment.get('payment_type', 'Не указан')
                payment_types[payment_type] += payment['sum']

            # Топ плательщиков
            all_customers = list(customers.values())
            top_payers = sorted(all_customers, key=lambda x: x['total'], reverse=True)[:10]

            # Статистика по типам платежей
            payment_types_stats = [
                {'type': k, 'total': v, 'count': sum(1 for p in payments if p.get('payment_type') == k)}
                for k, v in payment_types.items()
            ]
            payment_types_stats.sort(key=lambda x: x['total'], reverse=True)

            logger.info(f"DEBUG: всего плательщиков {len(customers)}")
            logger.info(f"DEBUG: типы платежей {len(payment_types_stats)}")

            return {
                'total_payments': count,
                'total_amount': total_amount,
                'customer_count': len(customers),
                'top_payers': top_payers,
                'payment_types': payment_types_stats,
                'payments_data': payments
            }

        except Exception as e:
            logger.error(f"DEBUG Ошибка stats платежей: {e}")
            return {
                'total_payments': 0,
                'total_amount': Decimal('0'),
                'customer_count': 0,
                'top_payers': [],
                'payment_types': [],
                'payments_data': []
            }

    def get_daily_summary(self) -> Dict:
        """Получает сводку за сегодня (заказы покупателей + розница)"""
        try:
            # Получаем даты за сегодня
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = datetime.now()

            start_date = today_start.strftime('%Y-%m-%d %H:%M:%S')
            end_date = today_end.strftime('%Y-%m-%d %H:%M:%S')

            # Получаем статистику заказов покупателей
            orders_count, orders_total, orders_data = self.get_customer_orders_data(start_date, end_date)

            # Получаем статистику розничных продаж
            retail_count, retail_total, retail_data = self.get_retail_sales_data(start_date, end_date)

            # Получаем статистику платежей
            payments_count, payments_total, payments_data = self.get_incoming_payments_data(start_date, end_date)

            # Общая статистика по продажам
            total_sales_count = orders_count + retail_count
            total_sales_amount = orders_total + retail_total

            # Группировка покупателей по заказам
            customers = {}
            for order in orders_data:
                if order['agent']:
                    agent = order['agent']
                    agent_id = agent['id']

                    if agent_id not in customers:
                        customers[agent_id] = {
                            'id': agent_id,
                            'name': agent['name'],
                            'phone': agent['phone'],
                            'orders': 0,
                            'total': Decimal('0')
                        }

                    customers[agent_id]['orders'] += 1
                    customers[agent_id]['total'] += order['sum']

            # Топ 3 покупателя по заказам
            all_customers = list(customers.values())
            top_customers = sorted(all_customers, key=lambda x: x['total'], reverse=True)[:3]

            # Группировка плательщиков
            payers = {}
            for payment in payments_data:
                if payment['agent']:
                    agent = payment['agent']
                    agent_id = agent['id']

                    if agent_id not in payers:
                        payers[agent_id] = {
                            'id': agent_id,
                            'name': agent['name'],
                            'phone': agent['phone'],
                            'payments': 0,
                            'total': Decimal('0')
                        }

                    payers[agent_id]['payments'] += 1
                    payers[agent_id]['total'] += payment['sum']

            # Топ 3 плательщика
            all_payers = list(payers.values())
            top_payers = sorted(all_payers, key=lambda x: x['total'], reverse=True)[:3]

            # Рассчитываем средние значения
            avg_order = orders_total / orders_count if orders_count > 0 else Decimal('0')
            avg_retail = retail_total / retail_count if retail_count > 0 else Decimal('0')
            avg_total_sales = total_sales_amount / total_sales_count if total_sales_count > 0 else Decimal('0')
            avg_payment = payments_total / payments_count if payments_count > 0 else Decimal('0')

            return {
                'date': today_start.strftime('%d.%m.%Y'),
                'customer_orders': {
                    'count': orders_count,
                    'total': orders_total,
                    'avg_order': avg_order
                },
                'retail': {
                    'count': retail_count,
                    'total': retail_total,
                    'avg_order': avg_retail
                },
                'total_sales': {
                    'count': total_sales_count,
                    'total': total_sales_amount,
                    'avg_order': avg_total_sales
                },
                'payments': {
                    'count': payments_count,
                    'total': payments_total,
                    'avg_payment': avg_payment
                },
                'top_customers': top_customers,
                'top_payers': top_payers,
                'unique_customers': len(customers),
                'unique_payers': len(payers)
            }

        except Exception as e:
            logger.error(f"Ошибка при получении ежедневной сводки: {e}", exc_info=True)
            return {
                'date': datetime.now().strftime('%d.%m.%Y'),
                'customer_orders': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'retail': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'total_sales': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'payments': {'count': 0, 'total': Decimal('0'), 'avg_payment': Decimal('0')},
                'top_customers': [],
                'top_payers': [],
                'unique_customers': 0,
                'unique_payers': 0
            }

    def get_retail_sales_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о розничных продажах за период"""
        try:
            # Используем только даты (без времени) для фильтра
            start_date_only = start_date.split()[0] if ' ' in start_date else start_date
            end_date_only = end_date.split()[0] if ' ' in end_date else end_date

            filter_params = {
                'filter': f'moment>={start_date_only} 00:00:00;moment<={end_date_only} 23:59:59',
                'limit': 1000,
            }

            response = requests.get(
                f"{self.base_url}/entity/retaildemand",
                headers=self.headers,
                params=filter_params,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API при запросе розничных продаж: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()

            total_sales = Decimal('0')
            count = 0
            sales_data = []

            if 'rows' in data:
                logger.info(f"DEBUG: Получено {len(data['rows'])} розничных продаж")

                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        # Для розничных продаж контрагента может не быть
                        agent_info = {
                            'id': 'retail_customer',
                            'name': 'Розничный клиент',
                            'phone': 'Не указан',
                            'email': 'Не указан'
                        }

                        sale_amount = Decimal(str(row['sum'] / 100))
                        total_sales += sale_amount
                        count += 1

                        sales_data.append({
                            'id': row['id'],
                            'moment': row.get('moment', ''),
                            'sum': sale_amount,
                            'agent': agent_info,
                            'retail': True  # Флаг что это розничная продажа
                        })

            logger.info(f"DEBUG ИТОГ РОЗНИЦА: {count} продаж на сумму {total_sales} ₽")
            return count, total_sales, sales_data

        except Exception as e:
            logger.error(f"DEBUG Ошибка при получении розничных продаж: {e}", exc_info=True)
            return 0, Decimal('0'), []

    def get_sales_stats_with_retail(self, start_date: str, end_date: str) -> Dict:
        """Получает статистику продаж с разделением на заказы покупателей и розницу"""
        try:

            # Получаем заказы покупателей
            orders_count, orders_total, orders_data = self.get_customer_orders_data(start_date, end_date)

            # Получаем розничные продажи
            retail_count, retail_total, retail_data = self.get_retail_sales_data(start_date, end_date)

            # Общие продажи
            total_count = orders_count + retail_count
            total_amount = orders_total + retail_total

            # Группировка покупателей
            customers = {}

            for i, order in enumerate(orders_data):
                if order['agent']:
                    agent = order['agent']
                    agent_id = agent['id']

                    if agent_id not in customers:
                        customers[agent_id] = {
                            'id': agent_id,
                            'name': agent['name'],
                            'phone': agent['phone'],
                            'email': agent['email'],
                            'orders': 0,
                            'total': Decimal('0')
                        }

                    customers[agent_id]['orders'] += 1
                    customers[agent_id]['total'] += order['sum']
                else:
                    logger.info(f"   Заказ {i + 1}: агента нет")

            # Топ покупателей (только по заказам)
            all_customers = list(customers.values())
            top_customers = sorted(all_customers, key=lambda x: x['total'], reverse=True)[:10]

            # Рассчитываем средние чеки
            avg_order = orders_total / orders_count if orders_count > 0 else Decimal('0')
            avg_retail = retail_total / retail_count if retail_count > 0 else Decimal('0')
            avg_total = total_amount / total_count if total_count > 0 else Decimal('0')

            # Подсчет новых и постоянных покупателей (только для заказов)
            new_customers = sum(1 for cust in customers.values() if cust['orders'] == 1)
            returning_customers = sum(1 for cust in customers.values() if cust['orders'] > 1)

            # Списки новых и постоянных покупателей
            new_customers_list = [cust for cust in customers.values() if cust['orders'] == 1]
            returning_customers_list = [cust for cust in customers.values() if cust['orders'] > 1]

            return {
                'customer_orders': {
                    'count': orders_count,
                    'total': orders_total,
                    'avg_order': avg_order
                },
                'retail': {
                    'count': retail_count,
                    'total': retail_total,
                    'avg_order': avg_retail
                },
                'total_sales': {
                    'count': total_count,
                    'total': total_amount,
                    'avg_order': avg_total
                },
                'customer_count': len(customers),
                'new_customers': new_customers,
                'returning_customers': returning_customers,
                'top_customers': top_customers,
                'new_customers_list': new_customers_list,
                'returning_customers_list': returning_customers_list
            }

        except Exception as e:
            logger.error(f"Ошибка при получении статистики с заказами покупателей: {e}", exc_info=True)
            return {
                'customer_orders': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'retail': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'total_sales': {'count': 0, 'total': Decimal('0'), 'avg_order': Decimal('0')},
                'customer_count': 0,
                'new_customers': 0,
                'returning_customers': 0,
                'top_customers': [],
                'new_customers_list': [],
                'returning_customers_list': []
            }

    def get_customer_orders_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о заказах покупателей - с дополнительной загрузкой контрагентов"""
        try:
            # Используем только даты (без времени) для фильтра
            start_date_only = start_date.split()[0] if ' ' in start_date else start_date
            end_date_only = end_date.split()[0] if ' ' in end_date else end_date

            filter_params = {
                'filter': f'moment>={start_date_only} 00:00:00;moment<={end_date_only} 23:59:59',
                'limit': 1000,
                'expand': 'agent'  # Уже есть
            }

            response = requests.get(
                f"{self.base_url}/entity/customerorder",
                headers=self.headers,
                params=filter_params,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"❌ Ошибка API: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()

            total_amount = Decimal('0')
            count = 0
            orders_data = []

            if 'rows' in data:

                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        agent_info = None

                        # Проверяем наличие агента
                        if 'agent' in row and row['agent']:
                            agent = row['agent']
                            agent_href = agent.get('meta', {}).get('href')

                            if agent_href:
                                # Пробуем загрузить полные данные контрагента
                                try:
                                    agent_response = requests.get(
                                        agent_href,
                                        headers=self.headers,
                                        timeout=10
                                    )

                                    if agent_response.status_code == 200:
                                        agent_full = agent_response.json()

                                        # Получаем имя из разных возможных полей
                                        agent_name = (
                                                agent_full.get('name') or
                                                agent_full.get('legalTitle') or
                                                agent_full.get('companyType') or
                                                agent_full.get('code') or
                                                f"Клиент {agent_full.get('id', 'unknown')[:8]}"
                                        )

                                        agent_info = {
                                            'id': agent_full.get('id', ''),
                                            'name': str(agent_name) if agent_name else 'Без имени',
                                            'phone': agent_full.get('phone', 'Не указан'),
                                            'email': agent_full.get('email', 'Не указан')
                                        }
                                    else:
                                        logger.warning(
                                            f"   ⚠️ Не удалось загрузить контрагента: {agent_response.status_code}")
                                        agent_info = {
                                            'id': agent_href.split('/')[-1],
                                            'name': 'Без имени',
                                            'phone': 'Не указан',
                                            'email': 'Не указан'
                                        }

                                except Exception as agent_error:
                                    logger.error(f"   ❌ Ошибка загрузки контрагента: {agent_error}")
                                    agent_info = {
                                        'id': agent_href.split('/')[-1] if agent_href else 'unknown',
                                        'name': 'Без имени',
                                        'phone': 'Не указан',
                                        'email': 'Не указан'
                                    }
                            else:
                                logger.info(f"   ⚠️ Нет href у агента")
                                agent_info = {
                                    'id': 'no_href',
                                    'name': 'Без имени',
                                    'phone': 'Не указан',
                                    'email': 'Не указан'
                                }
                        else:
                            logger.info(f"🔎 Заказ {i + 1}: агента нет")
                            agent_info = {
                                'id': 'no_agent',
                                'name': 'Без имени',
                                'phone': 'Не указан',
                                'email': 'Не указан'
                            }

                        order_amount = Decimal(str(row['sum'] / 100))
                        total_amount += order_amount
                        count += 1

                        orders_data.append({
                            'id': row['id'],
                            'moment': row.get('moment', ''),
                            'sum': order_amount,
                            'agent': agent_info,
                            'customer_order': True
                        })

                logger.info(f"📦 ИТОГО: {count} заказов на сумму {total_amount} ₽")

                # Детальный лог
                if orders_data:
                    for i, order in enumerate(orders_data[:5], 1):
                        agent_name = order['agent']['name'] if order['agent'] else 'Нет агента'

            return count, total_amount, orders_data

        except Exception as e:
            logger.error(f"💥 Ошибка: {e}", exc_info=True)
            return 0, Decimal('0'), []


# ============================================================
# ТЕЛЕГРАМ БОТ С ПОЛНОЙ СТАТИСТИКОЙ (ДОБАВЛЕН ПЕРИОД)
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    welcome_text = """
    🤖 *Бот статистики МойСклад - Полная версия*

    📊 *Доступные команды:*
    /today - Статистика за сегодня
    /week - Статистика за неделю
    /month - Статистика за месяц
    /period - Статистика за указанный период
    /top - Топ покупателей за месяц
    /customers - Меню статистики покупателей
    /payments - Входящие платежи
    /daily - Итоги дня
    /help - Справка

    """

    keyboard = [
        [
            InlineKeyboardButton("📅 Сегодня", callback_data='today'),
            InlineKeyboardButton("📆 Неделя", callback_data='week')
        ],
        [
            InlineKeyboardButton("📈 Месяц", callback_data='month'),
            InlineKeyboardButton("🏆 Топ", callback_data='top')
        ],
        [
            InlineKeyboardButton("📊 Произвольный период", callback_data='period_menu')
        ],
        [
            InlineKeyboardButton("👥 Покупатели", callback_data='customers_menu'),
            InlineKeyboardButton("💰 Платежи", callback_data='payments_menu')
        ],
        [InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')


async def period_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало ввода произвольного периода - шаг 1"""
    keyboard = [
        [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
📊 *Статистика за произвольный период*

📝 *Как указать период:*
1. Напишите начальную дату в формате *ДД.ММ.ГГГГ*
   Например: *01.01.2024*

2. Затем напишите конечную дату в том же формате
   Например: *31.01.2024*

📅 *Пример полного запроса:*
01.01.2024
31.01.2024
💡 *Совет:* Вы можете указать любой период от 1 дня до нескольких лет.

*Отправьте начальную дату:*
"""

    if update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.callback_query.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    # Возвращаем состояние, чтобы указать, что ожидается начальная дата
    return PERIOD_START_DATE


async def handle_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода начальной даты - шаг 2"""
    user_input = update.message.text.strip()

    # Проверяем и парсим дату
    try:
        date_formats = ['%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        date_obj = None

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(user_input, fmt)
                # Приводим к стандартному формату ДД.ММ.ГГГГ
                date_str = date_obj.strftime('%d.%m.%Y')
                break
            except ValueError:
                continue

        if date_obj is None:
            await update.message.reply_text(
                "❌ *Неверный формат даты!*\n\n"
                "Пожалуйста, введите дату в формате ДД.ММ.ГГГГ\n"
                "Например: 01.01.2024\n\n"
                "Попробуйте снова:"
            )
            return PERIOD_START_DATE  # Остаемся в том же состоянии

        # Сохраняем начальную дату в context.user_data
        context.user_data['period_start_date'] = date_str

        await update.message.reply_text(
            f"✅ *Начальная дата принята:* {date_str}\n\n"
            "📅 Теперь введите конечную дату в том же формате:\n"
            "Например: 31.01.2024"
        )

        return PERIOD_END_DATE

    except Exception as e:
        logger.error(f"Ошибка при обработке начальной даты: {e}")
        await update.message.reply_text(
            "❌ *Ошибка при обработке даты!*\n\n"
            "Пожалуйста, введите дату в формате ДД.ММ.ГГГГ\n"
            "Например: 01.01.2024\n\n"
            "Попробуйте снова:"
        )
        return PERIOD_START_DATE


async def cancel_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена ввода периода"""
    # Очищаем временные данные
    if 'period_start_date' in context.user_data:
        del context.user_data['period_start_date']

    await update.message.reply_text(
        "❌ *Ввод периода отменен.*\n\n"
        "Для ввода нового периода используйте команду /period"
    )

    return ConversationHandler.END


async def handle_period_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода дат для произвольного периода"""
    try:
        user_data = context.user_data

        if 'awaiting_period' not in user_data:
            return

        user_input = update.message.text.strip()

        # Проверяем формат даты
        try:
            # Пробуем разные форматы дат
            date_formats = [
                '%d.%m.%Y',  # 01.01.2024
                '%d.%m.%y',  # 01.01.24
                '%d/%m/%Y',  # 01/01/2024
                '%Y-%m-%d',  # 2024-01-01
                '%d-%m-%Y',  # 01-01-2024
            ]

            date_obj = None
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(user_input, fmt)
                    break
                except ValueError:
                    continue

            if date_obj is None:
                await update.message.reply_text(
                    "❌ *Неверный формат даты!*\n\n"
                    "Пожалуйста, введите дату в формате ДД.ММ.ГГГГ\n"
                    "Например: 01.01.2024"
                )
                return

        except Exception as e:
            await update.message.reply_text(
                "❌ *Ошибка при обработке даты!*\n\n"
                f"Ошибка: {str(e)}\n\n"
                "Пожалуйста, введите дату в формате ДД.ММ.ГГГГ\n"
                "Например: 01.01.2024"
            )
            return

        if user_data['awaiting_period'] == 'start_date':
            # Сохраняем начальную дату
            user_data['period_start'] = date_obj
            user_data['awaiting_period'] = 'end_date'

            await update.message.reply_text(
                f"✅ *Начальная дата принята:* {date_obj.strftime('%d.%m.%Y')}\n\n"
                "📅 Теперь введите конечную дату:\n"
                "Например: 31.01.2024"
            )

        elif user_data['awaiting_period'] == 'end_date':
            # Сохраняем конечную дату
            end_date = date_obj

            # Проверяем, что конечная дата не раньше начальной
            if end_date < user_data['period_start']:
                await update.message.reply_text(
                    "❌ *Конечная дата не может быть раньше начальной!*\n\n"
                    f"Начальная дата: {user_data['period_start'].strftime('%d.%m.%Y')}\n"
                    f"Конечная дата: {end_date.strftime('%d.%m.%Y')}\n\n"
                    "Пожалуйста, введите конечную дату заново:"
                )
                return

            # Формируем период
            start_date_str = user_data['period_start'].strftime('%Y-%m-%d %H:%M:%S')
            end_date_str = end_date.strftime('%Y-%m-%d 23:59:59')  # Добавляем время до конца дня

            # Очищаем состояние
            del user_data['awaiting_period']
            del user_data['period_start']

            # Отправляем статистику за период
            await send_period_statistics(update, start_date_str, end_date_str,
                                         user_data['period_start'].strftime('%d.%m.%Y'),
                                         end_date.strftime('%d.%m.%Y'))

    except Exception as e:
        logger.error(f"Ошибка в handle_period_input: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")


async def handle_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода конечной даты - шаг 3"""
    logger.info(f"handle_end_date вызван с текстом: {update.message.text}")

    user_input = update.message.text.strip()

    try:
        date_formats = ['%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        end_date_obj = None

        for fmt in date_formats:
            try:
                end_date_obj = datetime.strptime(user_input, fmt)
                end_date_str = end_date_obj.strftime('%d.%m.%Y')
                logger.info(f"Дата распознана: {end_date_str} (формат: {fmt})")
                break
            except ValueError:
                continue

        if end_date_obj is None:
            error_msg = "❌ *Неверный формат даты!*\n\nПожалуйста, введите дату в формате ДД.ММ.ГГГГ\nНапример: 31.01.2024\n\nПопробуйте снова:"
            logger.warning(f"Не удалось распознать дату: {user_input}")
            await update.message.reply_text(error_msg, parse_mode='Markdown')
            return PERIOD_END_DATE

        # Получаем начальную дату из context.user_data
        start_date_str = context.user_data.get('period_start_date')
        logger.info(f"Начальная дата из контекста: {start_date_str}")

        if not start_date_str:
            error_msg = "❌ *Ошибка: не найдена начальная дата!*\n\nНачните заново командой /period"
            logger.error("Не найдена начальная дата в контексте")
            await update.message.reply_text(error_msg, parse_mode='Markdown')
            return ConversationHandler.END

        # Преобразуем строки в даты для сравнения
        start_date_obj = datetime.strptime(start_date_str, '%d.%m.%Y')
        logger.info(f"Начальная дата объект: {start_date_obj}")
        logger.info(f"Конечная дата объект: {end_date_obj}")

        # Проверяем, что конечная дата не раньше начальной
        if end_date_obj < start_date_obj:
            error_msg = f"❌ *Конечная дата не может быть раньше начальной!*\n\nНачальная дата: {start_date_str}\nКонечная дата: {end_date_str}\n\nПожалуйста, введите конечную дату заново:"
            logger.warning(f"Конечная дата раньше начальной: {end_date_str} < {start_date_str}")
            await update.message.reply_text(error_msg, parse_mode='Markdown')
            return PERIOD_END_DATE

        # Формируем даты для API
        start_date_api = start_date_obj.strftime('%Y-%m-%d 00:00:00')
        end_date_api = end_date_obj.strftime('%Y-%m-%d 23:59:59')
        logger.info(f"Даты для API: {start_date_api} - {end_date_api}")

        # Очищаем временные данные
        if 'period_start_date' in context.user_data:
            del context.user_data['period_start_date']

        # Показываем сообщение о загрузке
        await update.message.reply_text("⏳ *Загружаю статистику...*", parse_mode='Markdown')

        # Отправляем статистику
        await send_period_statistics(
            update,
            start_date_api,
            end_date_api,
            start_date_str,
            end_date_str
        )

        logger.info(f"Статистика отправлена за период: {start_date_str} - {end_date_str}")
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка при обработке конечной даты: {e}", exc_info=True)
        error_msg = f"❌ *Ошибка при обработке даты!*\n\nПожалуйста, введите дату в формате ДД.ММ.ГГГГ\nНапример: 31.01.2024\n\nОшибка: {str(e)}\n\nПопробуйте снова:"
        await update.message.reply_text(error_msg, parse_mode='Markdown')
        return PERIOD_END_DATE


async def send_period_statistics(update: Update, start_date: str, end_date: str,
                                 start_date_display: str, end_date_display: str):
    """Отправляет статистику за произвольный период"""
    try:
        logger.info(f"send_period_statistics вызван: {start_date_display} - {end_date_display}")

        # Создаем клиент
        client = DebugMoySkladClient()
        logger.info("Клиент создан")

        # Получаем статистику
        logger.info(f"Запрашиваю статистику за {start_date} - {end_date}")
        stats = client.get_sales_stats_with_retail(start_date, end_date)
        logger.info(f"Статистика получена: {stats.get('total_sales', {}).get('count', 0)} продаж")

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        # Рассчитываем длительность периода
        start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
        end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')
        days_count = (end_date_obj - start_date_obj).days + 1

        message = f"""
📊 *Статистика продаж за период*

📅 Период: *{start_date_display} - {end_date_display}*
⏱️ Длительность: *{days_count}* дней

🛒 *ЗАКАЗЫ ПОКУПАТЕЛЕЙ:*
• Количество заказов: *{stats['customer_orders']['count']}*
• Общая сумма: *{stats['customer_orders']['total']:,.2f} ₽*
• Средний чек: *{stats['customer_orders']['avg_order']:,.2f} ₽*
• Уникальных покупателей: *{stats['customer_count']}*

🏪 *РОЗНИЧНЫЕ ПРОДАЖИ:*
• Количество продаж: *{stats['retail']['count']}*
• Общая сумма: *{stats['retail']['total']:,.2f} ₽*
• Средний чек: *{stats['retail']['avg_order']:,.2f} ₽*

📈 *ОБЩАЯ СТАТИСТИКА ПРОДАЖ:*
• Всего продаж: *{stats['total_sales']['count']}*
• Общая сумма: *{stats['total_sales']['total']:,.2f} ₽*
• Средний чек: *{stats['total_sales']['avg_order']:,.2f} ₽*
"""

        # Добавляем анализ покупателей только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""
👤 *Анализ покупателей (по заказам):*
• Новые покупатели (1 заказ): *{stats['new_customers']}*
• Постоянные покупатели (>1 заказа): *{stats['returning_customers']}*
• Соотношение новых/постоянных: *{calculate_ratio(stats['new_customers'], stats['returning_customers'])}*
"""
        else:
            message += f"""
👤 *Анализ покупателей:*
• Заказов покупателей нет - статистика недоступна
"""

        # Добавляем средние показатели в день
        if days_count > 0:
            avg_per_day = {
                'orders': stats['customer_orders']['count'] / days_count,
                'retail': stats['retail']['count'] / days_count,
                'total_sales': stats['total_sales']['count'] / days_count,
                'total_amount': stats['total_sales']['total'] / days_count,
            }

            message += f"""
📊 *Средние показатели в день:*
• Заказы покупателей: *{avg_per_day['orders']:.1f}* в день
• Розничные продажи: *{avg_per_day['retail']:.1f}* в день
• Всего продаж: *{avg_per_day['total_sales']:.1f}* в день
• Средняя выручка: *{avg_per_day['total_amount']:,.2f} ₽* в день
"""

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton("👥 Детали по покупателям",
                                     callback_data=f'customers_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton("🏆 Топ покупателей",
                                     callback_data=f'top_custom_{start_date_display}_{end_date_display}')
            ],
            [
                InlineKeyboardButton("💰 Платежи за период",
                                     callback_data=f'payments_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [
                InlineKeyboardButton("📅 Новый период", callback_data='period_menu'),
                InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            try:
                await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            except Exception as edit_error:
                if "Message is not modified" in str(edit_error):
                    logger.info("Сообщение не изменилось при обновлении - игнорируем")
                    await update.callback_query.answer("✅ Данные актуальны")
                else:
                    raise edit_error


    except Exception as e:

        logger.error(f"Ошибка в send_period_statistics: {e}", exc_info=True)

        error_msg = f"❌ Ошибка при получении статистики за период {start_date_display} - {end_date_display}: {str(e)}"

        if isinstance(update, Update) and update.message:

            await update.message.reply_text(error_msg)

        else:

            try:

                await update.edit_message_text(error_msg)

            except Exception:

                await update.callback_query.message.reply_text(error_msg)


async def period_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню выбора произвольного периода"""
    keyboard = [
        [
            InlineKeyboardButton("📅 Ввести период", callback_data='enter_period'),
            InlineKeyboardButton("📆 Быстрый выбор", callback_data='quick_periods')
        ],
        [
            InlineKeyboardButton("📅 Сегодня", callback_data='today'),
            InlineKeyboardButton("📆 Неделя", callback_data='week'),
            InlineKeyboardButton("📈 Месяц", callback_data='month')
        ],
        [
            InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
📊 *Статистика за произвольный период*

Выберите вариант:
• *Ввести период* - укажите начальную и конечную даты
• *Быстрый выбор* - выберите из готовых вариантов

📝 *Формат дат:* ДД.ММ.ГГГГ
Пример: 01.01.2024 - 31.01.2024
"""

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def quick_periods_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Быстрый выбор периода"""
    # Рассчитываем даты для быстрых периодов
    now = datetime.now()

    # Последние 7 дней
    last_7_days_start = (now - timedelta(days=7)).strftime('%d.%m.%Y')
    last_7_days_end = now.strftime('%d.%m.%Y')

    # Последние 30 дней
    last_30_days_start = (now - timedelta(days=30)).strftime('%d.%m.%Y')
    last_30_days_end = now.strftime('%d.%m.%Y')

    # Текущий квартал
    current_month = now.month
    quarter_start_month = ((current_month - 1) // 3) * 3 + 1
    quarter_start = datetime(now.year, quarter_start_month, 1).strftime('%d.%m.%Y')
    quarter_end = now.strftime('%d.%m.%Y')

    # Текущий год
    year_start = datetime(now.year, 1, 1).strftime('%d.%m.%Y')
    year_end = now.strftime('%d.%m.%Y')

    keyboard = [
        [
            InlineKeyboardButton("📅 Последние 7 дней",
                                 callback_data=f'quick_period_{last_7_days_start}_{last_7_days_end}'),
            InlineKeyboardButton("📅 Последние 30 дней",
                                 callback_data=f'quick_period_{last_30_days_start}_{last_30_days_end}')
        ],
        [
            InlineKeyboardButton("📅 Текущий квартал",
                                 callback_data=f'quick_period_{quarter_start}_{quarter_end}'),
            InlineKeyboardButton("📅 Текущий год",
                                 callback_data=f'quick_period_{year_start}_{year_end}')
        ],
        [
            InlineKeyboardButton("📝 Ввести вручную", callback_data='enter_period'),
            InlineKeyboardButton("🔙 Назад", callback_data='period_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
📊 *Быстрый выбор периода*

Выберите один из готовых периодов:
• *Последние 7 дней* - статистика за неделю
• *Последние 30 дней* - статистика за месяц
• *Текущий квартал* - с начала квартала
• *Текущий год* - с начала года

Или введите период вручную
"""

    await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def enter_period_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать ввод периода вручную"""
    keyboard = [
        [
            InlineKeyboardButton("🔙 Назад", callback_data='period_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
📊 *Ввод произвольного периода*

📝 *Как указать период:*
1. Напишите начальную дату в формате *ДД.ММ.ГГГГ*
   Например: *01.01.2024*

2. Затем напишите конечную дату в том же формате
   Например: *31.01.2024*

📅 *Пример полного запроса:*
01.01.2024
31.01.2024

💡 *Совет:* Вы можете указать любой период от 1 дня до нескольких лет.

⚠️ *Внимание:* После отправки сообщения с датой бот будет ожидать следующую дату автоматически.

*Отправьте начальную дату:*
"""

    await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    # Устанавливаем состояние ожидания ввода периода
    context.user_data['awaiting_period'] = 'start_date'

    # Отправляем отдельное сообщение, чтобы можно было ответить на него
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📅 *Введите начальную дату в формате ДД.ММ.ГГГГ:*",
        parse_mode='Markdown'
    )


async def customers_custom_period(update: Update, start_date_display: str, end_date_display: str):
    """Детальная статистика по покупателям за произвольный период"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Преобразуем даты из формата ДД.ММ.ГГГГ в формат для API
        start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
        end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')

        start_date = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date_obj.strftime('%Y-%m-%d 23:59:59')

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        # Рассчитываем длительность периода
        days_count = (end_date_obj - start_date_obj).days + 1

        message = f"""
👥 *Статистика по покупателям за произвольный период*

📅 Период: {start_date_display} - {end_date_display}
⏱️ Длительность: {days_count} дней

📊 *Общая статистика по заказам:*
• Количество заказов: *{stats['customer_orders']['count']}*
• Общая сумма: *{stats['customer_orders']['total']:,.2f} ₽*
• Средний чек: *{stats['customer_orders']['avg_order']:,.2f} ₽*
• Уникальных покупателей: *{stats['customer_count']}*

🏪 *Розничные продажи за период:*
• Количество продаж: *{stats['retail']['count']}*
• Общая сумма: *{stats['retail']['total']:,.2f} ₽*
"""

        # Добавляем анализ покупателей только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""
👤 *Анализ покупателей:*
• Новые покупатели (1 заказ): *{stats['new_customers']}*
• Постоянные покупатели (>1 заказа): *{stats['returning_customers']}*
• Соотношение новых/постоянных: *{calculate_ratio(stats['new_customers'], stats['returning_customers'])}*
"""
        else:
            message += f"""
👤 *Анализ покупателей:*
• Заказов покупателей нет - статистика недоступна
"""

        # Новые покупатели (только если есть)
        if stats['new_customers'] > 0:
            message += f"\n🆕 *Новые покупатели ({stats['new_customers']}):*\n"
            for i, customer in enumerate(stats['new_customers_list'][:5], 1):
                name = customer['name']
                phone = customer['phone']
                phone_info = f" 📞 {phone}" if phone != 'Не указан' else ""
                message += f"{i}. *{name}* - {customer['total']:,.2f} ₽{phone_info}\n"

            if stats['new_customers'] > 5:
                message += f"... и ещё {stats['new_customers'] - 5} покупателей\n"

        # Постоянные покупатели (только если есть)
        if stats['returning_customers'] > 0:
            message += f"\n🎯 *Постоянные покупатели ({stats['returning_customers']}):*\n"
            for i, customer in enumerate(stats['returning_customers_list'][:5], 1):
                name = customer['name']
                orders = customer['orders']
                phone = customer['phone']
                phone_info = f" 📞 {phone}" if phone != 'Не указан' else ""
                orders_text = "заказ" if orders == 1 else "заказа"
                message += f"{i}. *{name}* - {orders} {orders_text}, {customer['total']:,.2f} ₽{phone_info}\n"

            if stats['returning_customers'] > 5:
                message += f"... и ещё {stats['returning_customers'] - 5} покупателей\n"

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за период",
                                     callback_data=f'period_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton(f"🏆 Топ покупателей",
                                     callback_data=f'top_custom_{start_date_display}_{end_date_display}')
            ],
            [
                InlineKeyboardButton(f"💰 Платежи за период",
                                     callback_data=f'payments_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [
                InlineKeyboardButton("📅 Новый период", callback_data='period_menu'),
                InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в customers_custom_period: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики покупателей за период {start_date_display} - {end_date_display}: {str(e)}"
        await update.edit_message_text(error_msg)


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика за сегодня"""
    await send_statistics(update, 'today', 'сегодня')


async def week_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика за неделю"""
    await send_statistics(update, 'week', 'неделю')


async def month_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика за месяц"""
    await send_statistics(update, 'month', 'месяц')


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ покупателей за месяц"""
    await send_top_customers(update, 'month', 'месяц')


async def payments_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Входящие платежи - меню"""
    await payments_menu(update, context)


async def daily_summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Итоги дня"""
    await send_daily_summary(update)


async def payments_today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Входящие платежи за сегодня"""
    await send_incoming_payments(update, 'today', 'сегодня')


async def payments_week_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Входящие платежи за неделю"""
    await send_incoming_payments(update, 'week', 'неделю')


async def payments_month_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Входящие платежи за месяц"""
    await send_incoming_payments(update, 'month', 'месяц')


def get_period_dates(period: str) -> Tuple[str, str]:
    """Возвращает даты начала и конца периода"""
    now = datetime.now()

    if period == 'today':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif period == 'week':
        start_date = now - timedelta(days=now.weekday())
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif period == 'month':
        start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    else:
        # Последние 30 дней
        start_date = now - timedelta(days=30)
        end_date = now

    return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')


async def send_daily_summary(update: Update = None, context: ContextTypes.DEFAULT_TYPE = None, chat_id: int = None):
    """Отправляет ежедневную сводку"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Получаем сводку за сегодня
        summary = client.get_daily_summary()

        # Добавляем уникальную временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        # Формируем сообщение
        message = f"""
📊 *ИТОГИ ДНЯ — {summary['date']}*

🕐 *Время формирования:* {datetime.now().strftime('%H:%M')}

🛒 *ЗАКАЗЫ ПОКУПАТЕЛЕЙ:*
• Количество заказов: *{summary['customer_orders']['count']}*
• Общая сумма: *{summary['customer_orders']['total']:,.2f} ₽*
• Средний чек: *{summary['customer_orders']['avg_order']:,.2f} ₽*
• Уникальных покупателей: *{summary['unique_customers']}*

🏪 *РОЗНИЧНЫЕ ПРОДАЖИ:*
• Количество продаж: *{summary['retail']['count']}*
• Общая сумма: *{summary['retail']['total']:,.2f} ₽*
• Средний чек: *{summary['retail']['avg_order']:,.2f} ₽*

📈 *ОБЩАЯ СТАТИСТИКА ПРОДАЖ:*
• Всего продаж: *{summary['total_sales']['count']}*
• Общая сумма: *{summary['total_sales']['total']:,.2f} ₽*
• Средний чек: *{summary['total_sales']['avg_order']:,.2f} ₽*

💰 *ПЛАТЕЖИ:*
• Количество платежей: *{summary['payments']['count']}*
• Общая сумма: *{summary['payments']['total']:,.2f} ₽*
• Средний платеж: *{summary['payments']['avg_payment']:,.2f} ₽*
• Уникальных плательщиков: *{summary['unique_payers']}*
"""

        # Добавляем топ 3 покупателя (по заказам)
        if summary['top_customers']:
            message += f"\n🏆 *ТОП-3 ПОКУПАТЕЛЯ ДНЯ (по заказам):*\n"
            for i, customer in enumerate(summary['top_customers'], 1):
                phone_info = f" 📞 {customer['phone']}" if customer['phone'] != 'Не указан' else ""
                orders_text = "заказ" if customer['orders'] == 1 else "заказа"
                message += f"{i}. *{customer['name']}*{phone_info}\n"
                message += f"   💰 *{customer['total']:,.2f} ₽* ({customer['orders']} {orders_text})\n"
        else:
            message += "\n📭 *Заказов покупателей за сегодня нет*\n"

        # Добавляем топ 3 плательщика
        if summary['top_payers']:
            message += f"\n💰 *ТОП-3 ПЛАТЕЛЬЩИКА ДНЯ:*\n"
            for i, payer in enumerate(summary['top_payers'], 1):
                phone_info = f" 📞 {payer['phone']}" if payer['phone'] != 'Не указан' else ""
                payments_text = "платеж" if payer['payments'] == 1 else "платежа"
                message += f"{i}. *{payer['name']}*{phone_info}\n"
                message += f"   💸 *{payer['total']:,.2f} ₽* ({payer['payments']} {payments_text})\n"
        else:
            message += "\n📭 *Платежей за сегодня нет*\n"

        # Добавляем общую статистику
        total_revenue = summary['total_sales']['total'] + summary['payments']['total']
        message += f"\n💵 *ОБЩАЯ ВЫРУЧКА ДНЯ:* *{total_revenue:,.2f} ₽*\n"

        # Рассчитываем эффективность
        if summary['total_sales']['count'] > 0 and summary['total_sales']['total'] > 0:
            efficiency = (summary['payments']['total'] / summary['total_sales']['total'] * 100)
            message += f"📈 *Конверсия платежей:* {efficiency:.1f}%\n"

        # Добавляем временную метку
        message += f"\n⏰ *Обновлено:* {timestamp}"
        message += f"\n*Следующий отчет:* завтра в 23:00"

        # Создаем клавиатуру
        keyboard = [
            [
                InlineKeyboardButton("📊 Подробная статистика", callback_data='today'),
                InlineKeyboardButton("💰 Платежи сегодня", callback_data='payments_today')
            ],
            [
                InlineKeyboardButton("🔄 Обновить", callback_data='daily_summary'),
                InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Отправляем сообщение
        if update and not chat_id:
            # Команда от пользователя
            if isinstance(update, Update) and update.message:
                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            else:
                # Callback query
                try:
                    await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
                except Exception as edit_error:
                    if "Message is not modified" in str(edit_error):
                        logger.info("Сообщение не изменилось при обновлении - игнорируем")
                        await update.callback_query.answer("✅ Данные актуальны")
                    else:
                        raise edit_error
        elif chat_id:
            # Автоматическая отправка по расписанию
            if context and context.bot:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            else:
                logger.error("Контекст или бот не доступны для отправки по расписанию")
        else:
            logger.error("Не указан получатель для отправки сводки")

    except Exception as e:
        logger.error(f"Ошибка в send_daily_summary: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при формировании итогов дня: {str(e)}"

        if update and not chat_id:
            if isinstance(update, Update) and update.message:
                await update.message.reply_text(error_msg)
            else:
                try:
                    await update.edit_message_text(error_msg)
                except Exception:
                    await update.callback_query.message.reply_text(error_msg)
        elif chat_id and context and context.bot:
            await context.bot.send_message(chat_id=chat_id, text=error_msg)


async def send_customers_details(update: Update, period: str, period_name: str):
    """Отправляет детальную статистику по покупателям (по заказам)"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        message = f"""
👥 *Статистика по покупателям (по заказам) за {period_name}*

📅 Период: {start_date.split()[0]} - {end_date.split()[0]}

📊 *Общая статистика по заказам:*
• Количество заказов: *{stats['customer_orders']['count']}*
• Общая сумма: *{stats['customer_orders']['total']:,.2f} ₽*
• Средний чек: *{stats['customer_orders']['avg_order']:,.2f} ₽*
• Уникальных покупателей: *{stats['customer_count']}*

🏪 *Розничные продажи за {period_name}:*
• Количество продаж: *{stats['retail']['count']}*
• Общая сумма: *{stats['retail']['total']:,.2f} ₽*
"""

        # Добавляем анализ покупателей только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""
👤 *Анализ покупателей:*
• Новые покупатели (1 заказ): *{stats['new_customers']}*
• Постоянные покупатели (>1 заказа): *{stats['returning_customers']}*
• Соотношение новых/постоянных: *{calculate_ratio(stats['new_customers'], stats['returning_customers'])}*
"""
        else:
            message += f"""
👤 *Анализ покупателей:*
• Заказов покупателей нет - статистика недоступна
"""

        # Новые покупатели (только если есть)
        if stats['new_customers'] > 0:
            message += f"\n🆕 *Новые покупатели ({stats['new_customers']}):*\n"
            for i, customer in enumerate(stats['new_customers_list'][:5], 1):
                name = customer['name']
                phone = customer['phone']
                phone_info = f" 📞 {phone}" if phone != 'Не указан' else ""
                message += f"{i}. *{name}* - {customer['total']:,.2f} ₽{phone_info}\n"

            if stats['new_customers'] > 5:
                message += f"... и ещё {stats['new_customers'] - 5} покупателей\n"

        # Постоянные покупатели (только если есть)
        if stats['returning_customers'] > 0:
            message += f"\n🎯 *Постоянные покупатели ({stats['returning_customers']}):*\n"
            for i, customer in enumerate(stats['returning_customers_list'][:5], 1):
                name = customer['name']
                orders = customer['orders']
                phone = customer['phone']
                phone_info = f" 📞 {phone}" if phone != 'Не указан' else ""
                orders_text = "заказ" if orders == 1 else "заказа"
                message += f"{i}. *{name}* - {orders} {orders_text}, {customer['total']:,.2f} ₽{phone_info}\n"

            if stats['returning_customers'] > 5:
                message += f"... и ещё {stats['returning_customers'] - 5} покупателей\n"

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за {period_name}", callback_data=period),
                InlineKeyboardButton(f"🏆 Топ покупателей", callback_data=f'top_{period}')
            ],
            [
                InlineKeyboardButton(f"💰 Платежи за {period_name}", callback_data=f'payments_{period}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [InlineKeyboardButton("🔙 Меню покупателей", callback_data='customers_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            try:
                await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            except Exception as edit_error:
                # Если сообщение не изменилось, просто игнорируем ошибку
                if "Message is not modified" in str(edit_error):
                    logger.info("Сообщение не изменилось при обновлении - игнорируем")
                    await update.callback_query.answer("✅ Данные актуальны")
                else:
                    raise edit_error

    except Exception as e:
        logger.error(f"Ошибка в send_customers_details: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики покупателей за {period_name}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            try:
                await update.edit_message_text(error_msg)
            except Exception:
                await update.callback_query.message.reply_text(error_msg)


async def send_statistics(update: Update, period: str, period_name: str):
    """Отправляет статистику за период с разделением на заказы покупателей и розницу"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        message = f"""
📊 *Статистика продаж за {period_name}*

📅 Период: {start_date.split()[0]} - {end_date.split()[0]}

🛒 *ЗАКАЗЫ ПОКУПАТЕЛЕЙ:*
• Количество заказов: *{stats['customer_orders']['count']}*
• Общая сумма: *{stats['customer_orders']['total']:,.2f} ₽*
• Средний чек: *{stats['customer_orders']['avg_order']:,.2f} ₽*
• Уникальных покупателей: *{stats['customer_count']}*

🏪 *РОЗНИЧНЫЕ ПРОДАЖИ:*
• Количество продаж: *{stats['retail']['count']}*
• Общая сумма: *{stats['retail']['total']:,.2f} ₽*
• Средний чек: *{stats['retail']['avg_order']:,.2f} ₽*

📈 *ОБЩАЯ СТАТИСТИКА ПРОДАЖ:*
• Всего продаж: *{stats['total_sales']['count']}*
• Общая сумма: *{stats['total_sales']['total']:,.2f} ₽*
• Средний чек: *{stats['total_sales']['avg_order']:,.2f} ₽*
"""

        # Добавляем анализ покупателей только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""
👤 *Анализ покупателей (по заказам):*
• Новые покупатели (1 заказ): *{stats['new_customers']}*
• Постоянные покупатели (>1 заказа): *{stats['returning_customers']}*
• Соотношение новых/постоянных: *{calculate_ratio(stats['new_customers'], stats['returning_customers'])}*
"""
        else:
            message += f"""
👤 *Анализ покупателей:*
• Заказов покупателей нет - статистика недоступна
"""

        # Добавляем кнопки для навигации
        keyboard = [
            [
                InlineKeyboardButton("👥 Подробнее о покупателях", callback_data=f'customers_{period}'),
                InlineKeyboardButton("🏆 Топ покупателей", callback_data=f'top_{period}')
            ],
            [
                InlineKeyboardButton("💰 Входящие платежи", callback_data=f'payments_{period}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            try:
                await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            except Exception as edit_error:
                if "Message is not modified" in str(edit_error):
                    logger.info("Сообщение не изменилось при обновлении - игнорируем")
                    await update.callback_query.answer("✅ Данные актуальны")
                else:
                    raise edit_error

    except Exception as e:
        logger.error(f"Ошибка в send_statistics: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики за {period_name}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            try:
                await update.edit_message_text(error_msg)
            except Exception:
                await update.callback_query.message.reply_text(error_msg)


async def send_incoming_payments(update: Update, period: str, period_name: str):
    """Отправляет статистику по входящим платежам за период"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Получаем статистику по платежам
        stats = client.get_incoming_payments_stats(start_date, end_date)

        # Рассчитываем средний платеж
        avg_payment = stats['total_amount'] / stats['total_payments'] if stats['total_payments'] > 0 else Decimal('0')

        message = f"""
💰 *Входящие платежи за {period_name}*

📅 Период: {start_date.split()[0]} - {end_date.split()[0]}

📈 *Основные показатели:*
• Количество платежей: *{stats['total_payments']}*
• Общая сумма: *{stats['total_amount']:,.2f} ₽*
• Средний платеж: *{avg_payment:,.2f} ₽*
• Уникальных плательщиков: *{stats['customer_count']}*
"""

        # Добавляем статистику по типам платежей
        if stats['payment_types']:
            message += f"\n💳 *Статистика по типам платежей:*\n"
            for i, pt in enumerate(stats['payment_types'][:5], 1):
                message += f"{i}. *{pt['type']}*: {pt['total']:,.2f} ₽ ({pt['count']} платежей)\n"

        # Добавляем топ плательщиков
        if stats['top_payers']:
            message += f"\n🏆 *Топ-5 плательщиков:*\n\n"
            for i, payer in enumerate(stats['top_payers'][:5], 1):
                phone_info = f" 📞 {payer['phone']}" if payer['phone'] != 'Не указан' else ""
                message += f"{i}. *{payer['name']}*{phone_info}\n"
                message += f"   💰 *{payer['total']:,.2f} ₽* ({payer['payments']} платежей)\n\n"

        # Добавляем последние платежи
        if stats['payments_data']:
            message += f"\n🕒 *Последние платежи:*\n"
            for i, payment in enumerate(stats['payments_data'][:3], 1):
                agent_name = payment['agent']['name'] if payment['agent'] else 'Без имени'

                # Получаем дату и время в формате ДД.ММ ЧЧ:ММ
                if payment['moment']:
                    if 'T' in payment['moment']:
                        # Формат: "2024-01-06T14:30:00.000"
                        # Разделяем дату и время
                        date_part, time_part = payment['moment'].split('T')
                        # Разделяем год, месяц, день
                        year, month, day = date_part.split('-')
                        # Берем только часы и минуты
                        time_hhmm = time_part[:5]
                        # Форматируем: день.месяц часы:минуты
                        payment_datetime = f"{day}.{month} {time_hhmm}"
                    else:
                        # Формат: "2024-01-06 14:30:00"
                        # Просто берем нужные символы
                        day = payment['moment'][8:10]  # "06"
                        month = payment['moment'][5:7]  # "01"
                        time_hhmm = payment['moment'][11:16]  # "14:30"
                        payment_datetime = f"{day}.{month} {time_hhmm}"
                else:
                    payment_datetime = "--.-- --:--"

                message += f"{i}. {agent_name}: *{payment['sum']:,.2f} ₽*\n {payment_datetime}\n\n"

        # Добавляем кнопки для навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за {period_name}", callback_data=period),
                InlineKeyboardButton(f"🏆 Топ плательщиков", callback_data=f'payments_top_{period}')
            ],
            [
                InlineKeyboardButton("📅 Другие периоды", callback_data='payments_menu'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_incoming_payments: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики платежей за {period_name}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            await update.edit_message_text(error_msg)


async def send_top_customers(update: Update, period: str, period_name: str):
    """Отправляет топ покупателей по заказам за период"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        message = f"""
🏆 *Топ покупателей по заказам за {period_name}*

📅 Период: {start_date.split()[0]} - {end_date.split()[0]}
"""

        if stats['top_customers']:
            message += f"\n📊 *Топ-10 покупателей по сумме заказов:*\n"
            for i, customer in enumerate(stats['top_customers'], 1):
                orders_text = "заказ" if customer['orders'] == 1 else "заказа"
                phone_info = f" 📞 {customer['phone']}" if customer['phone'] != 'Не указан' else ""
                message += f"\n{i}. *{customer['name']}*{phone_info}\n"
                message += f"   💰 *{customer['total']:,.2f} ₽* ({customer['orders']} {orders_text})\n"
        else:
            message += "\n📭 *Заказов покупателей не найдено за выбранный период*\n"

        # Общая статистика
        message += f"""

📈 *Общая статистика за {period_name}:*
• Заказы покупателей: *{stats['customer_orders']['total']:,.2f} ₽* ({stats['customer_orders']['count']} заказов)
• Розничные продажи: *{stats['retail']['total']:,.2f} ₽* ({stats['retail']['count']} продаж)
• Всего продаж: *{stats['total_sales']['total']:,.2f} ₽* ({stats['total_sales']['count']} шт.)
"""

        # Добавляем статистику по покупателям только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""• Уникальных покупателей (по заказам): *{stats['customer_count']}*
• Новые покупатели: *{stats['new_customers']}*
• Постоянные покупатели: *{stats['returning_customers']}*
"""

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за {period_name}", callback_data=period),
                InlineKeyboardButton(f"👥 Все покупатели", callback_data=f'customers_{period}')
            ],
            [
                InlineKeyboardButton(f"💰 Платежи за {period_name}", callback_data=f'payments_{period}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            try:
                await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            except Exception as edit_error:
                if "Message is not modified" in str(edit_error):
                    logger.info("Сообщение не изменилось при обновлении - игнорируем")
                    await update.callback_query.answer("✅ Данные актуальны")
                else:
                    raise edit_error

    except Exception as e:
        logger.error(f"Ошибка в send_top_customers: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении топа покупателей за {period_name}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            try:
                await update.edit_message_text(error_msg)
            except Exception:
                await update.callback_query.message.reply_text(error_msg)


async def customers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню статистики покупателей"""
    keyboard = [
        [
            InlineKeyboardButton("👥 Сегодня", callback_data='customers_today'),
            InlineKeyboardButton("👥 Неделя", callback_data='customers_week')
        ],
        [
            InlineKeyboardButton("👥 Месяц", callback_data='customers_month'),
            InlineKeyboardButton("🏆 Топ месяца", callback_data='top_month')
        ],
        [
            InlineKeyboardButton("💰 Платежи", callback_data='payments_menu'),
            InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
        ],
        [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "📊 *Статистика покупателей*\n\nВыберите период:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        await update.edit_message_text(
            "📊 *Статистика покупателей*\n\nВыберите период:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def payments_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню входящих платежей"""
    keyboard = [
        [
            InlineKeyboardButton("💰 Сегодня", callback_data='payments_today'),
            InlineKeyboardButton("💰 Неделя", callback_data='payments_week')
        ],
        [
            InlineKeyboardButton("💰 Месяц", callback_data='payments_month'),
            InlineKeyboardButton("🏆 Топ плательщиков", callback_data='payments_top_month')
        ],
        [
            InlineKeyboardButton("📊 Продажи", callback_data='customers_menu'),
            InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
        ],
        [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "💰 *Входящие платежи*\n\nВыберите период:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        await update.edit_message_text(
            "💰 *Входящие платежи*\n\nВыберите период:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


def calculate_ratio(new: int, returning: int) -> str:
    """Рассчитывает соотношение новых и постоянных покупателей"""
    total = new + returning
    if total == 0:
        return "0% / 0%"

    new_percent = (new / total) * 100
    returning_percent = (returning / total) * 100

    return f"{new_percent:.1f}% / {returning_percent:.1f}%"


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для отладки"""
    try:
        # Получаем даты за сегодня
        start_date, end_date = get_period_dates('today')

        # Получаем сырые данные
        filter_params = {
            'filter': f'moment>={start_date}',
            'limit': 3,
            'expand': 'agent'
        }

        response = requests.get(
            f"{MOYSKLAD_BASE_URL}/entity/demand",
            headers=HEADERS,
            params=filter_params,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            debug_text = "🔍 *Отладочная информация - прямые данные API*\n\n"

            if 'rows' in data and data['rows']:
                for i, row in enumerate(data['rows'], 1):
                    debug_text += f"*Запись #{i}:*\n"
                    debug_text += f"ID: `{row.get('id', 'Нет')}`\n"
                    debug_text += f"Сумма: {row.get('sum', 0) / 100:.2f} руб\n"
                    debug_text += f"Дата: {row.get('moment', 'Нет')}\n"

                    if 'agent' in row and row['agent']:
                        agent = row['agent']
                        debug_text += "*Контрагент:*\n"
                        debug_text += f"  • ID: `{agent.get('meta', {}).get('href', '').split('/')[-1]}`\n"
                        debug_text += f"  • Имя: `{agent.get('name', 'НЕТ')}`\n"
                        debug_text += f"  • Телефон: `{agent.get('phone', 'НЕТ')}`\n"
                        debug_text += f"  • Email: `{agent.get('email', 'НЕТ')}`\n"
                    else:
                        debug_text += "Контрагент: ❌ Нет данных\n"

                    debug_text += "\n" + "─" * 30 + "\n\n"
            else:
                debug_text += "📭 Нет данных за сегодня\n"

            await update.message.reply_text(debug_text, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"❌ Ошибка API: {response.status_code}")

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Справка"""
    help_text = """
    📚 *Справка по боту*

    *Основные команды:*
    /start - Главное меню с кнопками
    /today - Статистика за сегодня
    /week - Статистика за неделю
    /month - Статистика за месяц
    /top - Топ покупателей за месяц
    /customers - Меню статистики покупателей
    /payments - Входящие платежи
    /daily - Итоги дня
    /debug - Отладка API
    /help - Эта справка

    *Что показывается:*
    📊 *Статистика продаж:*
    • Количество и сумма продаж
    • Средний чек
    • Уникальные покупатели
    • Новые/постоянные покупатели

    💰 *Входящие платежи:*
    • Количество и сумма платежей
    • Средний платеж
    • Статистика по типам платежей
    • Топ плательщиков
    • Последние платежи

    🏆 *Топ покупателей:*
    • Топ-10 по сумме покупок
    • Количество заказов
    • Контактные данные

    📊 *Итоги дня (в 23:00):*
    • Продажи за день
    • Платежи за день
    • Топ-3 покупателя
    • Топ-3 плательщика
    • Общая выручка

    👥 *Детали по покупателям:*
    • Списки новых и постоянных
    • Телефоны и email
    • Анализ соотношения
    """

    await update.message.reply_text(help_text, parse_mode='Markdown')


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий кнопок"""
    query = update.callback_query

    # 1. Немедленно отвечаем на callback
    try:
        await query.answer()
    except Exception as e:
        error_msg = str(e)
        if "Query is too old" in error_msg or "response timeout expired" in error_msg:
            logger.warning(f"Callback query устарел: {query.data}")
            return  # Просто выходим, если запрос устарел
        logger.warning(f"Ошибка при answer на callback: {e}")

    # 2. Обрабатываем callback_data с обработкой ошибок
    try:
        if query.data == 'main_menu':
            await start_from_callback(query)

        elif query.data == 'period_menu':
            await period_menu_handler(query, context)

        elif query.data == 'enter_period':
            await enter_period_handler(query, context)

        elif query.data == 'quick_periods':
            await quick_periods_handler(query, context)

        elif query.data.startswith('quick_period_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]

                start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
                end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')

                start_date = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                end_date = end_date_obj.strftime('%Y-%m-%d 23:59:59')

                # Показываем сообщение о загрузке
                await query.edit_message_text(
                    f"⏳ *Загружаю статистику за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown'
                )

                await send_period_statistics(query, start_date, end_date, start_date_display, end_date_display)

        elif query.data.startswith('period_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]

                start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
                end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')

                start_date = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                end_date = end_date_obj.strftime('%Y-%m-%d 23:59:59')

                # Показываем сообщение о загрузке
                await query.edit_message_text(
                    f"⏳ *Загружаю статистику за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown'
                )

                await send_period_statistics(query, start_date, end_date, start_date_display, end_date_display)

        elif query.data == 'customers_menu':
            await customers_menu(query, context)

        elif query.data == 'payments_menu':
            await payments_menu(query, context)

        elif query.data == 'daily_summary':
            await send_daily_summary(query)

        elif query.data == 'today':
            # Показываем сообщение о загрузке
            await query.edit_message_text("⏳ *Загружаю статистику за сегодня...*", parse_mode='Markdown')
            await send_statistics(query, 'today', 'сегодня')

        elif query.data == 'week':
            await query.edit_message_text("⏳ *Загружаю статистику за неделю...*", parse_mode='Markdown')
            await send_statistics(query, 'week', 'неделю')

        elif query.data == 'month':
            await query.edit_message_text("⏳ *Загружаю статистику за месяц...*", parse_mode='Markdown')
            await send_statistics(query, 'month', 'месяц')

        elif query.data == 'top':
            await query.edit_message_text("⏳ *Загружаю топ покупателей...*", parse_mode='Markdown')
            await send_top_customers(query, 'month', 'месяц')

        elif query.data == 'top_month':
            await query.edit_message_text("⏳ *Загружаю топ покупателей за месяц...*", parse_mode='Markdown')
            await send_top_customers(query, 'month', 'месяц')

        elif query.data == 'payments_today':
            await query.edit_message_text("⏳ *Загружаю платежи за сегодня...*", parse_mode='Markdown')
            await send_incoming_payments(query, 'today', 'сегодня')

        elif query.data == 'payments_week':
            await query.edit_message_text("⏳ *Загружаю платежи за неделю...*", parse_mode='Markdown')
            await send_incoming_payments(query, 'week', 'неделю')

        elif query.data == 'payments_month':
            await query.edit_message_text("⏳ *Загружаю платежи за месяц...*", parse_mode='Markdown')
            await send_incoming_payments(query, 'month', 'месяц')

        elif query.data.startswith('payments_'):
            if query.data == 'payments_menu':
                await payments_menu(query, context)
            elif query.data.startswith('payments_top_'):
                period = query.data.split('_')[2] if len(query.data.split('_')) > 2 else 'month'
                period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
                await query.edit_message_text(f"⏳ *Загружаю топ плательщиков за {period_name}...*",
                                              parse_mode='Markdown')
                await send_incoming_payments(query, period, period_name)
            else:
                period = query.data.split('_')[1] if len(query.data.split('_')) > 1 else 'today'
                period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
                await query.edit_message_text(f"⏳ *Загружаю платежи за {period_name}...*", parse_mode='Markdown')
                await send_incoming_payments(query, period, period_name)

        elif query.data == 'customers_today':
            await query.edit_message_text("⏳ *Загружаю статистику покупателей за сегодня...*", parse_mode='Markdown')
            await send_customers_details(query, 'today', 'сегодня')

        elif query.data == 'customers_week':
            await query.edit_message_text("⏳ *Загружаю статистику покупателей за неделю...*", parse_mode='Markdown')
            await send_customers_details(query, 'week', 'неделю')

        elif query.data == 'customers_month':
            await query.edit_message_text("⏳ *Загружаю статистику покупателей за месяц...*", parse_mode='Markdown')
            await send_customers_details(query, 'month', 'месяц')

        elif query.data.startswith('customers_'):
            period = query.data.split('_')[1]
            period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
            await query.edit_message_text(f"⏳ *Загружаю статистику покупателей за {period_name}...*",
                                          parse_mode='Markdown')
            await send_customers_details(query, period, period_name)

        elif query.data.startswith('top_'):
            period = query.data.split('_')[1]
            period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
            await query.edit_message_text(f"⏳ *Загружаю топ покупателей за {period_name}...*", parse_mode='Markdown')
            await send_top_customers(query, period, period_name)

        elif query.data.startswith('customers_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(
                    f"⏳ *Загружаю детали по покупателям за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown')
                await customers_custom_period(query, start_date_display, end_date_display)

        elif query.data.startswith('top_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(
                    f"⏳ *Загружаю топ покупателей за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown')
                await send_top_customers_custom(query, start_date_display, end_date_display)

        elif query.data.startswith('payments_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(f"⏳ *Загружаю платежи за {start_date_display} - {end_date_display}...*",
                                              parse_mode='Markdown')
                await send_payments_custom_period(query, start_date_display, end_date_display)

        else:
            # Неизвестный callback_data
            logger.warning(f"Неизвестный callback_data: {query.data}")
            await query.message.reply_text("❌ Неизвестная команда. Используйте меню.")

    except Exception as e:
        logger.error(f"Ошибка в обработке кнопки {query.data}: {e}", exc_info=True)
        try:
            # Пробуем отредактировать сообщение об ошибке
            await query.edit_message_text(
                f"❌ *Ошибка при обработке запроса*\n\n"
                f"Ошибка: {str(e)[:200]}\n\n"
                f"Попробуйте снова или выберите другую команду.",
                parse_mode='Markdown'
            )
        except Exception as edit_error:
            # Если не удалось отредактировать, отправляем новое сообщение
            try:
                await query.message.reply_text(
                    f"❌ Ошибка при обработке запроса: {str(e)[:100]}"
                )
            except Exception:
                pass  # Если ничего не работает, просто логируем ошибку


async def send_statistics_from_query(query, period: str, period_name: str):
    """Отправка статистики из callback query"""

    # Создаем фиктивный update для совместимости
    class MockUpdate:
        def __init__(self, query):
            self.callback_query = query

    mock_update = MockUpdate(query)
    await send_statistics(mock_update, period, period_name)


async def send_top_customers_custom(update: Update, start_date_display: str, end_date_display: str):
    """Отправляет топ покупателей за произвольный период"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Преобразуем даты
        start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
        end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')

        start_date = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date_obj.strftime('%Y-%m-%d 23:59:59')

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Добавляем временную метку
        timestamp = datetime.now().strftime('%H:%M:%S')

        # Рассчитываем длительность периода
        days_count = (end_date_obj - start_date_obj).days + 1

        message = f"""
🏆 *Топ покупателей за произвольный период*

📅 Период: {start_date_display} - {end_date_display}
⏱️ Длительность: {days_count} дней
"""

        if stats['top_customers']:
            message += f"\n📊 *Топ-10 покупателей по сумме заказов:*\n"
            for i, customer in enumerate(stats['top_customers'], 1):
                orders_text = "заказ" if customer['orders'] == 1 else "заказа"
                phone_info = f" 📞 {customer['phone']}" if customer['phone'] != 'Не указан' else ""
                message += f"\n{i}. *{customer['name']}*{phone_info}\n"
                message += f"   💰 *{customer['total']:,.2f} ₽* ({customer['orders']} {orders_text})\n"
        else:
            message += "\n📭 *Заказов покупателей не найдено за выбранный период*\n"

        # Общая статистика
        message += f"""

📈 *Общая статистика за период:*
• Заказы покупателей: *{stats['customer_orders']['total']:,.2f} ₽* ({stats['customer_orders']['count']} заказов)
• Розничные продажи: *{stats['retail']['total']:,.2f} ₽* ({stats['retail']['count']} продаж)
• Всего продаж: *{stats['total_sales']['total']:,.2f} ₽* ({stats['total_sales']['count']} шт.)
"""

        # Добавляем статистику по покупателям только если есть заказы
        if stats['customer_count'] > 0:
            message += f"""• Уникальных покупателей (по заказам): *{stats['customer_count']}*
• Новые покупатели: *{stats['new_customers']}*
• Постоянные покупатели: *{stats['returning_customers']}*
"""

        # Средние показатели в день
        if days_count > 0:
            avg_orders_per_day = stats['customer_orders']['count'] / days_count
            avg_amount_per_day = stats['customer_orders']['total'] / days_count

            message += f"""
📊 *Средние показатели в день:*
• Заказов в день: *{avg_orders_per_day:.1f}*
• Сумма в день: *{avg_amount_per_day:,.2f} ₽*
"""

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за период",
                                     callback_data=f'period_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton(f"👥 Все покупатели",
                                     callback_data=f'customers_custom_{start_date_display}_{end_date_display}')
            ],
            [
                InlineKeyboardButton(f"💰 Платежи за период",
                                     callback_data=f'payments_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [
                InlineKeyboardButton("📅 Новый период", callback_data='period_menu'),
                InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_top_customers_custom: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении топа покупателей за период {start_date_display} - {end_date_display}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            await update.edit_message_text(error_msg)


async def send_payments_custom_period(update: Update, start_date_display: str, end_date_display: str):
    """Отправляет статистику по платежам за произвольный период"""
    try:
        # Создаем клиент
        client = DebugMoySkladClient()

        # Преобразуем даты
        start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
        end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')

        start_date = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date_obj.strftime('%Y-%m-%d 23:59:59')

        # Получаем статистику по платежам
        stats = client.get_incoming_payments_stats(start_date, end_date)

        # Рассчитываем средний платеж
        avg_payment = stats['total_amount'] / stats['total_payments'] if stats['total_payments'] > 0 else Decimal('0')

        # Рассчитываем длительность периода
        days_count = (end_date_obj - start_date_obj).days + 1

        message = f"""
💰 *Входящие платежи за произвольный период*

📅 Период: {start_date_display} - {end_date_display}
⏱️ Длительность: {days_count} дней

📈 *Основные показатели:*
• Количество платежей: *{stats['total_payments']}*
• Общая сумма: *{stats['total_amount']:,.2f} ₽*
• Средний платеж: *{avg_payment:,.2f} ₽*
• Уникальных плательщиков: *{stats['customer_count']}*
"""

        # Средние показатели в день
        if days_count > 0:
            avg_payments_per_day = stats['total_payments'] / days_count
            avg_amount_per_day = stats['total_amount'] / days_count

            message += f"""
📊 *Средние показатели в день:*
• Платежей в день: *{avg_payments_per_day:.1f}*
• Сумма в день: *{avg_amount_per_day:,.2f} ₽*
"""

        # Добавляем статистику по типам платежей
        if stats['payment_types']:
            message += f"\n💳 *Статистика по типам платежей:*\n"
            for i, pt in enumerate(stats['payment_types'][:5], 1):
                message += f"{i}. *{pt['type']}*: {pt['total']:,.2f} ₽ ({pt['count']} платежей)\n"

        # Добавляем топ плательщиков
        if stats['top_payers']:
            message += f"\n🏆 *Топ-5 плательщиков:*\n\n"
            for i, payer in enumerate(stats['top_payers'][:5], 1):
                phone_info = f" 📞 {payer['phone']}" if payer['phone'] != 'Не указан' else ""
                message += f"{i}. *{payer['name']}*{phone_info}\n"
                message += f"   💰 *{payer['total']:,.2f} ₽* ({payer['payments']} платежей)\n\n"

        # Кнопки навигации
        keyboard = [
            [
                InlineKeyboardButton(f"📊 Статистика за период",
                                     callback_data=f'period_custom_{start_date_display}_{end_date_display}'),
                InlineKeyboardButton(f"🏆 Топ плательщиков",
                                     callback_data=f'payments_custom_{start_date_display}_{end_date_display}')
            ],
            [
                InlineKeyboardButton("📅 Другие периоды", callback_data='payments_menu'),
                InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')
            ],
            [
                InlineKeyboardButton("📅 Новый период", callback_data='period_menu'),
                InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_payments_custom_period: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики платежей за период {start_date_display} - {end_date_display}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            await update.edit_message_text(error_msg)


async def start_from_callback(query):
    """Старт из callback"""
    welcome_text = """
    🤖 *Бот статистики МойСклад - Полная версия*

    📊 *Доступные команды:*
    /today - Статистика за сегодня
    /week - Статистика за неделю
    /month - Статистика за месяц
    /top - Топ покупателей за месяц
    /customers - Меню статистики покупателей
    /payments - Входящие платежи
    /daily - Итоги дня
    /help - Справка

    """

    keyboard = [
        [
            InlineKeyboardButton("📅 Сегодня", callback_data='today'),
            InlineKeyboardButton("📆 Неделя", callback_data='week')
        ],
        [
            InlineKeyboardButton("📈 Месяц", callback_data='month'),
            InlineKeyboardButton("🏆 Топ", callback_data='top')
        ],
        [
            InlineKeyboardButton("👥 Покупатели", callback_data='customers_menu'),
            InlineKeyboardButton("💰 Платежи", callback_data='payments_menu')
        ],
        [InlineKeyboardButton("📊 Итоги дня", callback_data='daily_summary')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Ошибка в боте: {context.error}")
    if update and update.message:
        await update.message.reply_text(f"❌ Произошла ошибка: {context.error}")


async def send_daily_report(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет ежедневный отчет в 20:00"""
    try:
        logger.info("⏰ Отправка ежедневного отчета в 20:00")

        # Получаем chat_id из данных контекста или используем ADMIN_CHAT_ID
        chat_id = None

        # Используем ADMIN_CHAT_ID из переменных окружения
        if ADMIN_CHAT_ID:
            try:
                chat_id = int(ADMIN_CHAT_ID)
            except ValueError:
                logger.error(f"Некорректный ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
                return

        if chat_id:
            await send_daily_summary(context=context, chat_id=chat_id)
        else:
            logger.warning("Не указан ADMIN_CHAT_ID для отправки ежедневного отчета. "
                           "Добавьте ADMIN_CHAT_ID в файл .env или используйте /setreport")

    except Exception as e:
        logger.error(f"Ошибка при отправке ежедневного отчета: {e}", exc_info=True)


async def setup_daily_report(application: Application):
    """Настраивает ежедневный отчет"""
    try:
        # Создаем job queue
        job_queue = application.job_queue

        if job_queue:
            # Добавляем задачу на отправку отчета в 20:00 каждый день
            job_queue.run_daily(
                send_daily_report,
                time=datetime.strptime("16:00", "%H:%M").time(),
                days=(0, 1, 2, 3, 4, 5, 6),
                name="daily_report_23_00"
            )
            logger.info("✅ Ежедневный отчет настроен на 20:00")

            # Также можно добавить тестовую отправку через 10 секунд после запуска
            # для проверки работы
            async def test_scheduler_callback(context: ContextTypes.DEFAULT_TYPE):
                logger.info("✅ Планировщик ежедневных отчетов запущен")

            job_queue.run_once(test_scheduler_callback, when=10)

    except Exception as e:
        logger.error(f"Ошибка при настройке ежедневного отчета: {e}", exc_info=True)


async def set_report_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Устанавливает chat_id для ежедневных отчетов (только для админа)"""
    try:
        chat_id = update.effective_chat.id

        # Сохраняем chat_id в переменные окружения или в контекст бота
        # Для простоты будем использовать файл .env или базу данных
        # В данном случае просто логируем

        logger.info(f"Установлен chat_id для ежедневных отчетов: {chat_id}")

        # Сохраняем в контекст бота
        if not hasattr(context.bot_data, 'report_chats'):
            context.bot_data['report_chats'] = []

        if chat_id not in context.bot_data['report_chats']:
            context.bot_data['report_chats'].append(chat_id)

        await update.message.reply_text(
            f"✅ Этот чат ({chat_id}) будет получать ежедневные отчеты в 23:00\n\n"
            f"Для проверки отправьте команду /daily чтобы увидеть текущие итоги дня."
        )

    except Exception as e:
        logger.error(f"Ошибка при установке chat_id: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при настройке отчетов")


def main():
    """Основная функция"""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ Не установлен TELEGRAM_BOT_TOKEN в .env файле")
        return

    if not MOYSKLAD_TOKEN:
        logger.error("❌ Не установлен MOYSKLAD_TOKEN в .env файле")
        return

    try:
        logger.info("=" * 50)
        logger.info("ЗАПУСК БОТА МОЙСКЛАД - С ПРОИЗВОЛЬНЫМ ПЕРИОДОМ")
        logger.info("=" * 50)

        # Создаем приложение с JobQueue
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Создаем ConversationHandler для ввода периода
        period_conversation_handler = ConversationHandler(
            entry_points=[
                CommandHandler("period", period_command),
                CallbackQueryHandler(period_command, pattern='^enter_period$')
            ],
            states={
                PERIOD_START_DATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_date),
                    CommandHandler("cancel", cancel_period)
                ],
                PERIOD_END_DATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_end_date),
                    CommandHandler("cancel", cancel_period)
                ]
            },
            fallbacks=[
                CommandHandler("cancel", cancel_period),
                CallbackQueryHandler(cancel_period, pattern='^main_menu$')
            ],
            allow_reentry=True
        )

        # Добавляем обработчики команд
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("today", today_command))
        application.add_handler(CommandHandler("week", week_command))
        application.add_handler(CommandHandler("month", month_command))
        application.add_handler(period_conversation_handler)  # <-- ВМЕСТО простого MessageHandler
        application.add_handler(CommandHandler("top", top_command))
        application.add_handler(CommandHandler("customers", customers_menu))
        application.add_handler(CommandHandler("payments", payments_command))
        application.add_handler(CommandHandler("daily", daily_summary_command))
        application.add_handler(CommandHandler("setreport", set_report_chat_command))
        application.add_handler(CommandHandler("debug", debug_command))
        application.add_handler(CommandHandler("help", help_command))

        # Добавляем обработчики для платежей
        application.add_handler(CommandHandler("payments_today", payments_today_command))
        application.add_handler(CommandHandler("payments_week", payments_week_command))
        application.add_handler(CommandHandler("payments_month", payments_month_command))

        # Обработчик кнопок
        application.add_handler(CallbackQueryHandler(button_handler))

        # Обработчик ошибок
        application.add_error_handler(error_handler)

        # Настраиваем ежедневные отчеты
        async def post_init(application: Application):
            await setup_daily_report(application)

        application.post_init = post_init

        # Запускаем бота
        logger.info("Бот запущен. Ожидание команд...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка запуска бота: {e}", exc_info=True)


if __name__ == '__main__':
    main()