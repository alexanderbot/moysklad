import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Tuple, List, Optional
from collections import defaultdict
import asyncio
import json

import requests
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters, JobQueue
)

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
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID')
MOYSKLAD_BASE_URL = 'https://api.moysklad.ru/api/remap/1.2'

# Определяем состояния для ConversationHandler
(
    PERIOD_START_DATE,
    PERIOD_END_DATE,
    TOKEN_INPUT
) = range(3)

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
    return user_data if user_data else None


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
        if value:
            tokens[user_id_str][key] = value

    tokens[user_id_str]['updated_at'] = datetime.now().isoformat()
    save_user_tokens(tokens)


def delete_user_token(user_id: int):
    """Удаление токена пользователя"""
    tokens = load_user_tokens()
    user_id_str = str(user_id)

    if user_id_str in tokens:
        if 'moysklad_token' in tokens[user_id_str]:
            del tokens[user_id_str]['moysklad_token']

        for key in ['organization_name', 'organization_inn', 'organization_email']:
            if key in tokens[user_id_str]:
                del tokens[user_id_str][key]

        save_user_tokens(tokens)


def update_user_activity(user_id: int, username: str = None,
                         first_name: str = None, last_name: str = None):
    """Обновление активности пользователя"""
    tokens = load_user_tokens()
    user_id_str = str(user_id)

    if user_id_str not in tokens:
        tokens[user_id_str] = {}

    if username:
        tokens[user_id_str]['username'] = username
    if first_name:
        tokens[user_id_str]['first_name'] = first_name
    if last_name:
        tokens[user_id_str]['last_name'] = last_name

    tokens[user_id_str]['last_activity'] = datetime.now().isoformat()
    save_user_tokens(tokens)


# ============================================================
# ПРОСТОЙ КЛИЕНТ МОЙСКЛАД (ЗАМЕНА DebugMoySkladClient)
# ============================================================

class SimpleMoySkladClient:
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
            # Используем правильные заголовки
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Accept-Encoding': 'gzip',
                'Content-Type': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/entity/company",
                headers=headers,
                timeout=15
            )

            logger.info(f"Проверка токена: статус {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                org_name = data.get('name', 'Неизвестно')
                return True, f"✅ Активен (организация: {org_name})"
            elif response.status_code == 401:
                return False, "❌ Неавторизован (токен недействителен или устарел)"
            elif response.status_code == 403:
                return False, "❌ Доступ запрещен (недостаточно прав)"
            elif response.status_code == 412:
                return False, "❌ Неверный формат запроса (ошибка 412)"
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get('errors', [{}])[0].get('error', f"Ошибка {response.status_code}")
                    return False, f"❌ {error_msg}"
                except:
                    return False, f"❌ Ошибка API: {response.status_code}"

        except requests.exceptions.Timeout:
            return False, "❌ Таймаут запроса (сервер не отвечает)"
        except requests.exceptions.ConnectionError:
            return False, "❌ Ошибка соединения"
        except Exception as e:
            logger.error(f"Ошибка при проверке токена: {e}")
            return False, f"❌ Ошибка: {str(e)[:100]}"

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
                    'phone': data.get('phone', 'Не указан'),
                    'actual_address': data.get('actualAddress', 'Не указан'),
                    'legal_address': data.get('legalAddress', 'Не указан')
                }
        except Exception as e:
            logger.error(f"Ошибка получения информации об организации: {e}")
        return {}

    def get_customer_orders_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о заказах покупателей"""
        try:
            start_date_only = start_date.split()[0] if ' ' in start_date else start_date
            end_date_only = end_date.split()[0] if ' ' in end_date else end_date

            filter_params = {
                'filter': f'moment>={start_date_only} 00:00:00;moment<={end_date_only} 23:59:59',
                'limit': 1000,
                'expand': 'agent'
            }

            response = requests.get(
                f"{self.base_url}/entity/customerorder",
                headers=self.headers,
                params=filter_params,
                timeout=self.timeout
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()
            total_amount = Decimal('0')
            count = 0
            orders_data = []

            if 'rows' in data:
                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        agent_info = None
                        if 'agent' in row and row['agent']:
                            agent = row['agent']
                            agent_href = agent.get('meta', {}).get('href')

                            if agent_href:
                                try:
                                    agent_response = requests.get(
                                        agent_href,
                                        headers=self.headers,
                                        timeout=10
                                    )

                                    if agent_response.status_code == 200:
                                        agent_full = agent_response.json()
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
                                except Exception:
                                    agent_info = {
                                        'id': agent_href.split('/')[-1],
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

                logger.info(f"📦 Получено {count} заказов на сумму {total_amount} ₽")

            return count, total_amount, orders_data

        except Exception as e:
            logger.error(f"Ошибка в get_customer_orders_data: {e}", exc_info=True)
            return 0, Decimal('0'), []

    def get_retail_sales_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о розничных продажах"""
        try:
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
                timeout=self.timeout
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API при запросе розничных продаж: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()
            total_sales = Decimal('0')
            count = 0
            sales_data = []

            if 'rows' in data:
                for row in data['rows']:
                    if row.get('sum'):
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
                            'retail': True
                        })

            logger.info(f"Получено {count} розничных продаж на сумму {total_sales} ₽")
            return count, total_sales, sales_data

        except Exception as e:
            logger.error(f"Ошибка при получении розничных продаж: {e}", exc_info=True)
            return 0, Decimal('0'), []

    def get_incoming_payments_data(self, start_date: str, end_date: str) -> Tuple[int, Decimal, List[dict]]:
        """Получает данные о входящих платежах за период"""
        try:
            filter_params = {
                'filter': f'moment>={start_date};moment<={end_date}',
                'limit': 100,
                'expand': 'agent'
            }

            response = requests.get(
                f"{self.base_url}/entity/paymentin",
                headers=self.headers,
                params=filter_params,
                timeout=self.timeout
            )

            if response.status_code != 200:
                logger.error(f"Ошибка API при запросе платежей: {response.status_code}")
                return 0, Decimal('0'), []

            data = response.json()
            total_amount = Decimal('0')
            count = 0
            payments_data = []

            if 'rows' in data:
                for i, row in enumerate(data['rows']):
                    if row.get('sum'):
                        agent_info = None
                        if 'agent' in row and row['agent']:
                            agent = row['agent']
                            raw_name = agent.get('name')
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

            logger.info(f"Получено {count} платежей на сумму {total_amount} ₽")
            return count, total_amount, payments_data

        except Exception as e:
            logger.error(f"Ошибка при получении платежей: {e}", exc_info=True)
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
            for order in orders_data:
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

            # Топ покупателей
            all_customers = list(customers.values())
            top_customers = sorted(all_customers, key=lambda x: x['total'], reverse=True)[:10]

            # Средние чеки
            avg_order = orders_total / orders_count if orders_count > 0 else Decimal('0')
            avg_retail = retail_total / retail_count if retail_count > 0 else Decimal('0')
            avg_total = total_amount / total_count if total_count > 0 else Decimal('0')

            # Новые и постоянные покупатели
            new_customers = sum(1 for cust in customers.values() if cust['orders'] == 1)
            returning_customers = sum(1 for cust in customers.values() if cust['orders'] > 1)

            # Списки покупателей
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
            logger.error(f"Ошибка в get_sales_stats_with_retail: {e}", exc_info=True)
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

    def get_incoming_payments_stats(self, start_date: str, end_date: str) -> Dict:
        """Получает статистику по входящим платежам"""
        try:
            count, total_amount, payments = self.get_incoming_payments_data(start_date, end_date)

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

            return {
                'total_payments': count,
                'total_amount': total_amount,
                'customer_count': len(customers),
                'top_payers': top_payers,
                'payment_types': payment_types_stats,
                'payments_data': payments
            }

        except Exception as e:
            logger.error(f"Ошибка stats платежей: {e}")
            return {
                'total_payments': 0,
                'total_amount': Decimal('0'),
                'customer_count': 0,
                'top_payers': [],
                'payment_types': [],
                'payments_data': []
            }

    def get_daily_summary(self) -> Dict:
        """Получает сводку за сегодня"""
        try:
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


# ============================================================
# ОСНОВНЫЕ ФУНКЦИИ БОТА
# ============================================================

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
        start_date = now - timedelta(days=30)
        end_date = now

    return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')


def calculate_ratio(new: int, returning: int) -> str:
    """Рассчитывает соотношение новых и постоянных покупателей"""
    total = new + returning
    if total == 0:
        return "0% / 0%"

    new_percent = (new / total) * 100
    returning_percent = (returning / total) * 100

    return f"{new_percent:.1f}% / {returning_percent:.1f}%"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user

    # Обновляем активность пользователя
    update_user_activity(
        user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )

    # Получаем информацию о пользователе
    has_token = bool(get_user_token(user.id))
    token_status = "✅ Настроен" if has_token else "❌ Не настроен"

    welcome_text = f"""
🤖 *Бот статистики МойСклад*

👤 *Пользователь:* {user.first_name or user.username}
🔑 *Токен API:* {token_status}

📊 *Доступные команды:*
/today - Статистика за сегодня
/week - Статистика за неделю
/month - Статистика за месяц
/period - Статистика за указанный период
/top - Топ покупателей за месяц
/token - Управление токеном API
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
            InlineKeyboardButton("🔑 Токен API", callback_data='token_menu'),
            InlineKeyboardButton("📊 Произвольный период", callback_data='period_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')


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


async def send_statistics(update: Update, period: str, period_name: str):
    """Отправляет статистику за период"""
    try:
        # Определяем тип запроса: сообщение или callback query
        if isinstance(update, Update) and update.message:
            # Это команда из чата (например, /today)
            user_id = update.effective_user.id
            user = update.effective_user
            message_to_edit = None
        elif isinstance(update, Update) and update.callback_query:
            # Это нажатие кнопки через callback query
            query = update.callback_query
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query
        else:
            # Это уже сам CallbackQuery объект
            query = update  # update на самом деле уже CallbackQuery
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query

        # Обновляем активность пользователя
        update_user_activity(user_id, user.username, user.first_name, user.last_name)

        # Создаем клиент
        client = SimpleMoySkladClient(user_id)

        # Проверяем токен
        if not client.token:
            error_msg = """
❌ *Токен API не настроен!*

Для работы бота необходимо:
1. Установить токен МойСклад командой /token
2. Или настроить общий токен в файле .env
"""
            if message_to_edit:
                await message_to_edit.edit_message_text(error_msg, parse_mode='Markdown')
            else:
                await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        is_valid, valid_message = client.is_token_valid()
        if not is_valid:
            error_msg = f"""
❌ *Проблема с токеном API!*

Ошибка: {valid_message}

Проверьте токен командой /token
"""
            if message_to_edit:
                await message_to_edit.edit_message_text(error_msg, parse_mode='Markdown')
            else:
                await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Показываем сообщение о загрузке
        loading_msg = f"⏳ *Загружаю статистику за {period_name}...*"
        if message_to_edit:
            await message_to_edit.edit_message_text(loading_msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(loading_msg, parse_mode='Markdown')

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        # Формируем сообщение
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

        message += f"\n⏰ Обновлено: {timestamp}"

        # Кнопки навигации
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

        if message_to_edit:
            # Редактируем существующее сообщение
            await message_to_edit.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            # Отправляем новое сообщение
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_statistics: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики за {period_name}: {str(e)}"

        # Определяем куда отправлять ошибку
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        elif isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text(error_msg)
        else:
            # Это уже CallbackQuery
            await update.edit_message_text(error_msg)


async def send_top_customers(update: Update, period: str, period_name: str):
    """Отправляет топ покупателей по заказам за период"""
    try:
        # Определяем тип запроса
        if isinstance(update, Update) and update.message:
            user_id = update.effective_user.id
            user = update.effective_user
            message_to_edit = None
        elif isinstance(update, Update) and update.callback_query:
            query = update.callback_query
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query
        else:
            query = update
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query

        update_user_activity(user_id, user.username, user.first_name, user.last_name)

        client = SimpleMoySkladClient(user_id)

        # Проверяем токен
        if not client.token:
            error_msg = "❌ *Токен API не настроен!*"
            if message_to_edit:
                await message_to_edit.edit_message_text(error_msg, parse_mode='Markdown')
            else:
                await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        # Получаем даты
        start_date, end_date = get_period_dates(period)

        # Показываем сообщение о загрузке
        loading_msg = f"⏳ *Загружаю топ покупателей за {period_name}...*"
        if message_to_edit:
            await message_to_edit.edit_message_text(loading_msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(loading_msg, parse_mode='Markdown')

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

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

        if message_to_edit:
            await message_to_edit.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_top_customers: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении топа покупателей за {period_name}: {str(e)}"

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        elif isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text(error_msg)
        else:
            await update.edit_message_text(error_msg)

# ============================================================
# ОБРАБОТЧИКИ ДЛЯ ВВОДА ПРОИЗВОЛЬНОГО ПЕРИОДА
# ============================================================

async def period_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало ввода произвольного периода"""
    user = update.effective_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

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

    return PERIOD_START_DATE


async def handle_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода начальной даты"""
    user = update.effective_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    user_input = update.message.text.strip()

    try:
        date_formats = ['%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        date_obj = None

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(user_input, fmt)
                date_str = date_obj.strftime('%d.%m.%Y')
                break
            except ValueError:
                continue

        if date_obj is None:
            await update.message.reply_text(
                "❌ *Неверный формат даты!*\n\n"
                "Пожалуйста, введите дату в формате ДД.ММ.ГГГГ\n"
                "Например: 01.01.2024\n\n"
                "Попробуйте снова:",
                parse_mode='Markdown'
            )
            return PERIOD_START_DATE

        # Сохраняем начальную дату
        context.user_data['period_start_date'] = date_str

        await update.message.reply_text(
            f"✅ *Начальная дата принята:* {date_str}\n\n"
            "📅 Теперь введите конечную дату в том же формате:\n"
            "Например: 31.01.2024",
            parse_mode='Markdown'
        )

        return PERIOD_END_DATE

    except Exception as e:
        logger.error(f"Ошибка при обработке начальной даты: {e}")
        await update.message.reply_text(
            "❌ *Ошибка при обработке даты!*\n\n"
            "Попробуйте снова:",
            parse_mode='Markdown'
        )
        return PERIOD_START_DATE


async def handle_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода конечной даты"""
    user = update.effective_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    user_input = update.message.text.strip()

    try:
        date_formats = ['%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        end_date_obj = None

        for fmt in date_formats:
            try:
                end_date_obj = datetime.strptime(user_input, fmt)
                end_date_str = end_date_obj.strftime('%d.%m.%Y')
                break
            except ValueError:
                continue

        if end_date_obj is None:
            await update.message.reply_text(
                "❌ *Неверный формат даты!*\n\n"
                "Попробуйте снова:",
                parse_mode='Markdown'
            )
            return PERIOD_END_DATE

        # Получаем начальную дату
        start_date_str = context.user_data.get('period_start_date')

        if not start_date_str:
            await update.message.reply_text(
                "❌ *Ошибка: не найдена начальная дата!*",
                parse_mode='Markdown'
            )
            return ConversationHandler.END

        # Преобразуем строки в даты
        start_date_obj = datetime.strptime(start_date_str, '%d.%m.%Y')

        # Проверяем, что конечная дата не раньше начальной
        if end_date_obj < start_date_obj:
            await update.message.reply_text(
                f"❌ *Конечная дата не может быть раньше начальной!*\n\n"
                f"Начальная дата: {start_date_str}\n"
                f"Конечная дата: {end_date_str}\n\n"
                f"Попробуйте снова:",
                parse_mode='Markdown'
            )
            return PERIOD_END_DATE

        # Формируем даты для API
        start_date_api = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        end_date_api = end_date_obj.strftime('%Y-%m-%d 23:59:59')

        # Очищаем временные данные
        if 'period_start_date' in context.user_data:
            del context.user_data['period_start_date']

        # Отправляем статистику
        await send_period_statistics(
            update,
            start_date_api,
            end_date_api,
            start_date_str,
            end_date_str
        )

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка при обработке конечной даты: {e}")
        await update.message.reply_text(
            "❌ *Ошибка при обработке даты!*\n\n"
            "Попробуйте снова:",
            parse_mode='Markdown'
        )
        return PERIOD_END_DATE


async def send_period_statistics(update: Update, start_date: str, end_date: str,
                                 start_date_display: str, end_date_display: str):
    """Отправляет статистику за произвольный период"""
    try:
        if isinstance(update, Update) and update.message:
            user_id = update.effective_user.id
            user = update.effective_user
        else:
            user_id = update.callback_query.from_user.id
            user = update.callback_query.from_user

        update_user_activity(user_id, user.username, user.first_name, user.last_name)

        client = SimpleMoySkladClient(user_id)

        # Проверяем токен
        if not client.token:
            error_msg = "❌ *Токен API не настроен!*"
            await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        is_valid, valid_message = client.is_token_valid()
        if not is_valid:
            error_msg = f"❌ *Проблема с токеном API!*\n\nОшибка: {valid_message}"
            await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        # Рассчитываем длительность периода
        start_date_obj = datetime.strptime(start_date_display, '%d.%m.%Y')
        end_date_obj = datetime.strptime(end_date_display, '%d.%m.%Y')
        days_count = (end_date_obj - start_date_obj).days + 1

        # Получаем статистику
        stats = client.get_sales_stats_with_retail(start_date, end_date)

        timestamp = datetime.now().strftime('%H:%M:%S')

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

        if stats['customer_count'] > 0:
            message += f"""
👤 *Анализ покупателей (по заказам):*
• Новые покупатели (1 заказ): *{stats['new_customers']}*
• Постоянные покупатели (>1 заказа): *{stats['returning_customers']}*
• Соотношение новых/постоянных: *{calculate_ratio(stats['new_customers'], stats['returning_customers'])}*
"""

        # Средние показатели в день
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
            await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_period_statistics: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при получении статистики за период {start_date_display} - {end_date_display}: {str(e)}"
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg)
        else:
            await update.edit_message_text(error_msg)


async def cancel_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена ввода периода"""
    if 'period_start_date' in context.user_data:
        del context.user_data['period_start_date']

    await update.message.reply_text(
        "❌ *Ввод периода отменен.*\n\n"
        "Для ввода нового периода используйте команду /period"
    )

    return ConversationHandler.END


# ============================================================
# ОБРАБОТЧИКИ УПРАВЛЕНИЯ ТОКЕНАМИ
# ============================================================

async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление токеном МойСклад"""
    user = update.effective_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    has_token = bool(get_user_token(user.id))
    token_status = "✅ *Активен*" if has_token else "❌ *Не настроен*"

    keyboard = [
        [
            InlineKeyboardButton("🔑 Установить токен", callback_data='set_token'),
            InlineKeyboardButton("✅ Проверить токен", callback_data='check_token')
        ],
        [
            InlineKeyboardButton("🗑️ Удалить токен", callback_data='delete_token'),
            InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = f"""
🔑 *Управление токеном МойСклад*

👤 Пользователь: {user.first_name or user.username}
🔑 Статус: {token_status}

Выберите действие:
• *Установить токен* - добавить или изменить токен
• *Проверить токен* - проверить валидность токена
• *Удалить токен* - удалить сохраненный токен
"""

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        # Это callback query
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def set_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса установки токена"""
    user = update.effective_user if update.message else update.callback_query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    message = """
🔑 *Установка токена МойСклад*

Отправьте ваш токен API.

⚠️ *Внимание:*
• Токен выглядит как длинная строка символов
• Для безопасности достаточно прав на чтение
• Никому не сообщайте свой токен

*Отправьте токен:* (или /cancel для отмены)
"""

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, parse_mode='Markdown')
    else:
        # Это callback query
        await update.callback_query.edit_message_text(message, parse_mode='Markdown')

    return TOKEN_INPUT


async def handle_token_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода токена"""
    try:
        user = update.effective_user

        # ОБЯЗАТЕЛЬНО логируем получение сообщения
        logger.info(f"=" * 50)
        logger.info(f"ПОЛУЧЕН ТОКЕН ОТ ПОЛЬЗОВАТЕЛЯ {user.id} ({user.username})")
        logger.info(f"Длина сообщения: {len(update.message.text)} символов")
        logger.info(f"Полное сообщение: {update.message.text}")
        logger.info(f"=" * 50)

        update_user_activity(user.id, user.username, user.first_name, user.last_name)

        token = update.message.text.strip()

        # Проверяем, не пустой ли токен
        if not token:
            await update.message.reply_text(
                "❌ *Вы отправили пустое сообщение!*\n\n"
                "Пожалуйста, отправьте токен или используйте /cancel",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        # ОЧИЩАЕМ токен максимально агрессивно
        import re
        original_token = token

        # 1. Удаляем ВСЕ пробелы, табуляции, переносы
        token = re.sub(r'\s+', '', token)

        # 2. Удаляем форматирование Markdown
        token = token.replace('`', '').replace('*', '').replace('_', '').replace('~', '')
        token = token.replace('\\', '').replace('/', '')

        # 3. Проверяем базовую структуру
        if '.' not in token:
            logger.error(f"Токен не содержит точек, возможно поврежден: {token[:50]}...")
            await update.message.reply_text(
                "❌ *Неверный формат токена!*\n\n"
                "Токен должен содержать точки (формат JWT).\n"
                "Возможно, токен поврежден при копировании.\n\n"
                "Создайте новый токен и попробуйте снова.\n"
                "Или /cancel для отмены",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        if len(token) < 50:
            logger.error(f"Токен слишком короткий: {len(token)} символов")
            await update.message.reply_text(
                f"❌ *Токен слишком короткий!*\n\n"
                f"Длина после очистки: {len(token)} символов\n"
                f"Ожидается: 100+ символов\n\n"
                f"Проверьте, что скопировали весь токен.\n"
                f"Или /cancel для отмены",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        # 4. Проверяем структуру JWT
        token_parts = token.split('.')
        if len(token_parts) != 3:
            logger.error(f"Токен не в формате JWT: {len(token_parts)} частей")
            await update.message.reply_text(
                f"❌ *Неверный формат JWT!*\n\n"
                f"Токен должен состоять из 3 частей.\n"
                f"Найдено: {len(token_parts)} частей\n\n"
                f"Создайте новый токен и попробуйте снова.\n"
                f"Или /cancel для отмены",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        # Логируем информацию о токене
        logger.info(f"Токен после очистки: {len(token)} символов")
        logger.info(f"Части JWT: {len(token_parts)}")
        logger.info(f"Длины частей: {[len(p) for p in token_parts]}")

        checking_msg = await update.message.reply_text(
            "⏳ *Проверяю токен...*\n\n"
            "Это может занять несколько секунд.",
            parse_mode='Markdown'
        )

        # ============================================================
        # ПРОСТАЯ проверка токена - минимальный запрос
        # ============================================================
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept-Encoding': 'gzip'
        }

        try:
            # Делаем ПРОСТОЙ запрос для проверки
            logger.info(f"Отправляю запрос к API МойСклад...")
            response = requests.get(
                f"{MOYSKLAD_BASE_URL}/entity/company",
                headers=headers,
                timeout=20
            )

            logger.info(f"Ответ получен: статус {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                org_name = data.get('name', 'Неизвестно')

                logger.info(f"✅ ТОКЕН РАБОТАЕТ! Организация: {org_name}")

                # Сохраняем токен
                set_user_token(user.id, token)

                await checking_msg.delete()

                success_msg = f"""
✅ *Токен успешно установлен и проверен!*

🏢 Организация: *{org_name}*
👤 Пользователь: *{user.first_name or user.username}*

Теперь вы можете использовать все функции бота.
"""

                keyboard = [
                    [InlineKeyboardButton("📊 Статистика за сегодня", callback_data='today')],
                    [InlineKeyboardButton("✅ Проверить токен", callback_data='check_token')],
                    [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await update.message.reply_text(success_msg, reply_markup=reply_markup, parse_mode='Markdown')
                return ConversationHandler.END

            else:
                # Подробная диагностика ошибки
                try:
                    error_data = response.json()
                    errors = error_data.get('errors', [])
                    if errors:
                        error_msg = errors[0].get('error', f"Ошибка {response.status_code}")
                    else:
                        error_msg = f"Ошибка {response.status_code}"
                except:
                    error_msg = f"Ошибка {response.status_code}"

                logger.error(f"Токен не прошел проверку: {error_msg}")

                # Диагностическая информация
                diagnostic = f"""
📊 *Диагностика:*

• Статус ответа: {response.status_code}
• Ошибка: {error_msg}
• Длина токена: {len(token)} символов
• Формат JWT: {'✅' if len(token_parts) == 3 else '❌'}
"""

                await checking_msg.edit_text(
                    f"❌ *Токен не прошел проверку!*\n\n"
                    f"{diagnostic}\n\n"
                    f"*Что сделать:*\n"
                    f"1. Убедитесь, что токен активен\n"
                    f"2. Проверьте права токена (нужны на чтение)\n"
                    f"3. Создайте новый токен\n"
                    f"4. Попробуйте снова\n\n"
                    f"Или /cancel для отмены",
                    parse_mode='Markdown'
                )
                return TOKEN_INPUT

        except requests.exceptions.Timeout:
            logger.error("Таймаут при проверке токена")
            await checking_msg.edit_text(
                "❌ *Таймаут запроса!*\n\n"
                "Сервер МойСклад не ответил.\n"
                "Попробуйте позже или /cancel",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        except requests.exceptions.ConnectionError:
            logger.error("Ошибка соединения при проверке токена")
            await checking_msg.edit_text(
                "❌ *Ошибка соединения!*\n\n"
                "Не удалось подключиться к серверу.\n"
                "Проверьте интернет-соединение.\n"
                "Попробуйте позже или /cancel",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

        except Exception as e:
            logger.error(f"Ошибка при проверке токена: {e}", exc_info=True)
            await checking_msg.edit_text(
                f"❌ *Ошибка проверки!*\n\n"
                f"Ошибка: {str(e)[:100]}\n\n"
                f"Попробуйте снова или /cancel",
                parse_mode='Markdown'
            )
            return TOKEN_INPUT

    except Exception as e:
        logger.error(f"Критическая ошибка в handle_token_input: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                f"❌ *Внутренняя ошибка бота!*\n\n"
                f"Пожалуйста, сообщите администратору.\n"
                f"Ошибка: {str(e)[:100]}",
                parse_mode='Markdown'
            )
        except:
            pass
        return ConversationHandler.END


async def check_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка токена"""
    user = update.effective_user if update.message else update.callback_query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    token = get_user_token(user.id)

    if not token:
        message = "❌ *Токен не установлен!*\n\nИспользуйте команду /token для установки токена."
    else:
        client = SimpleMoySkladClient(user.id)
        is_valid, error_message = client.is_token_valid()

        if is_valid:
            user_info = get_user_info(user.id)
            org_name = user_info.get('organization_name', 'Неизвестно') if user_info else 'Неизвестно'
            message = f"""
✅ *Токен активен и работает!*

🏢 Организация: *{org_name}*
👤 Пользователь: *{user.first_name or user.username}*

Токен действителен и готов к использованию.
"""
        else:
            message = f"""
❌ *Токен недействителен!*

Ошибка: {error_message}

Используйте команду /token для установки нового токена.
"""

    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='token_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        # Это callback query
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def delete_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление токена"""
    user = update.effective_user if update.message else update.callback_query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    has_token = bool(get_user_token(user.id))

    if not has_token:
        message = "❌ *У вас нет сохраненного токена для удаления.*"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='token_menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            # Это callback query
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        return

    keyboard = [
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data='confirm_delete_token'),
            InlineKeyboardButton("❌ Нет, отмена", callback_data='token_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
🗑️ *Удаление токена*

⚠️ *Вы уверены, что хотите удалить токен?*

После удаления:
• Вы не сможете получать статистику
• Вам нужно будет установить новый токен

*Это действие нельзя отменить!*
"""

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        # Это callback query
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

async def delete_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление токена"""
    user = update.effective_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    has_token = bool(get_user_token(user.id))

    if not has_token:
        message = "❌ *У вас нет сохраненного токена для удаления.*"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='token_menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        return

    keyboard = [
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data='confirm_delete_token'),
            InlineKeyboardButton("❌ Нет, отмена", callback_data='token_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = """
🗑️ *Удаление токена*

⚠️ *Вы уверены, что хотите удалить токен?*

После удаления:
• Вы не сможете получать статистику
• Вам нужно будет установить новый токен

*Это действие нельзя отменить!*
"""

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def confirm_delete_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления токена"""
    query = update.callback_query
    user = query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    delete_user_token(user.id)

    message = """
✅ *Токен удален!*

Теперь вы можете:
1. Использовать общий токен (если он настроен в .env)
2. Установить новый токен командой /token
"""

    keyboard = [
        [
            InlineKeyboardButton("🔑 Установить новый токен", callback_data='set_token'),
            InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def cancel_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции с токеном"""
    message = "❌ *Операция отменена.*"
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        # Это callback query
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    return ConversationHandler.END


async def cancel_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции с токеном"""
    message = "❌ *Операция отменена.*"
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    return ConversationHandler.END


# ============================================================
# ОБРАБОТЧИК КНОПОК
# ============================================================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий кнопок"""
    query = update.callback_query
    await query.answer()

    user = query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    try:
        if query.data == 'main_menu':
            await start_from_callback(query)

        elif query.data == 'token_menu':
            await token_command_from_callback(query)

        elif query.data == 'set_token':
            await set_token_command(update, context)  # Передаем update, а не query

        elif query.data == 'check_token':
            await check_token_command(update, context)  # Передаем update

        elif query.data == 'delete_token':
            await delete_token_command(update, context)  # Передаем update

        elif query.data == 'confirm_delete_token':
            await confirm_delete_token(update, context)  # Передаем update

        elif query.data == 'cancel_token':
            await cancel_token(update, context)  # Передаем update

        elif query.data == 'period_menu':
            await period_menu_handler(query, context)

        elif query.data == 'today':
            await query.edit_message_text("⏳ *Загружаю статистику за сегодня...*", parse_mode='Markdown')
            await send_statistics(query, 'today', 'сегодня')  # Передаем query

        elif query.data == 'week':
            await query.edit_message_text("⏳ *Загружаю статистику за неделю...*", parse_mode='Markdown')
            await send_statistics(query, 'week', 'неделю')  # Передаем query

        elif query.data == 'month':
            await query.edit_message_text("⏳ *Загружаю статистику за месяц...*", parse_mode='Markdown')
            await send_statistics(query, 'month', 'месяц')  # Передаем query

        elif query.data == 'top':
            await query.edit_message_text("⏳ *Загружаю топ покупателей...*", parse_mode='Markdown')
            await send_top_customers(query, 'month', 'месяц')  # Передаем query

        elif query.data.startswith('customers_'):
            period = query.data.split('_')[1]
            period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
            await query.edit_message_text(f"⏳ *Загружаю детали по покупателям за {period_name}...*",
                                          parse_mode='Markdown')
            await send_customers_details(query, period, period_name)  # Передаем query

        elif query.data.startswith('top_'):
            period = query.data.split('_')[1]
            period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
            await query.edit_message_text(f"⏳ *Загружаю топ покупателей за {period_name}...*", parse_mode='Markdown')
            await send_top_customers(query, period, period_name)  # Передаем query

        elif query.data == 'daily_summary':
            await query.edit_message_text("⏳ *Загружаю итоги дня...*", parse_mode='Markdown')
            await send_daily_summary(query)  # Передаем query

        elif query.data.startswith('payments_'):
            if query.data == 'payments_menu':
                await payments_menu(query, context)
            else:
                period = query.data.split('_')[1] if len(query.data.split('_')) > 1 else 'today'
                period_name = {'today': 'сегодня', 'week': 'неделю', 'month': 'месяц'}.get(period, period)
                await query.edit_message_text(f"⏳ *Загружаю платежи за {period_name}...*", parse_mode='Markdown')
                await send_incoming_payments(query, period, period_name)  # Передаем query

        elif query.data.startswith('customers_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(
                    f"⏳ *Загружаю детали по покупателям за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown')
                await customers_custom_period(query, start_date_display, end_date_display)  # Передаем query

        elif query.data.startswith('top_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(
                    f"⏳ *Загружаю топ покупателей за {start_date_display} - {end_date_display}...*",
                    parse_mode='Markdown')
                await send_top_customers_custom(query, start_date_display, end_date_display)  # Передаем query

        elif query.data.startswith('payments_custom_'):
            parts = query.data.split('_')
            if len(parts) >= 4:
                start_date_display = parts[2]
                end_date_display = parts[3]
                await query.edit_message_text(f"⏳ *Загружаю платежи за {start_date_display} - {end_date_display}...*",
                                              parse_mode='Markdown')
                await send_payments_custom_period(query, start_date_display, end_date_display)  # Передаем query

    except Exception as e:
        logger.error(f"Ошибка в обработке кнопки {query.data}: {e}", exc_info=True)
        try:
            await query.edit_message_text(
                f"❌ *Ошибка при обработке запроса*\n\n"
                f"Ошибка: {str(e)[:200]}\n\n"
                f"Попробуйте снова.",
                parse_mode='Markdown'
            )
        except Exception:
            try:
                await query.message.reply_text(
                    f"❌ Ошибка при обработке запроса: {str(e)[:100]}"
                )
            except Exception:
                pass


async def start_from_callback(query):
    """Старт из callback"""
    user = query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    has_token = bool(get_user_token(user.id))
    token_status = "✅ Настроен" if has_token else "❌ Не настроен"

    welcome_text = f"""
🤖 *Бот статистики МойСклад*

👤 *Пользователь:* {user.first_name or user.username}
🔑 *Токен API:* {token_status}

📊 *Доступные команды:*
/today - Статистика за сегодня
/week - Статистика за неделю
/month - Статистика за месяц
/period - Статистика за указанный период
/top - Топ покупателей за месяц
/token - Управление токеном API
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
            InlineKeyboardButton("🔑 Токен API", callback_data='token_menu'),
            InlineKeyboardButton("📊 Произвольный период", callback_data='period_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')


async def token_command_from_callback(query):
    """Команда токена из callback"""
    user = query.from_user
    update_user_activity(user.id, user.username, user.first_name, user.last_name)

    has_token = bool(get_user_token(user.id))
    token_status = "✅ *Активен*" if has_token else "❌ *Не настроен*"

    keyboard = [
        [
            InlineKeyboardButton("🔑 Установить токен", callback_data='set_token'),
            InlineKeyboardButton("✅ Проверить токен", callback_data='check_token')
        ],
        [
            InlineKeyboardButton("🗑️ Удалить токен", callback_data='delete_token'),
            InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = f"""
🔑 *Управление токеном МойСклад*

👤 Пользователь: {user.first_name or user.username}
🔑 Статус: {token_status}

Выберите действие:
• *Установить токен* - добавить или изменить токен
• *Проверить токен* - проверить валидность токена
• *Удалить токен* - удалить сохраненный токен
"""

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')


# ============================================================
# ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ (НУЖНО ДОПИСАТЬ)
# ============================================================

async def period_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню выбора произвольного периода"""
    # Реализация меню периода
    await update.edit_message_text("📊 *Выберите период*", parse_mode='Markdown')


async def send_customers_details(update: Update, period: str, period_name: str):
    """Детали по покупателям за период"""
    # Реализация деталей покупателей
    await update.edit_message_text(f"👥 *Детали по покупателям за {period_name}*", parse_mode='Markdown')


async def send_daily_summary(update: Update):
    """Итоги дня"""
    try:
        # Определяем тип запроса
        if isinstance(update, Update) and update.message:
            user_id = update.effective_user.id
            user = update.effective_user
            message_to_edit = None
        elif isinstance(update, Update) and update.callback_query:
            query = update.callback_query
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query
        else:
            query = update
            user_id = query.from_user.id
            user = query.from_user
            message_to_edit = query

        update_user_activity(user_id, user.username, user.first_name, user.last_name)

        client = SimpleMoySkladClient(user_id)

        if not client.token:
            error_msg = "❌ *Токен API не настроен!*"
            if message_to_edit:
                await message_to_edit.edit_message_text(error_msg, parse_mode='Markdown')
            else:
                await update.message.reply_text(error_msg, parse_mode='Markdown')
            return

        # Показываем сообщение о загрузке
        loading_msg = "⏳ *Загружаю итоги дня...*"
        if message_to_edit:
            await message_to_edit.edit_message_text(loading_msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(loading_msg, parse_mode='Markdown')

        summary = client.get_daily_summary()
        timestamp = datetime.now().strftime('%H:%M:%S')

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

        if summary['top_customers']:
            message += f"\n🏆 *ТОП-3 ПОКУПАТЕЛЯ ДНЯ:*\n"
            for i, customer in enumerate(summary['top_customers'], 1):
                phone_info = f" 📞 {customer['phone']}" if customer['phone'] != 'Не указан' else ""
                orders_text = "заказ" if customer['orders'] == 1 else "заказа"
                message += f"{i}. *{customer['name']}*{phone_info}\n"
                message += f"   💰 *{customer['total']:,.2f} ₽* ({customer['orders']} {orders_text})\n"

        if summary['top_payers']:
            message += f"\n💰 *ТОП-3 ПЛАТЕЛЬЩИКА ДНЯ:*\n"
            for i, payer in enumerate(summary['top_payers'], 1):
                phone_info = f" 📞 {payer['phone']}" if payer['phone'] != 'Не указан' else ""
                payments_text = "платеж" if payer['payments'] == 1 else "платежа"
                message += f"{i}. *{payer['name']}*{phone_info}\n"
                message += f"   💸 *{payer['total']:,.2f} ₽* ({payer['payments']} {payments_text})\n"

        total_revenue = summary['total_sales']['total'] + summary['payments']['total']
        message += f"\n💵 *ОБЩАЯ ВЫРУЧКА ДНЯ:* *{total_revenue:,.2f} ₽*\n"
        message += f"\n⏰ *Обновлено:* {timestamp}"

        keyboard = [
            [
                InlineKeyboardButton("📊 Подробная статистика", callback_data='today'),
                InlineKeyboardButton("🔄 Обновить", callback_data='daily_summary')
            ],
            [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if message_to_edit:
            await message_to_edit.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в send_daily_summary: {e}", exc_info=True)
        error_msg = f"❌ Ошибка при формировании итогов дня: {str(e)}"

        if isinstance(update, Update) and update.message:
            await update.message.reply_text(error_msg, parse_mode='Markdown')
        elif isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text(error_msg, parse_mode='Markdown')
        else:
            await update.edit_message_text(error_msg, parse_mode='Markdown')


async def send_incoming_payments(update: Update, period: str, period_name: str):
    """Входящие платежи за период"""
    # Реализация платежей
    await update.edit_message_text(f"💰 *Платежи за {period_name}*", parse_mode='Markdown')


async def customers_custom_period(update: Update, start_date_display: str, end_date_display: str):
    """Детали по покупателям за произвольный период"""
    # Реализация
    await update.edit_message_text(f"👥 *Детали по покупателям за {start_date_display} - {end_date_display}*",
                                   parse_mode='Markdown')


async def send_top_customers_custom(update: Update, start_date_display: str, end_date_display: str):
    """Топ покупателей за произвольный период"""
    # Реализация
    await update.edit_message_text(f"🏆 *Топ покупателей за {start_date_display} - {end_date_display}*",
                                   parse_mode='Markdown')


async def send_payments_custom_period(update: Update, start_date_display: str, end_date_display: str):
    """Платежи за произвольный период"""
    # Реализация
    await update.edit_message_text(f"💰 *Платежи за {start_date_display} - {end_date_display}*", parse_mode='Markdown')


async def payments_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню платежей"""
    # Реализация
    await update.edit_message_text("💰 *Меню платежей*", parse_mode='Markdown')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Справка"""
    help_text = """
📚 *Справка по боту*

*Основные команды:*
/start - Главное меню с кнопками
/today - Статистика за сегодня
/week - Статистика за неделю
/month - Статистика за месяц
/period - Статистика за произвольный период
/top - Топ покупателей за месяц
/token - Управление токеном API
/help - Эта справка

*Как установить токен:*
1. Используйте команду /token
2. Выберите "Установить токен"
3. Вставьте токен из МойСклад

*Где взять токен:*
МойСклад → Настройки → Безопасность → API → Токены
"""

    await update.message.reply_text(help_text, parse_mode='Markdown')


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Ошибка в боте: {context.error}", exc_info=True)

    error_str = str(context.error)
    if "Query is too old" in error_str or "response timeout expired" in error_str:
        logger.warning("Игнорируем ошибку устаревшего запроса")
        return

    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "❌ Произошла ошибка. Попробуйте снова."
            )
    except Exception:
        pass


# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def main():
    """Основная функция"""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ Не установлен TELEGRAM_BOT_TOKEN в .env файле")
        return

    if not MOYSKLAD_TOKEN:
        logger.warning("⚠️ Не установлен MOYSKLAD_TOKEN в .env файле")
        logger.info("Бот будет работать только с токенами пользователей")

    try:
        logger.info("=" * 50)
        logger.info("ЗАПУСК БОТА МОЙСКЛАД - ПРОСТАЯ ВЕРСИЯ")
        logger.info("=" * 50)

        # Создаем приложение
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # ============================================================
        # ИСПРАВЛЕННЫЙ ConversationHandler для токенов
        # ============================================================
        token_conversation_handler = ConversationHandler(
            entry_points=[
                CommandHandler("token", token_command),
                CallbackQueryHandler(set_token_command, pattern='^set_token$')
            ],
            states={
                TOKEN_INPUT: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        handle_token_input
                    ),
                    CommandHandler("cancel", cancel_token)
                ]
            },
            fallbacks=[
                CommandHandler("cancel", cancel_token),
                CallbackQueryHandler(cancel_token, pattern='^cancel_token$')
            ],
            allow_reentry=True,
            name="token_conversation"
        )

        # ConversationHandler для периода
        period_conversation_handler = ConversationHandler(
            entry_points=[
                CommandHandler("period", period_command),
                CallbackQueryHandler(period_command, pattern='^period_menu$')
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
            allow_reentry=True,
            name="period_conversation"
        )

        # ============================================================
        # ДОБАВИТЬ ВАЖНО: Общий MessageHandler для ВСЕХ текстовых сообщений
        # должен быть ПОСЛЕ ConversationHandler!
        # ============================================================
        async def handle_all_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Обработчик ВСЕХ текстовых сообщений, не попавших в ConversationHandler"""
            user = update.effective_user

            # Логируем полученное сообщение
            logger.info(f"Получено сообщение от {user.id} ({user.username}): {update.message.text[:50]}...")

            # Если пользователь не в ConversationHandler, предлагаем помощь
            await update.message.reply_text(
                "🤔 *Я не понял ваше сообщение.*\n\n"
                "Доступные команды:\n"
                "/start - Главное меню\n"
                "/today - Статистика за сегодня\n"
                "/token - Управление токеном\n"
                "/help - Справка\n\n"
                "Или используйте кнопки в меню.",
                parse_mode='Markdown'
            )

        # ============================================================
        # ВАЖНО: Порядок добавления обработчиков КРИТИЧЕН!
        # ============================================================

        # 1. Сначала добавляем ConversationHandler (они имеют приоритет)
        application.add_handler(token_conversation_handler)
        application.add_handler(period_conversation_handler)

        # 2. Затем добавляем команды
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("today", today_command))
        application.add_handler(CommandHandler("week", week_command))
        application.add_handler(CommandHandler("month", month_command))
        application.add_handler(CommandHandler("top", top_command))
        application.add_handler(CommandHandler("help", help_command))

        # 3. Обработчик кнопок
        application.add_handler(CallbackQueryHandler(button_handler))

        # 4. И только в САМОМ КОНЦЕ - общий обработчик текстовых сообщений
        # Это перехватит все сообщения, которые не обработаны выше
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_all_text_messages
        ))

        # Обработчик ошибок
        application.add_error_handler(error_handler)

        # Запускаем бота
        logger.info("Бот запущен. Ожидание команд...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка запуска бота: {e}", exc_info=True)


if __name__ == '__main__':
    main()