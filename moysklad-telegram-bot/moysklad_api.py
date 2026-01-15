import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class MoyskladReport:
    """Класс для хранения данных отчета МойСклад"""
    period: str
    total_sales: float  # Общая сумма продаж
    total_orders: int  # Количество заказов
    average_order: float  # Средний чек
    products_count: int  # Количество проданных товаров
    details: List[Dict]  # Детали по заказам/товарам

    def to_dict(self) -> Dict:
        """Преобразование в словарь"""
        return {
            'period': self.period,
            'total_sales': self.total_sales,
            'total_orders': self.total_orders,
            'average_order': self.average_order,
            'products_count': self.products_count,
            'details': self.details
        }

    def format_report(self) -> str:
        """Форматирование отчета для Telegram"""
        return (
            f"📊 *Отчет за {self.period}*\n\n"
            f"💰 *Общая сумма продаж:* {self.total_sales:,.2f} ₽\n"
            f"📦 *Количество заказов:* {self.total_orders}\n"
            f"🧮 *Средний чек:* {self.average_order:,.2f} ₽\n"
            f"📈 *Товаров продано:* {self.products_count}\n"
        )


class MoyskladAPI:
    """Класс для работы с API МойСклад"""

    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Выполнение запроса к API"""
        url = f"{self.BASE_URL}/{endpoint}"

        logger.info(f"🌐 Запрос к API: {endpoint}")
        logger.info(f"📋 Параметры запроса: {params}")

        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )

            logger.info(f"📡 Статус ответа: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Успешный ответ. Всего записей: {data.get('meta', {}).get('size', 'unknown')}")
                logger.info(f"📊 Возвращено записей: {len(data.get('rows', []))}")
                return data
            elif response.status_code == 401:
                logger.error(f"❌ Ошибка 401: Неверный API-токен")
                return None
            elif response.status_code == 400:
                logger.error(f"❌ Ошибка 400: Неверные параметры запроса")
                logger.error(f"📄 Ответ: {response.text[:500]}")
                return None
            else:
                logger.error(f"❌ Ошибка API: {response.status_code}")
                logger.error(f"📄 Ответ: {response.text[:500]}")
                return None

        except requests.exceptions.Timeout:
            logger.error("⏰ Таймаут при подключении к API МойСклад")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("🔌 Ошибка подключения к API МойСклад")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка: {e}")
            return None

    def get_sales_report(self, date_from: str, date_to: str) -> Optional[MoyskladReport]:
        """
        Получение отчета о продажах за период

        Args:
            date_from: Начальная дата в формате 'YYYY-MM-DD'
            date_to: Конечная дата в формате 'YYYY-MM-DD'
        """
        logger.info(f"📊 Запрос отчета продаж с {date_from} по {date_to}")

        # Параметры запроса - используем фильтр по created
        params = {
            "filter": f"created>={date_from} 00:00:00;created<={date_to} 23:59:59",
            "limit": 1000,
            "order": "created,desc"  # Сначала новые заказы
        }

        logger.info(f"📋 Параметры запроса: {params}")

        # Получаем заказы покупателей
        endpoint = "entity/customerorder"
        logger.info(f"🌐 Запрос к эндпоинту: {endpoint}")

        data = self._make_request(endpoint, params)

        if not data:
            logger.error("❌ Нет данных от API")
            return None

        if 'rows' not in data:
            logger.error(f"❌ Нет ключа 'rows' в ответе. Ответ: {json.dumps(data, ensure_ascii=False)[:200]}")
            return None

        orders = data['rows']
        logger.info(f"✅ Получено заказов: {len(orders)}")

        # Дополнительная фильтрация на нашей стороне (на всякий случай)
        filtered_orders = []
        for order in orders:
            created_date = order.get('created')
            if created_date:
                # Преобразуем дату из формата "2024-01-15 10:30:00"
                order_date = created_date[:10]  # Берем только YYYY-MM-DD
                if date_from <= order_date <= date_to:
                    filtered_orders.append(order)
                else:
                    logger.debug(f"Заказ {order.get('name')} с датой {order_date} вне периода")
            else:
                # Если нет даты создания, включаем в отчет
                filtered_orders.append(order)

        orders = filtered_orders
        logger.info(f"📅 После фильтрации по дате: {len(orders)} заказов")

        if len(orders) == 0:
            logger.warning(f"⚠️ Нет заказов за период {date_from} - {date_to}")
            # Возвращаем пустой отчет
            return MoyskladReport(
                period=f"{date_from} - {date_to}" if date_from != date_to else date_from,
                total_sales=0,
                total_orders=0,
                average_order=0,
                products_count=0,
                details=[]
            )

        total_sales = 0
        products_count = 0
        details = []

        logger.info(f"🔍 Обработка {len(orders)} заказов...")

        for order in orders:
            order_sum = order.get('sum', 0) / 100  # Сумма в копейках, переводим в рубли
            total_sales += order_sum

            # Получаем позиции заказа (опционально)
            order_id = order.get('id')
            order_name = order.get('name', f"Заказ {order_id[:8]}" if order_id else "Без номера")
            order_date = order.get('created', '')[:10] if order.get('created') else 'Неизвестно'

            # Логируем информацию о заказе для отладки
            logger.debug(f"Заказ: {order_name}, дата: {order_date}, сумма: {order_sum:.2f} ₽")

            # Получаем позиции заказа
            if order_id:
                positions = self.get_order_positions(order_id)
                if positions:
                    for pos in positions:
                        quantity = pos.get('quantity', 0)
                        products_count += quantity
                        logger.debug(
                            f"  Позиция: {pos.get('assortment', {}).get('name', 'Без названия')}, количество: {quantity}")

            # Добавляем детали заказа
            order_state = order.get('state', {}).get('name', 'Новый')

            details.append({
                'id': order_id,
                'name': order_name,
                'sum': order_sum,
                'created': order_date,
                'state': order_state
            })

        total_orders = len(orders)
        average_order = total_sales / total_orders if total_orders > 0 else 0

        logger.info(f"📈 Итоги: заказов={total_orders}, сумма={total_sales:.2f} ₽, товаров={int(products_count)}")

        # Формируем период для отчета
        period = f"{date_from} - {date_to}" if date_from != date_to else date_from

        return MoyskladReport(
            period=period,
            total_sales=total_sales,
            total_orders=total_orders,
            average_order=average_order,
            products_count=int(products_count),
            details=details[:10]  # Ограничиваем детали
        )

    def get_order_positions(self, order_id: str) -> List[Dict]:
        """Получение позиций заказа"""
        if not order_id:
            return []

        endpoint = f"entity/customerorder/{order_id}/positions"
        data = self._make_request(endpoint)

        if data and 'rows' in data:
            return data['rows']
        return []

    def get_detailed_sales_report(self, date_from: str, date_to: str) -> Optional[Dict]:
        """Получение детального отчета о продажах"""
        params = {
            "momentFrom": f"{date_from} 00:00:00",
            "momentTo": f"{date_to} 23:59:59"
        }

        # Используем отчет по продажам
        endpoint = "report/profit/byvariant"
        data = self._make_request(endpoint, params)

        return data

    def get_stock_report(self) -> Optional[Dict]:
        """Получение отчета по остаткам"""
        endpoint = "report/stock/all"
        data = self._make_request(endpoint)
        return data

    def validate_token(self) -> bool:
        """Проверка валидности API-токена"""
        try:
            response = requests.get(
                f"{self.BASE_URL}/entity/counterparty",
                headers=self.headers,
                params={"limit": 1},
                timeout=10
            )
            return response.status_code == 200
        except:
            return False


def get_period_dates(period_type: str) -> tuple:
    """
    Получение дат начала и конца периода

    Args:
        period_type: 'today', 'week', 'month', 'yesterday'

    Returns:
        tuple: (date_from, date_to) в формате 'YYYY-MM-DD'
    """
    today = datetime.now().date()

    if period_type == 'today':
        date_from = date_to = today.strftime('%Y-%m-%d')

    elif period_type == 'yesterday':
        yesterday = today - timedelta(days=1)
        date_from = date_to = yesterday.strftime('%Y-%m-%d')

    elif period_type == 'week':
        # Текущая неделя (понедельник - воскресенье)
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        date_from = start_of_week.strftime('%Y-%m-%d')
        date_to = end_of_week.strftime('%Y-%m-%d')

    elif period_type == 'month':
        # Текущий месяц
        date_from = today.replace(day=1).strftime('%Y-%m-%d')
        date_to = today.strftime('%Y-%m-%d')

    elif period_type == 'last_week':
        # Прошлая неделя
        today = datetime.now().date()
        start_of_last_week = today - timedelta(days=today.weekday() + 7)
        end_of_last_week = start_of_last_week + timedelta(days=6)
        date_from = start_of_last_week.strftime('%Y-%m-%d')
        date_to = end_of_last_week.strftime('%Y-%m-%d')

    elif period_type == 'last_month':
        # Прошлый месяц
        today = datetime.now().date()
        first_day_of_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        date_from = first_day_of_last_month.strftime('%Y-%m-%d')
        date_to = last_day_of_last_month.strftime('%Y-%m-%d')

    else:
        # По умолчанию сегодня
        date_from = date_to = today.strftime('%Y-%m-%d')

    return date_from, date_to


class AnalyticsCalculator:
    """Калькулятор для аналитики и сравнения периодов"""

    @staticmethod
    def calculate_growth(current: float, previous: float) -> Dict[str, Any]:
        """Расчет роста/падения показателей"""
        if previous == 0:
            return {
                'change': current,
                'percent': 100 if current > 0 else 0,
                'direction': 'up' if current > 0 else 'same'
            }

        change = current - previous
        percent = (change / abs(previous)) * 100

        return {
            'change': change,
            'percent': percent,
            'direction': 'up' if percent > 0 else 'down' if percent < 0 else 'same'
        }

    @staticmethod
    def compare_reports(current_report: MoyskladReport, previous_report: MoyskladReport) -> str:
        """Сравнение двух отчетов"""
        sales_growth = AnalyticsCalculator.calculate_growth(
            current_report.total_sales,
            previous_report.total_sales
        )

        orders_growth = AnalyticsCalculator.calculate_growth(
            current_report.total_orders,
            previous_report.total_orders
        )

        avg_order_growth = AnalyticsCalculator.calculate_growth(
            current_report.average_order,
            previous_report.average_order
        )

        products_growth = AnalyticsCalculator.calculate_growth(
            current_report.products_count,
            previous_report.products_count
        )

        # Форматируем результат
        result = (
            f"📊 *Сравнение периодов*\n\n"
            f"*Текущий период:* {current_report.period}\n"
            f"*Предыдущий период:* {previous_report.period}\n\n"

            f"💰 *Продажи:* {current_report.total_sales:,.2f} ₽\n"
            f"   Изменение: {sales_growth['change']:+,.2f} ₽ "
            f"({sales_growth['percent']:+.1f}%)\n\n"

            f"📦 *Заказы:* {current_report.total_orders}\n"
            f"   Изменение: {orders_growth['change']:+d} "
            f"({orders_growth['percent']:+.1f}%)\n\n"

            f"🧮 *Средний чек:* {current_report.average_order:,.2f} ₽\n"
            f"   Изменение: {avg_order_growth['change']:+,.2f} ₽ "
            f"({avg_order_growth['percent']:+.1f}%)\n\n"

            f"📈 *Товаров продано:* {current_report.products_count}\n"
            f"   Изменение: {products_growth['change']:+d} "
            f"({products_growth['percent']:+.1f}%)\n"
        )

        return result