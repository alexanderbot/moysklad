import logging
from telegram import Update, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler, MessageHandler, filters, CommandHandler
from telegram.constants import ParseMode

from moysklad_api import MoyskladAPI, get_period_dates, AnalyticsCalculator, MoyskladReport
from security import security
import asyncio
from datetime import datetime, timedelta

from database import Database
from security import security
from keyboards import (
    get_main_menu,
    get_phone_keyboard,
    get_report_keyboard,
    get_settings_keyboard,
    get_back_keyboard,
    get_analytics_keyboard
)

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
REGISTRATION, API_TOKEN = range(2)


class AuthHandlers:
    """Обработчики аутентификации"""

    def __init__(self, db: Database):
        self.db = db

    async def start_auth(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало процесса регистрации"""
        user = update.effective_user
        logger.info(f"Начало регистрации для пользователя: {user.id}")

        # Проверяем, зарегистрирован ли уже пользователь
        user_data = self.db.get_user(user.id)

        if user_data and user_data.get('phone_number') and user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "✅ Вы уже зарегистрированы!\n"
                "Используйте меню для работы с ботом.",
                reply_markup=get_main_menu(user.id)  # Используем динамическое меню
            )
            return ConversationHandler.END

        # Запрашиваем номер телефона
        await update.message.reply_text(
            "🔐 *Регистрация*\n\n"
            "Для доступа к статистике МойСклад необходимо:\n\n"
            "1. Предоставить номер телефона\n"
            "2. Указать API-токен из вашего аккаунта МойСклад\n\n"
            "Нажмите кнопку ниже, чтобы поделиться номером:",
            reply_markup=get_phone_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

        return REGISTRATION

    async def get_phone_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение номера телефона"""
        user = update.effective_user

        if update.message.contact:
            phone_number = update.message.contact.phone_number

            # Сохраняем пользователя с номером телефона
            user_id = self.db.add_user(user.id, phone_number)
            context.user_data['user_id'] = user_id
            context.user_data['phone'] = phone_number

            logger.info(f"Номер телефона получен для пользователя {user.id}: {phone_number}")

            # Запрашиваем API-токен
            await update.message.reply_text(
                f"✅ Номер телефона получен: *{phone_number}*\n\n"
                "📋 Теперь введите ваш *API-токен МойСклад*:\n\n"
                "1. Зайдите в МойСклад → Настройки → Безопасность\n"
                "2. Создайте новый токен или скопируйте существующий\n"
                "3. Вставьте его в чат\n\n"
                "⚠️ *Токен будет зашифрован и безопасно сохранен*",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )

            return API_TOKEN
        else:
            await update.message.reply_text(
                "❌ Пожалуйста, используйте кнопку 'Поделиться номером'",
                reply_markup=get_phone_keyboard()
            )
            return REGISTRATION

    async def get_api_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение и сохранение API-токена"""
        user = update.effective_user
        api_token = update.message.text.strip()

        # Базовая валидация токена
        if len(api_token) < 20:
            await update.message.reply_text(
                "❌ Токен слишком короткий. Пожалуйста, проверьте правильность токена и введите его снова:"
            )
            return API_TOKEN

        try:
            # Шифруем токен
            encrypted_token = security.encrypt(api_token)

            # Сохраняем в базе данных
            success = self.db.update_user_token(user.id, encrypted_token)

            if success:
                logger.info(f"API-токен сохранен для пользователя {user.id}")

                await update.message.reply_text(
                    "🎉 *Регистрация завершена!*\n\n"
                    "✅ Ваш API-токен успешно сохранен в зашифрованном виде\n"
                    "✅ Теперь вы можете получать статистику из МойСклад\n\n"
                    "Используйте меню для работы с отчетами:",
                    reply_markup=get_main_menu(user.id),  # Динамическое меню для зарегистрированного
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    "❌ Ошибка сохранения токена. Попробуйте снова:"
                )
                return API_TOKEN

        except Exception as e:
            logger.error(f"Ошибка сохранения токена: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при сохранении токена. Попробуйте снова:"
            )
            return API_TOKEN

        return ConversationHandler.END

    async def cancel_registration(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена регистрации"""
        user = update.effective_user
        await update.message.reply_text(
            "❌ Регистрация отменена.",
            reply_markup=get_main_menu(user.id)  # Динамическое меню
        )
        return ConversationHandler.END

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать настройки"""
        user = update.effective_user
        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "❌ Вы еще не зарегистрированы. Используйте /start для регистрации.",
                reply_markup=get_main_menu()
            )
            return

        phone_number = user_data.get('phone_number', 'Не указан')
        created_at = user_data.get('created_at', 'Неизвестно')

        text = (
            f"⚙️ *Ваши настройки*\n\n"
            f"📱 Телефон: `{phone_number}`\n"
            f"📅 Зарегистрирован: `{created_at}`\n"
            f"🔐 API-токен: {'✅ Сохранен' if user_data.get('api_token_encrypted') else '❌ Отсутствует'}\n\n"
            f"Используйте кнопки ниже для управления:"
        )

        await update.message.reply_text(
            text,
            reply_markup=get_settings_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    async def update_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обновление API-токена"""
        user = update.effective_user

        await update.message.reply_text(
            "🔑 *Обновление API-токена*\n\n"
            "Введите новый API-токен МойСклад:\n\n"
            "1. Зайдите в МойСклад → Настройки → Безопасность\n"
            "2. Создайте новый токен\n"
            "3. Вставьте его в чат\n\n"
            "Для отмены нажмите /cancel",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.MARKDOWN
        )

        context.user_data['waiting_for_token'] = True
        return 'WAITING_TOKEN'

    async def process_token_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нового токена"""
        user = update.effective_user
        api_token = update.message.text.strip()

        if len(api_token) < 20:
            await update.message.reply_text(
                "❌ Токен слишком короткий. Пожалуйста, введите валидный API-токен:"
            )
            return 'WAITING_TOKEN'

        try:
            encrypted_token = security.encrypt(api_token)
            success = self.db.update_user_token(user.id, encrypted_token)

            if success:
                await update.message.reply_text(
                    "✅ API-токен успешно обновлен!",
                    reply_markup=get_main_menu()
                )
            else:
                await update.message.reply_text(
                    "❌ Ошибка обновления токена. Попробуйте позже.",
                    reply_markup=get_main_menu()
                )

        except Exception as e:
            logger.error(f"Ошибка обновления токена: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте позже.",
                reply_markup=get_main_menu()
            )

        context.user_data.pop('waiting_for_token', None)
        return ConversationHandler.END


class MenuHandlers:
    """Обработчики меню"""

    def __init__(self, db: Database):
        self.db = db

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню"""
        user = update.effective_user

        await update.message.reply_text(
            "📱 *Главное меню*\n\n"
            "Выберите нужный раздел:",
            reply_markup=get_main_menu(user.id),  # Динамическое меню
            parse_mode=ParseMode.MARKDOWN
        )

    async def show_reports_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню отчетов"""
        user = update.effective_user
        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "❌ Сначала необходимо зарегистрироваться и указать API-токен.\n"
                "Используйте /start для регистрации.",
                reply_markup=get_main_menu()
            )
            return

        await update.message.reply_text(
            "📊 *Отчеты*\n\n"
            "Выберите период для получения статистики:",
            reply_markup=get_report_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    async def show_analytics_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню аналитики"""
        user = update.effective_user
        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "❌ Сначала необходимо зарегистрироваться и указать API-токен.\n"
                "Используйте /start для регистрации.",
                reply_markup=get_main_menu()
            )
            return

        await update.message.reply_text(
            "📈 *Аналитика*\n\n"
            "Сравнение периодов и детальная аналитика:",
            reply_markup=get_analytics_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кнопки Назад"""
        user = update.effective_user
        await update.message.reply_text(
            "📱 *Главное меню*\n\n"
            "Выберите нужный раздел:",
            reply_markup=get_main_menu(user.id),  # Динамическое меню
            parse_mode=ParseMode.MARKDOWN
        )

    async def get_today_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отчет за сегодня"""
        await self._get_report(update, context, 'today')

    async def get_week_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отчет за неделю"""
        await self._get_report(update, context, 'week')

    async def get_month_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отчет за месяц"""
        await self._get_report(update, context, 'month')

    async def get_yesterday_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отчет за вчера"""
        await self._get_report(update, context, 'yesterday')

    async def _get_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE, period_type: str):
        """Общий метод для получения отчетов"""
        user = update.effective_user
        logger.info(f"🔄 Запрос отчета '{period_type}' от пользователя {user.id}")

        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            logger.warning(f"❌ Пользователь {user.id} не зарегистрирован")
            await update.message.reply_text(
                "❌ Сначала необходимо зарегистрироваться и указать API-токен.\n"
                "Используйте /start для регистрации.",
                reply_markup=get_main_menu()
            )
            return

        # Получаем и расшифровываем токен
        encrypted_token = user_data['api_token_encrypted']
        api_token = security.decrypt(encrypted_token)

        logger.info(f"🔑 Токен пользователя {user.id}: {api_token[:15]}...")

        if not api_token:
            logger.error(f"❌ Ошибка расшифровки токена для пользователя {user.id}")
            await update.message.reply_text(
                "❌ Ошибка расшифровки токена. Пожалуйста, обновите API-токен в настройках.",
                reply_markup=get_settings_keyboard()
            )
            return

        # Показываем сообщение о загрузке
        loading_msg = await update.message.reply_text("⏳ Загружаем данные из МойСклад...")

        try:
            # Создаем API клиент
            api = MoyskladAPI(api_token)
            logger.info(f"📡 Создан API клиент для пользователя {user.id}")

            # Получаем даты периода
            date_from, date_to = get_period_dates(period_type)
            logger.info(f"📅 Период: {date_from} - {date_to}")

            # Получаем отчет
            logger.info(f"📊 Запрос отчета из МойСклад...")
            report = api.get_sales_report(date_from, date_to)

            if report:
                logger.info(f"✅ Отчет получен: {report.total_orders} заказов, {report.total_sales:.2f} руб.")

                # Логируем запрос
                self.db.log_request(user_data['id'], period_type, f"{date_from} - {date_to}")
                logger.info(f"📝 Запрос залогирован в БД")

                # Отправляем отчет
                report_text = report.format_report()
                await update.message.reply_text(
                    report_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_report_keyboard()
                )
                logger.info(f"📨 Отчет отправлен пользователю {user.id}")

                # Если есть детали, показываем топ-5 заказов
                if report.details and len(report.details) > 0:
                    details_text = "📋 *Последние заказы:*\n\n"
                    for i, detail in enumerate(report.details[:5], 1):
                        order_date = detail.get('created', '')[:10]
                        order_sum = detail.get('sum', 0)
                        details_text += f"{i}. {detail.get('name', '')}\n"
                        details_text += f"   💰 {order_sum:,.2f} ₽ | 📅 {order_date}\n"
                        details_text += f"   📊 Статус: {detail.get('state', 'Не указан')}\n\n"

                    await update.message.reply_text(
                        details_text,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logger.info(f"📋 Детали заказов отправлены")
            else:
                logger.warning(f"⚠️ Отчет не получен для пользователя {user.id}")
                await update.message.reply_text(
                    "❌ Не удалось получить данные из МойСклад.\n"
                    "Возможные причины:\n"
                    "• Нет данных за выбранный период\n"
                    "• Проблемы с подключением\n"
                    "• Ошибка API\n\n"
                    "Попробуйте другой период или проверьте настройки.",
                    reply_markup=get_report_keyboard()
                )

        except Exception as e:
            logger.error(f"❌ Ошибка при получении отчета: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Произошла ошибка при получении данных.\n\n"
                f"Ошибка: {str(e)[:100]}",
                reply_markup=get_report_keyboard()
            )

        finally:
            # Удаляем сообщение о загрузке
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=loading_msg.message_id
                )
                logger.info(f"🗑 Сообщение о загрузке удалено")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось удалить сообщение о загрузке: {e}")

    async def compare_today_yesterday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сравнение сегодня vs вчера"""
        await self._compare_periods(update, context, 'today', 'yesterday')

    async def compare_week(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сравнение этой недели с прошлой"""
        await self._compare_periods(update, context, 'week', 'last_week')

    async def compare_month(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сравнение этого месяца с прошлым"""
        await self._compare_periods(update, context, 'month', 'last_month')

    async def _compare_periods(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                               current_period: str, previous_period: str):
        """Общий метод для сравнения периодов"""
        user = update.effective_user
        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "❌ Сначала необходимо зарегистрироваться и указать API-токен.",
                reply_markup=get_main_menu()
            )
            return

        # Получаем и расшифровываем токен
        encrypted_token = user_data['api_token_encrypted']
        api_token = security.decrypt(encrypted_token)

        if not api_token:
            await update.message.reply_text(
                "❌ Ошибка расшифровки токена. Обновите API-токен.",
                reply_markup=get_settings_keyboard()
            )
            return

        loading_msg = await update.message.reply_text("⏳ Сравниваем периоды...")

        try:
            api = MoyskladAPI(api_token)

            # Получаем отчеты за оба периода
            curr_from, curr_to = get_period_dates(current_period)
            prev_from, prev_to = get_period_dates(previous_period)

            current_report = api.get_sales_report(curr_from, curr_to)
            previous_report = api.get_sales_report(prev_from, prev_to)

            if current_report and previous_report:
                # Сравниваем отчеты
                comparison = AnalyticsCalculator.compare_reports(current_report, previous_report)

                await update.message.reply_text(
                    comparison,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_analytics_keyboard()
                )

                # Логируем запрос
                self.db.log_request(
                    user_data['id'],
                    f'compare_{current_period}_{previous_period}',
                    f"current: {curr_from}-{curr_to}, previous: {prev_from}-{prev_to}"
                )
            else:
                error_msg = "❌ Не удалось получить данные для сравнения.\n"
                if not current_report:
                    error_msg += f"- Нет данных за текущий период ({curr_from} - {curr_to})\n"
                if not previous_report:
                    error_msg += f"- Нет данных за предыдущий период ({prev_from} - {prev_to})\n"

                await update.message.reply_text(
                    error_msg,
                    reply_markup=get_analytics_keyboard()
                )

        except Exception as e:
            logger.error(f"Ошибка при сравнении периодов: {e}")
            await update.message.reply_text(
                f"❌ Ошибка: {str(e)[:100]}",
                reply_markup=get_analytics_keyboard()
            )

        finally:
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=loading_msg.message_id
                )
            except:
                pass

    async def ask_custom_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос произвольного периода"""
        await update.message.reply_text(
            "🗓 *Произвольный период*\n\n"
            "Введите период в формате:\n"
            "`ДД.ММ.ГГГГ - ДД.ММ.ГГГГ`\n\n"
            "Пример: `01.01.2024 - 31.01.2024`\n\n"
            "Или введите одну дату для отчета за день: `01.01.2024`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_keyboard()
        )

        return 'WAITING_CUSTOM_PERIOD'

    async def process_custom_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка произвольного периода"""
        user_input = update.message.text.strip()

        try:
            if ' - ' in user_input:
                # Диапазон дат
                date1_str, date2_str = user_input.split(' - ')
                date1 = datetime.strptime(date1_str.strip(), '%d.%m.%Y')
                date2 = datetime.strptime(date2_str.strip(), '%d.%m.%Y')

                # Убедимся, что первая дата раньше второй
                if date1 > date2:
                    date1, date2 = date2, date1

                date_from = date1.strftime('%Y-%m-%d')
                date_to = date2.strftime('%Y-%m-%d')
                period_name = f"{date1_str} - {date2_str}"

            else:
                # Одна дата
                date = datetime.strptime(user_input.strip(), '%d.%m.%Y')
                date_from = date_to = date.strftime('%Y-%m-%d')
                period_name = user_input

            # Сохраняем период в context
            context.user_data['custom_period'] = {
                'date_from': date_from,
                'date_to': date_to,
                'period_name': period_name
            }

            # Запрашиваем отчет
            await self._get_custom_report(update, context)

            return ConversationHandler.END

        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат даты.\n"
                "Используйте формат: `ДД.ММ.ГГГГ - ДД.ММ.ГГГГ`\n"
                "Пример: `01.01.2024 - 31.01.2024`",
                parse_mode=ParseMode.MARKDOWN
            )
            return 'WAITING_CUSTOM_PERIOD'

    async def _get_custom_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение отчета за произвольный период"""
        user = update.effective_user
        user_data = self.db.get_user(user.id)

        if not user_data or not user_data.get('api_token_encrypted'):
            await update.message.reply_text(
                "❌ Сначала необходимо зарегистрироваться.",
                reply_markup=get_main_menu()
            )
            return

        period_data = context.user_data.get('custom_period', {})
        if not period_data:
            await update.message.reply_text(
                "❌ Ошибка: период не указан.",
                reply_markup=get_report_keyboard()
            )
            return

        encrypted_token = user_data['api_token_encrypted']
        api_token = security.decrypt(encrypted_token)

        if not api_token:
            await update.message.reply_text(
                "❌ Ошибка расшифровки токена.",
                reply_markup=get_settings_keyboard()
            )
            return

        loading_msg = await update.message.reply_text("⏳ Загружаем данные...")

        try:
            api = MoyskladAPI(api_token)
            report = api.get_sales_report(
                period_data['date_from'],
                period_data['date_to']
            )

            if report:
                # Переопределяем период для отображения
                report.period = period_data['period_name']
                report_text = report.format_report()

                await update.message.reply_text(
                    report_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_report_keyboard()
                )

                # Логируем запрос
                self.db.log_request(
                    user_data['id'],
                    'custom_period',
                    f"{period_data['date_from']} - {period_data['date_to']}"
                )
            else:
                await update.message.reply_text(
                    "❌ Нет данных за выбранный период.",
                    reply_markup=get_report_keyboard()
                )

        except Exception as e:
            logger.error(f"Ошибка при получении кастомного отчета: {e}")
            await update.message.reply_text(
                f"❌ Ошибка: {str(e)[:100]}",
                reply_markup=get_report_keyboard()
            )

        finally:
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=loading_msg.message_id
                )
            except:
                pass