import logging
import asyncio
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler
)

from config import config
from database import init_database
from handlers import AuthHandlers, MenuHandlers, REGISTRATION, API_TOKEN  # Добавляем константы
from keyboards import get_main_menu

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def start(update, context):
    """Обработчик команды /start"""
    user = update.effective_user

    # Проверяем, зарегистрирован ли пользователь
    try:
        from database import init_database
        from config import config

        db = init_database(config.DB_PATH)
        user_data = db.get_user(user.id)

        if user_data and user_data.get('api_token_encrypted'):
            # Пользователь зарегистрирован
            await update.message.reply_text(
                f"С возвращением, {user.first_name}! 👋\n\n"
                "Я бот для работы со статистикой МойСклад.\n\n"
                "Используйте меню для получения отчетов и аналитики.",
                reply_markup=get_main_menu(user.id)  # Динамическое меню
            )
        else:
            # Пользователь не зарегистрирован
            await update.message.reply_text(
                f"Привет, {user.first_name}! 👋\n\n"
                "Я бот для работы со статистикой МойСклад.\n\n"
                "Для начала работы необходимо:\n"
                "1. Зарегистрироваться по номеру телефона\n"
                "2. Указать ваш API-токен МойСклад\n\n"
                "Используйте меню для навигации.",
                reply_markup=get_main_menu(user.id)  # Динамическое меню
            )

    except Exception as e:
        logger.error(f"Ошибка при проверке регистрации: {e}")
        # В случае ошибки показываем стандартное приветствие
        await update.message.reply_text(
            f"Привет, {user.first_name}! 👋\n\n"
            "Я бот для работы со статистикой МойСклад.",
            reply_markup=get_main_menu()  # Меню по умолчанию
        )


async def help_command(update, context):
    """Обработчик команды /help"""
    help_text = """
📊 *Доступные команды:*

/start - Начать работу с ботом
/help - Получить справку
/settings - Настройки бота

📈 *Основные функции:*
• Отчет за сегодня
• Отчет за неделю  
• Отчет за месяц
• Произвольный период
• Сравнение периодов

🔐 *Безопасность:*
Ваши API-токены хранятся в зашифрованном виде.

📱 *Регистрация:*
Нажмите кнопку "Регистрация" в меню
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')


def setup_handlers(application, db):
    """Настройка всех обработчиков"""
    auth = AuthHandlers(db)
    menu = MenuHandlers(db)

    # ConversationHandler для регистрации
    registration_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex('^(📱 Регистрация)$'), auth.start_auth)
        ],
        states={
            REGISTRATION: [
                MessageHandler(filters.CONTACT, auth.get_phone_number)
            ],
            API_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, auth.get_api_token)
            ]
        },
        fallbacks=[CommandHandler('cancel', auth.cancel_registration)]
    )

    # ConversationHandler для обновления токена
    token_update_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex('^(🔄 Обновить токен)$'), auth.update_token)
        ],
        states={
            'WAITING_TOKEN': [
                MessageHandler(filters.TEXT & ~filters.COMMAND, auth.process_token_update)
            ]
        },
        fallbacks=[CommandHandler('cancel', auth.cancel_registration)]
    )

    # ConversationHandler для произвольного периода
    custom_period_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex('^(🗓 Произвольный период)$'), menu.ask_custom_period)
        ],
        states={
            'WAITING_CUSTOM_PERIOD': [
                MessageHandler(filters.TEXT & ~filters.COMMAND, menu.process_custom_period)
            ]
        },
        fallbacks=[CommandHandler('cancel', auth.cancel_registration)]
    )


    # Основные обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("settings", auth.show_settings))

    application.add_handler(registration_handler)
    application.add_handler(token_update_handler)

    # Обработчики отчетов
    application.add_handler(MessageHandler(
        filters.Regex('^(📅 Сегодня)$'), menu.get_today_report
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(📆 Неделя)$'), menu.get_week_report
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(📈 Месяц)$'), menu.get_month_report
    ))

    # Обработчики аналитики
    application.add_handler(MessageHandler(
        filters.Regex('^(📈 Сегодня vs Вчера)$'), menu.compare_today_yesterday
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(📆 Неделя vs Прошлая)$'), menu.compare_week
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(📊 Месяц vs Прошлый)$'), menu.compare_month
    ))

    application.add_handler(custom_period_handler)

    # Обработчики кнопок меню
    application.add_handler(MessageHandler(
        filters.Regex('^(📊 Отчеты)$'), menu.show_reports_menu
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(📈 Аналитика)$'), menu.show_analytics_menu
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(⚙️ Настройки)$'), auth.show_settings
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(ℹ️ Помощь)$'), help_command
    ))
    application.add_handler(MessageHandler(
        filters.Regex('^(🔙 Назад)$'), menu.handle_back
    ))

    # Обработка кнопки установки токена
    application.add_handler(MessageHandler(
        filters.Regex('^(🔑 Установить API-токен)$'), auth.start_auth
    ))

    # Эхо-обработчик (на случай, если сообщение не обработано)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, menu.show_main_menu
    ))



def main():
    """Основная функция запуска бота"""
    # Проверка конфигурации
    try:
        config.validate()
        config.setup_dirs()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return

    # Инициализация базы данных
    db = init_database(config.DB_PATH)
    logger.info(f"Database initialized at {config.DB_PATH}")

    # Создание приложения бота
    application = Application.builder().token(config.BOT_TOKEN).build()

    # Настройка обработчиков
    setup_handlers(application, db)

    # Запуск бота
    logger.info("Bot starting...")
    application.run_polling(allowed_updates=None)


if __name__ == '__main__':
    main()