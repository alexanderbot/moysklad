from telegram import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from database import init_database
from config import config
from security import security


def get_main_menu(telegram_id: int = None):
    """
    Главное меню бота
    Если передан telegram_id - проверяем регистрацию пользователя
    """
    # Если передан ID пользователя, проверяем его статус
    if telegram_id:
        try:
            db = init_database(config.DB_PATH)
            user_data = db.get_user(telegram_id)

            if user_data and user_data.get('api_token_encrypted'):
                # Пользователь зарегистрирован - показываем основное меню
                keyboard = [
                    [KeyboardButton("📊 Отчеты"), KeyboardButton("📈 Аналитика")],
                    [KeyboardButton("⚙️ Настройки"), KeyboardButton("ℹ️ Помощь")]
                ]
            else:
                # Пользователь не зарегистрирован - показываем кнопку регистрации
                keyboard = [
                    [KeyboardButton("📱 Регистрация")],
                    [KeyboardButton("ℹ️ Помощь")]
                ]
        except Exception as e:
            # В случае ошибки показываем меню по умолчанию
            print(f"Ошибка при получении меню: {e}")
            keyboard = [
                [KeyboardButton("📱 Регистрация")],
                [KeyboardButton("📊 Отчеты"), KeyboardButton("📈 Аналитика")],
                [KeyboardButton("⚙️ Настройки"), KeyboardButton("ℹ️ Помощь")]
            ]
    else:
        # Если ID не передан, показываем меню по умолчанию
        keyboard = [
            [KeyboardButton("📱 Регистрация")],
            [KeyboardButton("📊 Отчеты"), KeyboardButton("📈 Аналитика")],
            [KeyboardButton("⚙️ Настройки"), KeyboardButton("ℹ️ Помощь")]
        ]

    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_dynamic_main_menu(db, telegram_id: int):
    """
    Альтернативная версия с передачей объекта базы данных
    """
    user_data = db.get_user(telegram_id)

    if user_data and user_data.get('api_token_encrypted'):
        # Пользователь зарегистрирован
        keyboard = [
            [KeyboardButton("📊 Отчеты"), KeyboardButton("📈 Аналитика")],
            [KeyboardButton("⚙️ Настройки"), KeyboardButton("ℹ️ Помощь")]
        ]
    else:
        # Пользователь не зарегистрирован
        keyboard = [
            [KeyboardButton("📱 Регистрация")],
            [KeyboardButton("ℹ️ Помощь")]
        ]

    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_phone_keyboard():
    """Клавиатура для запроса номера телефона"""
    keyboard = [[KeyboardButton("📱 Поделиться номером", request_contact=True)]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)


def get_report_keyboard():
    """Клавиатура для выбора отчетов"""
    keyboard = [
        [KeyboardButton("📅 Сегодня"), KeyboardButton("📆 Неделя")],
        [KeyboardButton("📈 Месяц"), KeyboardButton("🗓 Произвольный период")],
        [KeyboardButton("🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_settings_keyboard():
    """Клавиатура настроек"""
    keyboard = [
        [KeyboardButton("🔑 Установить API-токен")],
        [KeyboardButton("🔄 Обновить токен"), KeyboardButton("📊 Настройки отчетов")],
        [KeyboardButton("🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_back_keyboard():
    """Простая кнопка назад"""
    keyboard = [[KeyboardButton("🔙 Назад")]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_analytics_keyboard():
    """Клавиатура для аналитики"""
    keyboard = [
        [KeyboardButton("📊 Сравнить периоды")],
        [KeyboardButton("📈 Сегодня vs Вчера"), KeyboardButton("📅 Год назад")],
        [KeyboardButton("📆 Неделя vs Прошлая"), KeyboardButton("📊 Месяц vs Прошлый")],
        [KeyboardButton("🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)