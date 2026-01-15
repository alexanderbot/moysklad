import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
from typing import List

# Загружаем переменные окружения
load_dotenv()


@dataclass
class Config:
    """Конфигурация приложения"""
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    DB_PATH: str = os.getenv("DB_PATH", "data/bot_database.db")
    ENCRYPTION_KEY: str = os.getenv("ENCRYPTION_KEY", "")

    # Настройки API МойСклад
    MOYSKLAD_API_BASE_URL: str = "https://api.moysklad.ru/api/remap/1.2"
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))

    # Настройки бота - используем field с default_factory для списка
    ADMIN_IDS: List[int] = field(default_factory=lambda:
    list(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else []
                                 )

    # Папки для данных
    DATA_DIR: str = "src/data"
    LOGS_DIR: str = "src/logs"

    def validate(self) -> bool:
        """Проверка обязательных настроек"""
        if not self.BOT_TOKEN:
            raise ValueError("BOT_TOKEN не установлен в .env файле")

        # Проверка ключа шифрования
        if not self.ENCRYPTION_KEY:
            print("⚠️  Внимание: ENCRYPTION_KEY не установлен. Создаю временный ключ...")
            self.ENCRYPTION_KEY = self._generate_temp_key()
        elif len(self.ENCRYPTION_KEY) != 44:  # Fernet ключ всегда 44 символа
            print(f"⚠️  Внимание: ENCRYPTION_KEY имеет неверную длину ({len(self.ENCRYPTION_KEY)} вместо 44)")
            print("Создаю временный ключ...")
            self.ENCRYPTION_KEY = self._generate_temp_key()

        return True

    def _generate_temp_key(self) -> str:
        """Генерация временного ключа шифрования"""
        from cryptography.fernet import Fernet
        key = Fernet.generate_key().decode()
        print(f"📋 Временный ключ (добавьте в .env):")
        print(f"ENCRYPTION_KEY={key}")
        return key

    def setup_dirs(self):
        """Создание необходимых директорий"""
        os.makedirs(self.DATA_DIR, exist_ok=True)
        os.makedirs(self.LOGS_DIR, exist_ok=True)

        # Создаем папку для базы данных, если нужно
        db_dir = os.path.dirname(self.DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)


# Создаем экземпляр конфигурации
config = Config()