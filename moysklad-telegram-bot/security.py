import base64
from cryptography.fernet import Fernet
import os

# Импортируем конфигурацию
try:
    from config import config
except ImportError:
    # Создаем минимальную конфигурацию, если config.py не найден
    class SimpleConfig:
        ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")


    config = SimpleConfig()


class SecurityManager:
    """Менеджер безопасности для шифрования данных"""

    def __init__(self, encryption_key: str = None):
        self.encryption_key = encryption_key or config.ENCRYPTION_KEY
        print(f"🔐 Инициализация SecurityManager с ключом: {self.encryption_key[:20]}...")
        self.fernet = self._init_fernet()

    def _init_fernet(self):
        """Инициализация Fernet с ключом"""
        try:
            print(f"🔑 Попытка инициализации Fernet с ключом длиной {len(self.encryption_key)}")

            # Проверяем, является ли ключ валидным Fernet ключом
            if not self.encryption_key:
                print("❌ Ключ шифрования отсутствует")
                return self._generate_new_key()

            if len(self.encryption_key) != 44:
                print(f"⚠️  Ключ имеет неверную длину: {len(self.encryption_key)} (должно быть 44)")
                return self._generate_new_key()

            # Декодируем ключ из base64
            try:
                key = base64.urlsafe_b64decode(self.encryption_key)
                print(f"✅ Ключ успешно декодирован из base64, длина: {len(key)} байт")

                if len(key) != 32:
                    print(f"❌ Декодированный ключ не 32 байта: {len(key)}")
                    return self._generate_new_key()

                fernet = Fernet(self.encryption_key.encode())
                print("✅ Fernet успешно инициализирован")
                return fernet

            except Exception as decode_error:
                print(f"❌ Ошибка декодирования ключа: {decode_error}")
                return self._generate_new_key()

        except Exception as e:
            print(f"❌ Ошибка инициализации шифрования: {e}")
            return self._generate_new_key()

    def _generate_new_key(self):
        """Генерация нового ключа шифрования"""
        print("🔐 Генерация нового ключа шифрования...")
        key = Fernet.generate_key()
        self.encryption_key = key.decode()

        print(f"📋 Новый ключ (сохраните в .env):")
        print(f"ENCRYPTION_KEY={self.encryption_key}")

        return Fernet(key)

    def encrypt(self, data: str) -> str:
        """Шифрование данных"""
        print(f"🔒 Шифрование данных длиной {len(data)}")
        try:
            if not data:
                print("⚠️  Пустые данные для шифрования")
                return ""

            encrypted = self.fernet.encrypt(data.encode())
            result = encrypted.decode()
            print(f"✅ Данные успешно зашифрованы, результат: {result[:50]}...")
            return result

        except Exception as e:
            print(f"❌ Ошибка шифрования: {e}")
            return data

    def decrypt(self, encrypted_data: str) -> str:
        """Расшифровка данных"""

        try:
            if not encrypted_data:
                print("⚠️  Пустые данные для дешифрования")
                return ""

            decrypted = self.fernet.decrypt(encrypted_data.encode())
            result = decrypted.decode()
            print(f"✅ Данные успешно расшифрованы, результат: {result[:50]}...")
            return result

        except Exception as e:
            print(f"❌ Ошибка расшифровки: {e}")
            print(f"   Тип ошибки: {type(e).__name__}")
            return ""

    def hash_phone(self, phone_number: str) -> str:
        """Хеширование номера телефона"""
        import hashlib
        # Убираем все нецифровые символы
        clean_phone = ''.join(filter(str.isdigit, phone_number))
        # Хешируем с солью
        salt = os.getenv("HASH_SALT", "moysklad_bot_salt")
        return hashlib.sha256(f"{clean_phone}{salt}".encode()).hexdigest()


# Создаем глобальный экземпляр
security = SecurityManager()