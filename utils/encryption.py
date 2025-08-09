# -*- coding: utf-8 -*-
"""
Модуль шифрования для защиты паролей Instagram аккаунтов
"""

import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging

logger = logging.getLogger(__name__)

class PasswordEncryption:
    """Класс для шифрования и дешифрования паролей"""
    
    def __init__(self):
        self.key = self._get_or_create_key()
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self):
        """Получить или создать ключ шифрования"""
        key_file = os.path.join(os.path.dirname(__file__), '..', '.encryption_key')
        
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            # Генерируем новый ключ
            key = Fernet.generate_key()
            
            # Сохраняем в файл с правильными правами доступа
            with open(key_file, 'wb') as f:
                f.write(key)
            
            # Устанавливаем права доступа только для владельца
            os.chmod(key_file, 0o600)
            
            logger.warning(f"⚠️ Создан новый ключ шифрования: {key_file}")
            logger.warning("❗ ВАЖНО: Сделайте резервную копию этого файла!")
            
            return key
    
    def encrypt_password(self, password: str) -> str:
        """Зашифровать пароль"""
        if not password:
            return ""
        
        try:
            encrypted = self.cipher.encrypt(password.encode())
            return base64.b64encode(encrypted).decode()
        except Exception as e:
            logger.error(f"Ошибка шифрования: {e}")
            raise
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """Расшифровать пароль"""
        if not encrypted_password:
            return ""
        
        try:
            encrypted_bytes = base64.b64decode(encrypted_password.encode())
            decrypted = self.cipher.decrypt(encrypted_bytes)
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Ошибка дешифрования: {e}")
            raise

# Глобальный экземпляр
encryption = PasswordEncryption() 