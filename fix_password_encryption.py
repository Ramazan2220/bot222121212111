#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для исправления шифрования паролей
Принудительно зашифровывает все незашифрованные пароли
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts, update_instagram_account
from utils.encryption import encryption
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_password_encryption():
    """Исправить шифрование всех паролей"""
    
    accounts = get_instagram_accounts()
    logger.info(f"📊 Найдено аккаунтов: {len(accounts)}")
    
    encrypted_count = 0
    already_encrypted = 0
    errors = 0
    
    for account in accounts:
        if not account.password:
            logger.warning(f"⚠️ Аккаунт {account.username} без пароля")
            continue
            
        try:
            # Проверяем, зашифрован ли уже
            if account.password.startswith('gAAAAA'):
                already_encrypted += 1
                logger.debug(f"✅ {account.username} уже зашифрован")
                continue
            
            # Шифруем пароль
            encrypted = encryption.encrypt_password(account.password)
            
            # Проверяем что шифрование прошло успешно
            decrypted_test = encryption.decrypt_password(encrypted)
            if decrypted_test != account.password:
                logger.error(f"❌ Ошибка проверки шифрования для {account.username}")
                errors += 1
                continue
            
            # Обновляем в БД
            update_instagram_account(account.id, password=encrypted)
            encrypted_count += 1
            logger.info(f"🔐 Зашифрован пароль для {account.username}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка для {account.username}: {e}")
            errors += 1
    
    logger.info(f"""
📊 РЕЗУЛЬТАТ ИСПРАВЛЕНИЯ ШИФРОВАНИЯ:
✅ Зашифровано: {encrypted_count}
🔐 Уже было зашифровано: {already_encrypted}  
❌ Ошибки: {errors}
📱 Всего аккаунтов: {len(accounts)}
""")

if __name__ == "__main__":
    fix_password_encryption() 
# -*- coding: utf-8 -*-
"""
Скрипт для исправления шифрования паролей
Принудительно зашифровывает все незашифрованные пароли
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts, update_instagram_account
from utils.encryption import encryption
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_password_encryption():
    """Исправить шифрование всех паролей"""
    
    accounts = get_instagram_accounts()
    logger.info(f"📊 Найдено аккаунтов: {len(accounts)}")
    
    encrypted_count = 0
    already_encrypted = 0
    errors = 0
    
    for account in accounts:
        if not account.password:
            logger.warning(f"⚠️ Аккаунт {account.username} без пароля")
            continue
            
        try:
            # Проверяем, зашифрован ли уже
            if account.password.startswith('gAAAAA'):
                already_encrypted += 1
                logger.debug(f"✅ {account.username} уже зашифрован")
                continue
            
            # Шифруем пароль
            encrypted = encryption.encrypt_password(account.password)
            
            # Проверяем что шифрование прошло успешно
            decrypted_test = encryption.decrypt_password(encrypted)
            if decrypted_test != account.password:
                logger.error(f"❌ Ошибка проверки шифрования для {account.username}")
                errors += 1
                continue
            
            # Обновляем в БД
            update_instagram_account(account.id, password=encrypted)
            encrypted_count += 1
            logger.info(f"🔐 Зашифрован пароль для {account.username}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка для {account.username}: {e}")
            errors += 1
    
    logger.info(f"""
📊 РЕЗУЛЬТАТ ИСПРАВЛЕНИЯ ШИФРОВАНИЯ:
✅ Зашифровано: {encrypted_count}
🔐 Уже было зашифровано: {already_encrypted}  
❌ Ошибки: {errors}
📱 Всего аккаунтов: {len(accounts)}
""")

if __name__ == "__main__":
    fix_password_encryption() 
 
 
 