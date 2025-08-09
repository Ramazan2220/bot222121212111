#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для шифрования существующих паролей в БД
ЗАПУСКАТЬ ТОЛЬКО ОДИН РАЗ!
"""

import sys
import logging
from database.db_manager import get_session
from database.models import InstagramAccount
from utils.encryption import encryption

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def encrypt_existing_passwords():
    """Зашифровать все существующие пароли в БД"""
    session = get_session()
    
    try:
        # Получаем все аккаунты
        accounts = session.query(InstagramAccount).all()
        
        encrypted_count = 0
        skipped_count = 0
        
        for account in accounts:
            # Проверяем, не зашифрован ли уже пароль
            if account.password and not account.password.startswith('gAAAAA'):
                try:
                    # Шифруем пароль
                    encrypted_password = encryption.encrypt_password(account.password)
                    account.password = encrypted_password
                    encrypted_count += 1
                    logger.info(f"✅ Зашифрован пароль для {account.username}")
                except Exception as e:
                    logger.error(f"❌ Ошибка шифрования для {account.username}: {e}")
            else:
                skipped_count += 1
        
        # Сохраняем изменения
        session.commit()
        
        logger.info(f"\n📊 Результаты:")
        logger.info(f"✅ Зашифровано: {encrypted_count}")
        logger.info(f"⏭️ Пропущено: {skipped_count}")
        logger.info(f"📁 Всего аккаунтов: {len(accounts)}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    print("⚠️  ВНИМАНИЕ: Этот скрипт зашифрует все пароли в БД!")
    print("⚠️  Перед запуском сделайте резервную копию БД!")
    response = input("\nПродолжить? (yes/no): ")
    
    if response.lower() == 'yes':
        encrypt_existing_passwords()
    else:
        print("❌ Операция отменена") 