#!/usr/bin/env python3
"""
ОТЛАДКА USERSERVICE
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def test_userservice():
    print("🔍 ОТЛАДКА USERSERVICE")
    print("=" * 80)
    
    user_id = 6626270112
    
    try:
        # Проверяем UserService напрямую
        print("\n1️⃣ USERSERVICE В ПАМЯТИ:")
        from services.user_service import UserService
        user_service = UserService()
        
        user = user_service.get_user(user_id)
        if user:
            print(f"✅ Пользователь найден в памяти UserService:")
            print(f"   Username: {user.username}")
            print(f"   Status: {user.status}")
        else:
            print(f"❌ Пользователь НЕ найден в памяти UserService")
        
        all_users = user_service.get_all_users()
        print(f"📊 Всего пользователей в UserService: {len(all_users)}")
        user_ids = [u.telegram_id for u in all_users]
        print(f"📄 ID пользователей: {user_ids}")
        
        # Проверяем файл напрямую
        print(f"\n2️⃣ ФАЙЛ admin_bot/data/users.json:")
        userservice_file = "admin_bot/data/users.json"
        if os.path.exists(userservice_file):
            with open(userservice_file, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
                
            user_key = str(user_id)
            if user_key in file_data:
                user_data = file_data[user_key]
                print(f"✅ Пользователь найден в файле:")
                print(f"   Username: {user_data.get('username', 'N/A')}")
                print(f"   Status: {user_data.get('status', 'N/A')}")
            else:
                print(f"❌ Пользователь НЕ найден в файле")
                
            print(f"📊 Всего пользователей в файле: {len(file_data)}")
            print(f"📄 ID пользователей в файле: {list(file_data.keys())}")
        else:
            print(f"❌ Файл {userservice_file} не существует")
            
        # Тестируем удаление
        print(f"\n3️⃣ ТЕСТИРУЕМ УДАЛЕНИЕ:")
        
        # Добавляем пользователя
        if not user:
            print("➕ Добавляем пользователя...")
            user = user_service.create_user(user_id, "test_user")
            user_service.update_user(user)
            user_service.save_users()
            print("✅ Пользователь добавлен")
        
        # Проверяем что он есть
        user = user_service.get_user(user_id)
        print(f"📊 После добавления: пользователь {'найден' if user else 'НЕ найден'}")
        
        # Удаляем
        print("🗑️ Удаляем пользователя...")
        result = user_service.delete_user(user_id)
        print(f"📊 delete_user() вернул: {result}")
        
        # Проверяем в памяти
        user = user_service.get_user(user_id)
        print(f"📊 В памяти после удаления: пользователь {'найден' if user else 'НЕ найден'}")
        
        # Проверяем в файле
        with open(userservice_file, 'r', encoding='utf-8') as f:
            file_data = json.load(f)
        user_key = str(user_id)
        in_file = user_key in file_data
        print(f"📊 В файле после удаления: пользователь {'найден' if in_file else 'НЕ найден'}")
        
        # Пересоздаем UserService
        print(f"\n4️⃣ ПЕРЕСОЗДАЕМ USERSERVICE:")
        new_user_service = UserService()
        user = new_user_service.get_user(user_id)
        print(f"📊 В новом UserService: пользователь {'найден' if user else 'НЕ найден'}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_userservice()

if __name__ == "__main__":
    main() 