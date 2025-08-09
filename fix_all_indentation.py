#!/usr/bin/env python3
"""
Скрипт для исправления всех отступов в проекте
"""

import os

def fix_file_indentation(file_path):
    """Исправляет отступы в конкретном файле"""
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        fixed = False
        
        for i, line in enumerate(lines):
            # Исправляем основные проблемы с отступами
            
            # profile_manager.py строка 220
            if 'return True, "Ссылка профиля успешно обновлена"' in line and not line.startswith('                '):
                lines[i] = '                return True, "Ссылка профиля успешно обновлена"\n'
                print(f"Исправлена строка {i+1} в {file_path}: return True")
                fixed = True
            
            # client.py строки с proxy
            if 'proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"' in line and not line.startswith('                        '):
                lines[i] = '                        proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"\n'
                print(f"Исправлена строка {i+1} в {file_path}: proxy_url basic")
                fixed = True
            
            if 'if (proxy.username and proxy.password' in line and not line.startswith('                        '):
                lines[i] = line.replace(line[:line.index('if')], '                        ')
                print(f"Исправлена строка {i+1} в {file_path}: if proxy.username")
                fixed = True
            
            if 'proxy_url = f"{proxy.protocol}://{proxy.username}' in line and not line.startswith('                            '):
                lines[i] = line.replace(line[:line.index('proxy_url')], '                            ')
                print(f"Исправлена строка {i+1} в {file_path}: proxy_url auth")
                fixed = True
        
        if fixed:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"✅ Отступы в {file_path} исправлены!")
        
        return fixed
    
    except Exception as e:
        print(f"❌ Ошибка при обработке {file_path}: {e}")
        return False

def fix_all_indentation():
    """Исправляет отступы во всех Python файлах проекта"""
    
    files_to_fix = [
        'instagram/client.py',
        'instagram/profile_manager.py',
    ]
    
    total_fixed = 0
    
    for file_path in files_to_fix:
        if os.path.exists(file_path):
            if fix_file_indentation(file_path):
                total_fixed += 1
        else:
            print(f"⚠️ Файл {file_path} не найден")
    
    print(f"\n🎯 Исправлено файлов: {total_fixed}")
    print("✅ Все отступы исправлены! Можете запускать python main.py")

if __name__ == "__main__":
    fix_all_indentation() 