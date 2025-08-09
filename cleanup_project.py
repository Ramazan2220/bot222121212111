#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для очистки проекта от мусора
"""

import os
import shutil
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Файлы для удаления
FILES_TO_DELETE = [
    # Fix скрипты (должны быть интегрированы)
    "fix_all_indentation.py",
    "fix_client_indentation.py",
    "fix_encoding.py",
    "fix_windows_logging.py",
    "fix_windows_simple.py",
    "fix_database_models.py",
    "fix_database_schema.py",
    "fix_database.py",
    "fix_db_quick.py",
    "fix_task_statuses.py",
    
    # Временные и тестовые файлы
    "test_profile_links.py",
    "test_parallel_profiles.py",
    "test_optimized_profiles.py",
    "demo_instagrapi_analytics.py",
    "detailed_post_analytics.py",
    
    # Старые бэкапы
    "telegram_bot/handlers/publish_handlers_backup_20250709_144115.py",
    "telegram_bot/handlers/publish_handlers_old.py",
    "telegram_bot/handlers/publish_handlers.py.backup",
    
    # Архивы (перемещаем в папку backups)
    "instagram_bot_vds.tar.gz",
    "instagram_bot_windows_vds.zip",
    
    # Дублирующие файлы миграций
    "migrate_database.py",  # Уже есть система миграций
    "migrate_follow_tables.py",
    "migrate_groups_tables.py",
    "recreate_follow_tables.py",
    
    # Устаревшие скрипты
    "reset_problematic_accounts.py",
    "quick_account_check.py",
    "single_proxy_analysis.py",
    "mobile_rotating_analysis.py",
    "check_accounts_detailed.py",
    "check_paths.py",
    "check_proxies.py",
    "check_account_statuses.py",
    "debug_email.py",
    "debug_proxies.py",
    "diagnose_proxiware.py",
    
    # Анализ файлы (должны быть в docs)
    "api_integration_plan.md",
    "proxiware_setup_guide.md",
    "proxy_recommendations.md",
]

# Директории для реорганизации
DIRECTORIES_TO_ORGANIZE = {
    "web-dashboard": "archive/web-dashboard-old",  # Старая версия
    "instagram-automation-dashboard": "archive/dashboard-v1",  # Еще одна версия
    "test_content": "archive/test_content",
    "working_accounts": "archive/working_accounts",
    "email_logs": "archive/email_logs",
}

# MD файлы для перемещения в docs
MD_FILES_TO_MOVE = [
    "AUTOFOLLOW_COMPLETE_GUIDE.md",
    "AUTOFOLLOW_MODULE.md", 
    "FINAL_FIXES_REPORT.md",
    "FINAL_INTEGRATION_REPORT.md",
    "FIXES_SUMMARY.md",
    "HIGHLIGHTS_SYSTEM_DESIGN.md",
    "INTEGRATION_SUMMARY.md",
    "INTEREST_WARMUP_DOCUMENTATION.md",
    "MEGA_UPLOAD_GUIDE.md",
    "PROFILE_SETUP_INTEGRATION.md",
    "REELS_FIXES_REPORT.md",
    "REELS_SYSTEM_DOCUMENTATION.md",
    "REELS_TESTING_GUIDE.md",
    "RESIDENTIAL_PROXY_SETUP.md",
    "SAFETY_LIMITS_REPORT.md",
    "SYSTEM_MONITORING_README.md",
    "TELEGRAM_BOT_AUDIT_REPORT.md",
    "TESTING_README.md",
    "TESTING_RESULTS_20250613.md",
    "UNIQUIFIER_README.md",
    "VDS_MIGRATION_GUIDE.md",
    "README_WEB_BOT.md",
]

def cleanup_project():
    """Очистить проект от мусора"""
    
    # Создаем необходимые директории
    os.makedirs("archive", exist_ok=True)
    os.makedirs("docs/guides", exist_ok=True)
    os.makedirs("docs/reports", exist_ok=True)
    os.makedirs("data/backups", exist_ok=True)
    
    deleted_files = 0
    moved_files = 0
    errors = 0
    
    # Удаляем ненужные файлы
    logger.info("🗑️ Удаление временных файлов...")
    for file in FILES_TO_DELETE:
        if os.path.exists(file):
            try:
                if file.endswith(('.tar.gz', '.zip')):
                    # Архивы перемещаем
                    shutil.move(file, f"archive/{os.path.basename(file)}")
                    logger.info(f"📦 Перемещен архив: {file} -> archive/")
                    moved_files += 1
                else:
                    os.remove(file)
                    logger.info(f"✅ Удален: {file}")
                    deleted_files += 1
            except Exception as e:
                logger.error(f"❌ Ошибка удаления {file}: {e}")
                errors += 1
    
    # Реорганизуем директории
    logger.info("\n📁 Реорганизация директорий...")
    for old_dir, new_dir in DIRECTORIES_TO_ORGANIZE.items():
        if os.path.exists(old_dir):
            try:
                os.makedirs(os.path.dirname(new_dir), exist_ok=True)
                shutil.move(old_dir, new_dir)
                logger.info(f"✅ Перемещено: {old_dir} -> {new_dir}")
                moved_files += 1
            except Exception as e:
                logger.error(f"❌ Ошибка перемещения {old_dir}: {e}")
                errors += 1
    
    # Перемещаем документацию
    logger.info("\n📚 Организация документации...")
    for md_file in MD_FILES_TO_MOVE:
        if os.path.exists(md_file):
            try:
                # Определяем подпапку
                if "GUIDE" in md_file or "README" in md_file or "DOCUMENTATION" in md_file:
                    dest = f"docs/guides/{md_file}"
                else:
                    dest = f"docs/reports/{md_file}"
                
                shutil.move(md_file, dest)
                logger.info(f"✅ Перемещено: {md_file} -> {dest}")
                moved_files += 1
            except Exception as e:
                logger.error(f"❌ Ошибка перемещения {md_file}: {e}")
                errors += 1
    
    # Итоги
    logger.info(f"\n📊 Результаты очистки:")
    logger.info(f"   🗑️ Удалено файлов: {deleted_files}")
    logger.info(f"   📁 Перемещено файлов/папок: {moved_files}")
    logger.info(f"   ❌ Ошибок: {errors}")
    
    # Создаем .gitignore для archive
    gitignore_content = """# Архивные файлы
*
!.gitignore
"""
    with open("archive/.gitignore", "w") as f:
        f.write(gitignore_content)
    
    logger.info("\n✅ Очистка завершена!")

if __name__ == "__main__":
    print("🧹 ОЧИСТКА ПРОЕКТА")
    print("Это действие:")
    print("- Удалит временные fix-скрипты")
    print("- Переместит старые версии в archive/")
    print("- Организует документацию в docs/")
    print("- Удалит дублирующие файлы")
    
    response = input("\nПродолжить? (yes/no): ")
    
    if response.lower() == 'yes':
        cleanup_project()
    else:
        print("❌ Операция отменена") 