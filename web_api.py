#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Web API для интеграции веб-дашборда с Instagram Telegram Bot
"""

import os
import sys
import json
import logging
import threading
import concurrent.futures
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import requests
import tempfile

# Добавляем текущую директорию в путь Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ВАЖНО: Импортируем monkey patch ПЕРВЫМ для применения патчей уникализации устройства
from instagram.monkey_patch import *
from instagram.deep_patch import apply_deep_patch

# Применяем глубокий патч
apply_deep_patch()

# Импортируем модули проекта
from database.db_manager import (
    init_db, get_instagram_accounts, add_instagram_account, add_instagram_account_without_login,
    get_instagram_account, update_instagram_account, delete_instagram_account,
    get_proxies, add_proxy, get_proxy, update_proxy, delete_proxy,
    assign_proxy_to_account, bulk_add_instagram_accounts
)
from database.models import InstagramAccount, Proxy
from utils.user_logger import log_user_info, log_user_error

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Проверяем доступность модулей Instagram
try:
    from instagram.client import test_instagram_login_with_proxy
    INTEGRATION_AVAILABLE = True
    logger.info("✅ Интегрированный сервис аккаунтов подключен")
except ImportError as e:
    INTEGRATION_AVAILABLE = False
    logger.warning(f"⚠️ Интегрированный сервис недоступен: {e}")
except Exception as e:
    INTEGRATION_AVAILABLE = False
    logger.warning(f"⚠️ Ошибка инициализации интегрированного сервиса: {e}")

# Создаем Flask приложение
app = Flask(__name__)
CORS(app)  # Разрешаем CORS для всех доменов

# Инициализируем базу данных
init_db()

# Статические файлы веб-дашборда
@app.route('/')
def index():
    """Главная страница дашборда"""
    return send_from_directory('web-dashboard', 'index.html')

@app.route('/<path:filename>')
def static_files(filename):
    """Статические файлы дашборда"""
    return send_from_directory('web-dashboard', filename)

# =============================================================================
# API для работы с аккаунтами
# =============================================================================

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    """Получить список всех аккаунтов"""
    try:
        accounts = get_instagram_accounts()
        accounts_data = []
        
        for account in accounts:
            account_data = {
                'id': account.id,
                'username': account.username,
                'email': account.email,
                'full_name': account.full_name or '',
                'biography': account.biography or '',
                'is_active': account.is_active,
                'created_at': account.created_at.isoformat() if account.created_at else None,
                'updated_at': account.updated_at.isoformat() if account.updated_at else None,
                'proxy_id': account.proxy_id,
                'proxy': None
            }
            
            # Добавляем информацию о прокси, если назначен
            if account.proxy_id:
                proxy = get_proxy(account.proxy_id)
                if proxy:
                    account_data['proxy'] = {
                        'id': proxy.id,
                        'host': proxy.host,
                        'port': proxy.port,
                        'protocol': proxy.protocol
                    }
            
            accounts_data.append(account_data)
        
        return jsonify({
            'success': True,
            'data': accounts_data,
            'total': len(accounts_data)
        })
    
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунтов: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts', methods=['POST'])
def add_account():
    """Добавить новый аккаунт с полной интеграцией (асинхронно)"""
    try:
        data = request.get_json()
        
        # Проверяем обязательные поля
        required_fields = ['username', 'password']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({
                    'success': False,
                    'error': f'Поле {field} обязательно для заполнения'
                }), 400
        
        username = data['username']
        password = data['password']
        email = data.get('email', '')
        email_password = data.get('email_password', '')
        
        logger.info(f"🚀 Начинаем добавление аккаунта {username}")
        
        # Проверяем, существует ли уже аккаунт с таким именем
        existing_accounts = get_instagram_accounts()
        for acc in existing_accounts:
            if acc.username == username:
                return jsonify({
                    'success': False,
                    'error': f'Аккаунт с именем пользователя {username} уже существует'
                }), 400
        
        if INTEGRATION_AVAILABLE and (email and email_password):
            # Если есть данные почты, запускаем полную интеграцию асинхронно
            import threading
            import asyncio
            
            def background_add_account():
                """Фоновая обработка добавления аккаунта"""
                try:
                    # Создаем новый event loop для потока
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    async def run_add_account():
                        logger.info(f"🔄 Фоновая обработка аккаунта {username} начата")
                        
                        # Простая обработка без старого сервиса
                        success = True
                        message = "Аккаунт обработан"
                        account_data = None
                        
                        # Попытка входа в Instagram
                        from instagram.client import test_instagram_login_with_proxy
                        login_success = test_instagram_login_with_proxy(
                            account_id=account.id,
                            username=username,
                            password=password,
                            email=email,
                            email_password=email_password
                        )
                        
                        if login_success:
                            logger.info(f"✅ Успешный вход в Instagram для {username}")
                            # Активируем аккаунт
                            from database.db_manager import update_instagram_account
                            update_instagram_account(account.id, is_active=True)
                            message = "Аккаунт успешно активирован"
                        else:
                            logger.warning(f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram")
                            message = "Аккаунт добавлен, но не удалось войти в Instagram"
                            success = False
                        
                        return success, message, account_data
                    
                    # Запускаем асинхронную обработку
                    success, message, account_data = loop.run_until_complete(run_add_account())
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка в фоновой обработке аккаунта {username}: {e}")
                finally:
                    loop.close()
            
            # Сначала добавляем аккаунт в БД синхронно для быстрого отображения
            account = add_instagram_account_without_login(
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )
            
            if not account:
                return jsonify({
                    'success': False,
                    'error': 'Не удалось добавить аккаунт в базу данных',
                    'integration_used': False
                }), 500
            
            logger.info(f"✅ Аккаунт {username} добавлен в БД с ID {account.id}")
            
            # Обновляем дополнительные поля, если они переданы
            if 'full_name' in data or 'biography' in data:
                update_instagram_account(
                    account.id,
                    full_name=data.get('full_name', ''),
                    biography=data.get('biography', '')
                )
            
            # Запускаем полную обработку в фоновом режиме
            thread = threading.Thread(target=background_add_account, daemon=True)
            thread.start()
            
            return jsonify({
                'success': True,
                'data': {
                    'id': account.id,
                    'username': account.username,
                    'email': account.email,
                    'full_name': data.get('full_name', ''),
                    'biography': data.get('biography', ''),
                    'is_active': account.is_active,
                    'created_at': account.created_at.isoformat(),
                    'processing': True
                },
                'message': f'Аккаунт {username} добавлен и обрабатывается в фоновом режиме. Вскоре вы получите уведомление о результате.',
                'integration_used': True,
                'processing': True
            })
        else:
            # Для аккаунтов без данных почты или без интеграции - обычная обработка
            # Используем полный процесс как в Telegram боте
            logger.info(f"📝 Добавляем аккаунт {username} в базу данных")
            
            # Сначала добавляем аккаунт в базу данных без проверки входа
            account = add_instagram_account_without_login(
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )
            
            if not account:
                return jsonify({
                    'success': False,
                    'error': 'Не удалось добавить аккаунт в базу данных',
                    'integration_used': False
                }), 500
            
            logger.info(f"✅ Аккаунт {username} добавлен в БД с ID {account.id}")
            
            # Обновляем дополнительные поля, если они переданы
            if 'full_name' in data or 'biography' in data:
                update_instagram_account(
                    account.id,
                    full_name=data.get('full_name', ''),
                    biography=data.get('biography', '')
                )
            
            # Назначаем прокси для аккаунта
            logger.info(f"📡 Назначаем прокси для аккаунта {username}")
            from utils.proxy_manager import assign_proxy_to_account
            proxy_success, proxy_message = assign_proxy_to_account(account.id)
            
            if not proxy_success:
                logger.warning(f"⚠️ {proxy_message}")
            else:
                logger.info(f"✅ {proxy_message}")
            
            return jsonify({
                'success': True,
                'data': {
                    'id': account.id,
                    'username': account.username,
                    'email': account.email,
                    'full_name': data.get('full_name', ''),
                    'biography': data.get('biography', ''),
                    'is_active': account.is_active,
                    'created_at': account.created_at.isoformat(),
                    'proxy_assigned': proxy_success,
                    'login_successful': False
                },
                'message': f'Аккаунт {username} добавлен в базу данных. Для полной активации необходимо указать данные почты.',
                'integration_used': False
            })
    
    except Exception as e:
        logger.error(f"❌ Ошибка при добавлении аккаунта: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/bulk', methods=['POST'])
def bulk_add_accounts():
    """Массовое добавление аккаунтов - простой подход на основе одинарного добавления"""
    try:
        data = request.get_json()
        
        if 'accounts' not in data or not isinstance(data['accounts'], list):
            return jsonify({
                'success': False,
                'error': 'Поле accounts должно содержать массив аккаунтов'
            }), 400
        
        accounts_data = data['accounts']
        
        if len(accounts_data) == 0:
            return jsonify({
                'success': False,
                'error': 'Список аккаунтов не может быть пустым'
            }), 400
        
        # Получаем количество параллельных потоков (по умолчанию 1 - последовательно)
        parallel_threads = data.get('parallel_threads', 1)
        if parallel_threads < 1:
            parallel_threads = 1
        elif parallel_threads > 5:  # Ограничение безопасности
            parallel_threads = 5
        
        logger.info(f"🚀 Запуск массового добавления {len(accounts_data)} аккаунтов (потоков: {parallel_threads})")
        
        def process_single_account(account_data, account_index):
            """Обработка одного аккаунта"""
            username = account_data.get('username', '').strip()
            password = account_data.get('password', '').strip()
            email = account_data.get('email', '').strip()
            email_password = account_data.get('email_password', '').strip()
            
            try:
                logger.info(f"📝 Обрабатываем аккаунт {account_index + 1}/{len(accounts_data)}: {username}")
                
                # Проверяем обязательные поля
                if not username or not password:
                    logger.error(f"❌ Аккаунт {username}: отсутствуют обязательные поля")
                    return False
                
                # Добавляем аккаунт в базу данных
                account = add_instagram_account_without_login(
                    username=username,
                    password=password,
                    email=email,
                    email_password=email_password
                )
                
                if not account:
                    logger.error(f"❌ Не удалось добавить аккаунт {username} в БД")
                    return False
                
                logger.info(f"✅ Аккаунт {username} добавлен в БД с ID {account.id}")
                
                # Назначаем прокси для аккаунта
                logger.info(f"📡 Назначаем прокси для аккаунта {username}")
                from utils.proxy_manager import assign_proxy_to_account
                proxy_success, proxy_message = assign_proxy_to_account(account.id)
                
                if not proxy_success:
                    logger.warning(f"⚠️ {proxy_message}")
                
                # Если есть email данные, пытаемся войти в Instagram
                if email and email_password and INTEGRATION_AVAILABLE:
                    logger.info(f"🔑 Попытка входа в Instagram для {username}")
                    
                    from instagram.client import test_instagram_login_with_proxy
                    login_success = test_instagram_login_with_proxy(
                        account_id=account.id,
                        username=username,
                        password=password,
                        email=email,
                        email_password=email_password
                    )
                    
                    if login_success:
                        logger.info(f"✅ Успешный вход в Instagram для {username}")
                        # Активируем аккаунт
                        from database.db_manager import update_instagram_account
                        update_instagram_account(account.id, is_active=True)
                    else:
                        logger.warning(f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram")
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке аккаунта {username}: {e}")
                return False
        
        def background_bulk_add():
            """Фоновая обработка массового добавления"""
            success_count = 0
            failed_count = 0
            
            if parallel_threads == 1:
                # Последовательная обработка
                for i, account_data in enumerate(accounts_data):
                    if process_single_account(account_data, i):
                        success_count += 1
                    else:
                        failed_count += 1
                    
                    # Небольшая задержка между аккаунтами
                    import time
                    time.sleep(2)
            else:
                # Параллельная обработка
                logger.info(f"🔄 Используем {parallel_threads} параллельных потоков")
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_threads) as executor:
                    # Создаем задачи для всех аккаунтов
                    future_to_account = {
                        executor.submit(process_single_account, account_data, i): (account_data, i) 
                        for i, account_data in enumerate(accounts_data)
                    }
                    
                    # Обрабатываем результаты по мере завершения
                    for future in concurrent.futures.as_completed(future_to_account):
                        account_data, account_index = future_to_account[future]
                        try:
                            if future.result():
                                success_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            username = account_data.get('username', 'unknown')
                            logger.error(f"❌ Ошибка в потоке для аккаунта {username}: {e}")
                            failed_count += 1
            
            logger.info(f"✅ Фоновая обработка завершена: {success_count} успешно, {failed_count} ошибок")
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=background_bulk_add, daemon=True)
        thread.start()
        
        logger.info(f"🔄 Фоновая обработка {len(accounts_data)} аккаунтов начата")
        
        # Возвращаем немедленный ответ
        return jsonify({
            'success': True,
            'message': f'Начата обработка {len(accounts_data)} аккаунтов в {parallel_threads} потоке(ах). Процесс может занять несколько минут.',
            'processing': True,
            'total_accounts': len(accounts_data),
            'parallel_threads': parallel_threads
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при массовом добавлении аккаунтов: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/<int:account_id>', methods=['PUT'])
def update_account(account_id):
    """Обновить аккаунт"""
    try:
        data = request.get_json()
        
        # Обновляем аккаунт
        success, message = update_instagram_account(account_id, **data)
        
        if success:
            # Получаем обновленный аккаунт
            account = get_instagram_account(account_id)
            if account:
                return jsonify({
                    'success': True,
                    'data': {
                        'id': account.id,
                        'username': account.username,
                        'email': account.email,
                        'full_name': account.full_name or '',
                        'biography': account.biography or '',
                        'is_active': account.is_active,
                        'updated_at': account.updated_at.isoformat()
                    }
                })
        
        return jsonify({
            'success': False,
            'error': message
        }), 400
    
    except Exception as e:
        logger.error(f"Ошибка при обновлении аккаунта: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/<int:account_id>', methods=['DELETE'])
def delete_account(account_id):
    """Удалить аккаунт"""
    try:
        success, message = delete_instagram_account(account_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Аккаунт успешно удален'
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400
    
    except Exception as e:
        logger.error(f"Ошибка при удалении аккаунта: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для работы с прокси
# =============================================================================

@app.route('/api/proxies', methods=['GET'])
def get_proxies_api():
    """Получить список всех прокси"""
    try:
        proxies = get_proxies()
        proxies_data = []
        
        for proxy in proxies:
            proxy_data = {
                'id': proxy.id,
                'protocol': proxy.protocol,
                'type': proxy.protocol,  # Для совместимости с frontend
                'host': proxy.host,
                'port': proxy.port,
                'username': proxy.username,
                'is_active': proxy.is_active,
                'assigned_to': None,  # Будем заполнять ниже
                'created_at': proxy.created_at.isoformat() if proxy.created_at else None,
                'updated_at': proxy.updated_at.isoformat() if proxy.updated_at else None
            }
            
            # Ищем назначенные аккаунты
            assigned_accounts = get_instagram_accounts()
            assigned_account = None
            for account in assigned_accounts:
                if hasattr(account, 'proxy_id') and account.proxy_id == proxy.id:
                    assigned_account = account.username
                    break
            
            proxy_data['assigned_to'] = assigned_account
            proxies_data.append(proxy_data)
        
        return jsonify({
            'success': True,
            'data': proxies_data,
            'total': len(proxies_data)
        })
    
    except Exception as e:
        logger.error(f"Ошибка при получении прокси: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies', methods=['POST'])
def add_proxy_api():
    """Добавить новый прокси"""
    try:
        data = request.get_json()
        
        # Проверяем обязательные поля
        required_fields = ['host', 'port']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({
                    'success': False,
                    'error': f'Поле {field} обязательно для заполнения'
                }), 400
        
        # Добавляем прокси
        success, result = add_proxy(
            protocol=data.get('protocol', 'http'),
            host=data['host'],
            port=int(data['port']),
            username=data.get('username'),
            password=data.get('password')
        )
        
        if success:
            proxy = get_proxy(result)
            return jsonify({
                'success': True,
                'data': {
                    'id': proxy.id,
                    'protocol': proxy.protocol,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'is_active': proxy.is_active,
                    'created_at': proxy.created_at.isoformat()
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': result
            }), 400
    
    except Exception as e:
        logger.error(f"Ошибка при добавлении прокси: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/<int:account_id>/proxy', methods=['POST'])
def assign_proxy(account_id):
    """Назначить прокси аккаунту"""
    try:
        data = request.get_json()
        
        if 'proxy_id' not in data:
            return jsonify({
                'success': False,
                'error': 'Поле proxy_id обязательно'
            }), 400
        
        success, message = assign_proxy_to_account(account_id, data['proxy_id'])
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Прокси успешно назначен аккаунту'
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400
    
    except Exception as e:
        logger.error(f"Ошибка при назначении прокси: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies/bulk', methods=['POST'])
def bulk_add_proxies():
    """Массовое добавление прокси"""
    try:
        data = request.get_json()
        
        if 'proxies' not in data or not isinstance(data['proxies'], list):
            return jsonify({
                'success': False,
                'error': 'Поле proxies должно содержать массив прокси'
            }), 400
        
        proxies_data = data['proxies']
        check_proxies = data.get('check_proxies', False)
        
        if len(proxies_data) == 0:
            return jsonify({
                'success': False,
                'error': 'Список прокси не может быть пустым'
            }), 400
        
        logger.info(f"🚀 Запуск массового добавления {len(proxies_data)} прокси")
        
        # Добавляем прокси в базу данных
        success_count = 0
        failed_count = 0
        results = []
        
        for proxy_data in proxies_data:
            try:
                success, result = add_proxy(
                    protocol=proxy_data.get('protocol', 'http'),
                    host=proxy_data['host'],
                    port=int(proxy_data['port']),
                    username=proxy_data.get('username'),
                    password=proxy_data.get('password')
                )
                
                if success:
                    success_count += 1
                    results.append({
                        'host': proxy_data['host'],
                        'port': proxy_data['port'],
                        'success': True,
                        'id': result
                    })
                    logger.info(f"✅ Прокси {proxy_data['host']}:{proxy_data['port']} добавлен")
                else:
                    failed_count += 1
                    results.append({
                        'host': proxy_data['host'],
                        'port': proxy_data['port'],
                        'success': False,
                        'error': result
                    })
                    logger.error(f"❌ Ошибка добавления прокси {proxy_data['host']}:{proxy_data['port']}: {result}")
                    
            except Exception as e:
                failed_count += 1
                results.append({
                    'host': proxy_data.get('host', 'unknown'),
                    'port': proxy_data.get('port', 'unknown'),
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"❌ Исключение при добавлении прокси: {e}")
        
        # Если нужно проверить прокси
        if check_proxies and success_count > 0:
            logger.info(f"🔍 Проверка {success_count} добавленных прокси...")
            # Здесь можно добавить логику проверки прокси
        
        return jsonify({
            'success': True,
            'message': f'Массовое добавление завершено: {success_count} успешно, {failed_count} ошибок',
            'data': {
                'total': len(proxies_data),
                'success_count': success_count,
                'failed_count': failed_count,
                'results': results,
                'check_proxies': check_proxies
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при массовом добавлении прокси: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies/<int:proxy_id>/check', methods=['POST'])
def check_proxy_api(proxy_id):
    """Проверить отдельный прокси"""
    try:
        from database.db_manager import update_proxy
        import time
        
        # Получаем прокси из базы данных
        proxy = get_proxy(proxy_id)
        if not proxy:
            return jsonify({
                'success': False,
                'error': f'Прокси с ID {proxy_id} не найден'
            }), 404
        
        logger.info(f"🔍 Проверка прокси {proxy.host}:{proxy.port}")
        
        is_active = False
        error_message = None
        
        try:
            # Проверяем прокси через HTTP запрос
            if proxy.username and proxy.password:
                # Используем auth параметр для аутентификации
                proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                
                response = requests.get(
                    'http://httpbin.org/ip',
                    proxies=proxies,
                    auth=(proxy.username, proxy.password),
                    timeout=15
                )
            else:
                # Прокси без аутентификации
                proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                
                response = requests.get(
                    'http://httpbin.org/ip',
                    proxies=proxies,
                    timeout=15
                )
            
            if response.status_code == 200:
                is_active = True
                logger.info(f"✅ Прокси {proxy.host}:{proxy.port} работает")
            else:
                error_message = f"HTTP статус: {response.status_code}"
                logger.warning(f"⚠️ Прокси {proxy.host}:{proxy.port} вернул статус {response.status_code}")
                
        except requests.exceptions.ConnectTimeout:
            error_message = "Таймаут подключения"
            logger.warning(f"⚠️ Прокси {proxy.host}:{proxy.port}: таймаут подключения")
        except requests.exceptions.ProxyError as e:
            error_message = f"Ошибка прокси: {str(e)}"
            logger.warning(f"⚠️ Прокси {proxy.host}:{proxy.port}: ошибка прокси - {e}")
        except Exception as e:
            error_message = f"Ошибка: {str(e)}"
            logger.warning(f"⚠️ Прокси {proxy.host}:{proxy.port}: общая ошибка - {e}")
        
        # Обновляем статус прокси в базе данных
        update_proxy(proxy_id, is_active=is_active)
        
        return jsonify({
            'success': True,
            'data': {
                'proxy_id': proxy_id,
                'host': proxy.host,
                'port': proxy.port,
                'is_active': is_active,
                'error_message': error_message,
                'checked_at': time.time()
            },
            'is_active': is_active
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке прокси {proxy_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies/check-all', methods=['POST'])
def check_all_proxies():
    """Проверить все прокси"""
    try:
        import concurrent.futures
        import time
        import random
        
        logger.info("🔍 Начинаем проверку всех прокси")
        
        # Получаем все прокси
        proxies = get_proxies()
        if not proxies:
            return jsonify({
                'success': True,
                'message': 'Нет прокси для проверки',
                'data': {
                    'total': 0,
                    'checked': 0,
                    'active': 0,
                    'inactive': 0
                }
            })
        
        def check_single_proxy(proxy):
            """Проверка одного прокси"""
            try:
                import requests
                from database.db_manager import update_proxy
                
                # Добавляем случайную задержку для избежания rate limiting
                time.sleep(random.uniform(0.5, 2.0))
                
                # Формируем URL прокси более осторожно
                if proxy.username and proxy.password:
                    # Используем auth параметр для аутентификации
                    proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                    proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                    
                    response = requests.get(
                        'http://httpbin.org/ip',
                        proxies=proxies,
                        auth=(proxy.username, proxy.password),
                        timeout=20  # Увеличиваем таймаут
                    )
                else:
                    # Прокси без аутентификации
                    proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                    proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                    
                    response = requests.get(
                        'http://httpbin.org/ip',
                        proxies=proxies,
                        timeout=20
                    )
                
                is_active = response.status_code == 200
                
                # Обновляем статус в базе данных
                update_proxy(proxy.id, is_active=is_active)
                
                return {
                    'id': proxy.id,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'is_active': is_active,
                    'success': True,
                    'response_code': response.status_code if is_active else None
                }
                
            except requests.exceptions.ConnectTimeout:
                # Таймаут подключения
                update_proxy(proxy.id, is_active=False)
                return {
                    'id': proxy.id,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'is_active': False,
                    'success': False,
                    'error': 'Таймаут подключения'
                }
                
            except requests.exceptions.ProxyError as e:
                # Ошибка прокси
                update_proxy(proxy.id, is_active=False)
                return {
                    'id': proxy.id,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'is_active': False,
                    'success': False,
                    'error': f'Ошибка прокси: {str(e)}'
                }
                
            except Exception as e:
                # Общая ошибка
                update_proxy(proxy.id, is_active=False)
                return {
                    'id': proxy.id,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'is_active': False,
                    'success': False,
                    'error': f'Общая ошибка: {str(e)}'
                }
        
        # Проверяем прокси параллельно (снижаем до 3 потоков для стабильности)
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_proxy = {executor.submit(check_single_proxy, proxy): proxy for proxy in proxies}
            
            for future in concurrent.futures.as_completed(future_to_proxy):
                result = future.result()
                results.append(result)
                
                # Более детальное логирование
                if result['success']:
                    status = '✅' if result['is_active'] else '❌'
                    username_info = f" (user: {result['username']})" if result['username'] else ""
                    logger.info(f"{status} Прокси {result['host']}:{result['port']}{username_info}")
                else:
                    logger.warning(f"❌ Прокси {result['host']}:{result['port']}: {result['error']}")
        
        # Подсчитываем статистику
        total = len(results)
        active = len([r for r in results if r['is_active']])
        inactive = total - active
        
        # Подсчитываем типы ошибок
        error_types = {}
        for result in results:
            if not result['success'] and 'error' in result:
                error_type = result['error'].split(':')[0]
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        logger.info(f"✅ Проверка завершена: {total} всего, {active} активных, {inactive} неактивных")
        if error_types:
            logger.info(f"📊 Типы ошибок: {error_types}")
        
        return jsonify({
            'success': True,
            'message': f'Проверка завершена: {active} из {total} прокси активны',
            'data': {
                'total': total,
                'checked': total,
                'active': active,
                'inactive': inactive,
                'error_types': error_types,
                'results': results,
                'checked_at': time.time()
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке всех прокси: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['DELETE'])
def delete_proxy_api(proxy_id):
    """Удалить прокси"""
    try:
        success, message = delete_proxy(proxy_id)
        
        if success:
            logger.info(f"🗑️ Прокси ID {proxy_id} удален")
            return jsonify({
                'success': True,
                'message': 'Прокси успешно удален'
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400
    
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении прокси {proxy_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['PUT'])
def update_proxy_api(proxy_id):
    """Обновить прокси"""
    try:
        data = request.get_json()
        
        # Обновляем прокси
        success, message = update_proxy(proxy_id, **data)
        
        if success:
            # Получаем обновленный прокси
            proxy = get_proxy(proxy_id)
            if proxy:
                logger.info(f"✏️ Прокси ID {proxy_id} обновлен")
                return jsonify({
                    'success': True,
                    'data': {
                        'id': proxy.id,
                        'protocol': proxy.protocol,
                        'host': proxy.host,
                        'port': proxy.port,
                        'username': proxy.username,
                        'is_active': proxy.is_active,
                        'updated_at': proxy.updated_at.isoformat()
                    }
                })
        
        return jsonify({
            'success': False,
            'error': message
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении прокси {proxy_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для статистики
# =============================================================================

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Получить общую статистику"""
    try:
        accounts = get_instagram_accounts()
        proxies = get_proxies()
        
        # Подсчитываем статистику
        total_accounts = len(accounts)
        active_accounts = len([acc for acc in accounts if acc.is_active])
        inactive_accounts = total_accounts - active_accounts
        
        total_proxies = len(proxies)
        active_proxies = len([proxy for proxy in proxies if proxy.is_active])
        
        # Аккаунты с назначенными прокси
        accounts_with_proxy = len([acc for acc in accounts if acc.proxy_id])
        
        return jsonify({
            'success': True,
            'data': {
                'accounts': {
                    'total': total_accounts,
                    'active': active_accounts,
                    'inactive': inactive_accounts,
                    'with_proxy': accounts_with_proxy
                },
                'proxies': {
                    'total': total_proxies,
                    'active': active_proxies,
                    'inactive': total_proxies - active_proxies
                }
            }
        })
    
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для статуса интеграции
# =============================================================================

@app.route('/api/integration/status', methods=['GET'])
def get_integration_status_api():
    """Получить статус интеграции систем"""
    try:
        if INTEGRATION_AVAILABLE:
            status = get_integration_status()
            return jsonify({
                'success': True,
                'data': {
                    'integration_available': True,
                    'systems': status,
                    'total_systems': len(status),
                    'active_systems': sum(status.values())
                }
            })
        else:
            return jsonify({
                'success': True,
                'data': {
                    'integration_available': False,
                    'message': 'Интегрированный сервис недоступен'
                }
            })
    
    except Exception as e:
        logger.error(f"Ошибка при получении статуса интеграции: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/<int:account_id>/retry', methods=['POST'])
def retry_account_login(account_id):
    """Повторная попытка входа с новым кодом верификации"""
    try:
        data = request.get_json()
        max_retries = data.get('max_retries', 3)
        
        logger.info(f"🔄 Запрос повторной попытки входа для аккаунта ID {account_id}")
        
        if not INTEGRATION_AVAILABLE:
            return jsonify({
                'success': False,
                'error': 'Интегрированный сервис недоступен'
            }), 503
        
        # Запускаем повторную попытку в фоновом режиме
        import threading
        import asyncio
        
        # Глобальная переменная для хранения результата
        retry_result = {'completed': False, 'success': False, 'message': ''}
        
        def background_retry():
            """Фоновая обработка повторной попытки"""
            try:
                # Создаем новый event loop для потока
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def run_retry():
                    from account_integration_service import account_service
                    return await account_service.retry_account_login_with_new_code(
                        account_id, max_retries
                    )
                
                # Запускаем асинхронную обработку
                success, message = loop.run_until_complete(run_retry())
                
                retry_result['completed'] = True
                retry_result['success'] = success
                retry_result['message'] = message
                
                logger.info(f"✅ Фоновая повторная попытка завершена для аккаунта ID {account_id}: {message}")
                
            except Exception as e:
                retry_result['completed'] = True
                retry_result['success'] = False
                retry_result['message'] = f"Ошибка: {str(e)}"
                logger.error(f"❌ Ошибка в фоновой повторной попытке для аккаунта ID {account_id}: {e}")
            finally:
                loop.close()
        
        # Получаем данные аккаунта для ответа
        account = get_instagram_account(account_id)
        if not account:
            return jsonify({
                'success': False,
                'error': f'Аккаунт с ID {account_id} не найден'
            }), 404
        
        # Проверяем наличие данных почты
        if not account.email or not account.email_password:
            return jsonify({
                'success': False,
                'error': 'У аккаунта отсутствуют данные почты для получения кода верификации'
            }), 400
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=background_retry, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Начата повторная попытка входа для аккаунта {account.username}. Процесс может занять несколько минут.',
            'data': {
                'account_id': account_id,
                'username': account.username,
                'max_retries': max_retries,
                'processing': True
            }
        })
    
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске повторной попытки входа: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/retry-bulk', methods=['POST'])
def retry_bulk_accounts():
    """Массовая повторная попытка входа для неудачных аккаунтов"""
    try:
        data = request.get_json()
        
        if 'account_ids' not in data or not isinstance(data['account_ids'], list):
            return jsonify({
                'success': False,
                'error': 'Поле account_ids должно содержать массив ID аккаунтов'
            }), 400
        
        account_ids = data['account_ids']
        max_retries_per_account = data.get('max_retries_per_account', 3)
        
        if len(account_ids) == 0:
            return jsonify({
                'success': False,
                'error': 'Список ID аккаунтов не может быть пустым'
            }), 400
        
        logger.info(f"🔄 Запуск массовой повторной попытки для {len(account_ids)} аккаунтов")
        
        if not INTEGRATION_AVAILABLE:
            return jsonify({
                'success': False,
                'error': 'Интегрированный сервис недоступен'
            }), 503
        
        # Запускаем массовую повторную попытку в фоновом режиме
        import threading
        import asyncio
        
        def background_bulk_retry():
            """Фоновая обработка массовой повторной попытки"""
            try:
                # Создаем новый event loop для потока
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def run_bulk_retry():
                    from account_integration_service import account_service
                    return await account_service.bulk_retry_failed_accounts(
                        account_ids, max_retries_per_account
                    )
                
                # Запускаем асинхронную обработку
                results = loop.run_until_complete(run_bulk_retry())
                
                logger.info(f"✅ Массовая повторная попытка завершена: {len(results['success'])} успешно, {len(results['failed'])} ошибок")
                
                # Здесь можно добавить отправку уведомления
                return results
                
            except Exception as e:
                logger.error(f"❌ Ошибка в фоновой массовой повторной попытке: {e}")
            finally:
                loop.close()
        
        # Проверяем существование аккаунтов и их данные
        valid_accounts = []
        invalid_accounts = []
        
        for account_id in account_ids:
            account = get_instagram_account(account_id)
            if not account:
                invalid_accounts.append({'id': account_id, 'error': 'Аккаунт не найден'})
            elif not account.email or not account.email_password:
                invalid_accounts.append({
                    'id': account_id, 
                    'username': account.username,
                    'error': 'Отсутствуют данные почты'
                })
            else:
                valid_accounts.append({
                    'id': account_id,
                    'username': account.username
                })
        
        if len(valid_accounts) == 0:
            return jsonify({
                'success': False,
                'error': 'Нет валидных аккаунтов для повторной попытки',
                'invalid_accounts': invalid_accounts
            }), 400
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=background_bulk_retry, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Начата массовая повторная попытка для {len(valid_accounts)} аккаунтов. Процесс может занять длительное время.',
            'data': {
                'valid_accounts': valid_accounts,
                'invalid_accounts': invalid_accounts,
                'total_processing': len(valid_accounts),
                'max_retries_per_account': max_retries_per_account,
                'processing': True
            }
        })
    
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске массовой повторной попытки: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для публикации контента
# =============================================================================

@app.route('/api/posts', methods=['POST'])
def create_post():
    """Создать новый пост"""
    try:
        # Поддерживаем как form-data, так и JSON
        if request.content_type and 'application/json' in request.content_type:
            # JSON запрос с base64 файлом
            data = request.get_json()
            post_type = data.get('type')
            caption = data.get('caption', '')
            hashtags = data.get('hashtags', '')
            publish_now = data.get('publish_now', True)
            scheduled_time = data.get('scheduled_time') if not publish_now else None
            account_ids = data.get('accounts', [])
            uniquify_content = data.get('uniquify', False)
            concurrent_threads = data.get('concurrent_threads', 3)
            publish_delay = data.get('publish_delay', 60)
            
            # Обработка медиафайла из base64 (если передан)
            media_data = data.get('media_data')
            if not media_data:
                return jsonify({
                    'success': False,
                    'error': 'Медиафайл обязателен'
                }), 400
        else:
            # Form-data запрос
            post_type = request.form.get('type')
            caption = request.form.get('caption', '')
            hashtags = request.form.get('hashtags', '')
            publish_now = request.form.get('publish_now', 'true').lower() == 'true'
            scheduled_time = request.form.get('scheduled_time') if not publish_now else None
            
            # Получаем аккаунты из разных возможных полей
            account_ids = request.form.getlist('accounts[]')
            if not account_ids:
                account_ids = request.form.getlist('selected_accounts[]')
            if not account_ids:
                account_ids = request.form.getlist('account_ids[]')
                
            # Проверяем account_selection для определения выбора всех аккаунтов
            account_selection = request.form.get('account_selection', 'specific')
            if account_selection == 'all' or not account_ids:
                # Если выбрано "все аккаунты" или не передан список, получаем все активные аккаунты
                all_accounts = get_instagram_accounts()
                account_ids = [str(acc.id) for acc in all_accounts if acc.is_active]
                logger.info(f"📋 Выбраны все активные аккаунты: {len(account_ids)} шт.")
            
            uniquify_content = request.form.get('uniquify', 'false').lower() == 'true'
            concurrent_threads = int(request.form.get('concurrent_threads', 3))
            publish_delay = int(request.form.get('publish_delay', 60))
            
            # Получаем параметры для историй
            story_link = request.form.get('story_link', '')
            
            # Обработка файлов в зависимости от типа публикации
            if post_type == 'carousel':
                # Для карусели ожидаем несколько файлов
                media_files = []
                media_count = int(request.form.get('media_count', 0))
                
                for i in range(media_count):
                    file_key = f'media_{i}'
                    if file_key in request.files:
                        media_file = request.files[file_key]
                        if media_file.filename != '':
                            media_files.append(media_file)
                
                if not media_files:
                    return jsonify({
                        'success': False,
                        'error': 'Для карусели требуется хотя бы одно изображение'
                    }), 400
                    
                if len(media_files) > 10:
                    return jsonify({
                        'success': False,
                        'error': 'Максимум 10 изображений для карусели'
                    }), 400
            else:
                # Для остальных типов - один файл
                if 'media' not in request.files:
                    return jsonify({
                        'success': False,
                        'error': 'Медиафайл обязателен'
                    }), 400
                    
                media_file = request.files['media']
                if media_file.filename == '':
                    return jsonify({
                        'success': False,
                        'error': 'Медиафайл не выбран'
                    }), 400
        
        # Проверяем обязательные поля
        if not post_type:
            return jsonify({
                'success': False,
                'error': 'Тип публикации обязателен'
            }), 400
            
        if post_type not in ['feed', 'reels', 'story', 'carousel']:
            return jsonify({
                'success': False,
                'error': 'Неподдерживаемый тип публикации'
            }), 400
        
        # Валидация новых параметров
        if concurrent_threads < 1 or concurrent_threads > 10:
            concurrent_threads = 3
        if publish_delay < 10 or publish_delay > 300:
            publish_delay = 60
            
        # Конвертируем account_ids в числа
        try:
            if isinstance(account_ids, list):
                account_ids = [int(acc_id) for acc_id in account_ids if acc_id]
            else:
                account_ids = []
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Неверный формат ID аккаунтов'
            }), 400
            
        if not account_ids:
            return jsonify({
                'success': False,
                'error': 'Не выбраны аккаунты для публикации'
            }), 400
        
        logger.info(f"🎯 Создание публикации для {len(account_ids)} аккаунтов: {account_ids}")
        
        # Сохраняем медиафайл(ы)
        import tempfile
        import uuid
        import json
        temp_dir = tempfile.gettempdir()
        
        if request.content_type and 'application/json' in request.content_type:
            # Обработка base64 данных (для будущего использования)
            return jsonify({
                'success': False,
                'error': 'JSON upload пока не поддерживается, используйте form-data'
            }), 400
        else:
            # Обработка загруженных файлов
            if post_type == 'carousel':
                # Сохраняем все файлы карусели
                temp_paths = []
                for media_file in media_files:
                    file_extension = media_file.filename.split('.')[-1].lower()
                    temp_filename = f"{uuid.uuid4()}.{file_extension}"
                    temp_path = os.path.join(temp_dir, temp_filename)
                    media_file.save(temp_path)
                    temp_paths.append(temp_path)
                
                # Для карусели используем JSON список путей
                media_path_data = json.dumps(temp_paths)
            else:
                # Обработка одного файла
                file_extension = media_file.filename.split('.')[-1].lower()
                temp_filename = f"{uuid.uuid4()}.{file_extension}"
                temp_path = os.path.join(temp_dir, temp_filename)
                media_file.save(temp_path)
                media_path_data = temp_path
        
        # Создаем задачи на публикацию
        from database.db_manager import create_publish_task
        from database.models import TaskType, TaskStatus
        from utils.task_queue import add_task_to_queue, start_task_queue
        import random
        import time
        
        # Определяем тип задачи
        if post_type == 'carousel':
            task_type = TaskType.CAROUSEL
            file_extension = 'jpg'  # Для карусели используем расширение первого файла
        else:
            task_type_map = {
                'feed': TaskType.PHOTO if file_extension in ['jpg', 'jpeg', 'png', 'webp'] else TaskType.VIDEO,
                'reels': TaskType.REEL,
                'story': TaskType.STORY
            }
            task_type = task_type_map.get(post_type, TaskType.PHOTO)
            
            # Для reels всегда используем тип REEL независимо от расширения файла
            if post_type == 'reels':
                task_type = TaskType.REEL
        
        logger.info(f"📝 Тип публикации: {post_type} -> TaskType: {task_type}")
        logger.info(f"🔧 Параметры: потоки={concurrent_threads}, задержка={publish_delay}с")
        
        # Запускаем обработчик очереди, если он не запущен
        start_task_queue()
        
        created_tasks = []
        failed_tasks = []
        
        # Генерируем уникальный batch_id для группы публикаций
        batch_id = str(uuid.uuid4()) if len(account_ids) > 1 else None
        
        # Разбиваем аккаунты на батчи по количеству потоков
        account_batches = []
        for i in range(0, len(account_ids), concurrent_threads):
            batch = account_ids[i:i + concurrent_threads]
            account_batches.append(batch)
        
        logger.info(f"🚀 Создание {len(account_ids)} задач публикации в {len(account_batches)} батчах по {concurrent_threads} потоков")
        logger.info(f"⏰ Задержка между публикациями: {publish_delay}±{int(publish_delay*0.5)} секунд")
        
        task_counter = 0
        for batch_index, batch_account_ids in enumerate(account_batches):
            for account_id in batch_account_ids:
                try:
                    # Подготавливаем дополнительные данные
                    additional_data = {
                        'hide_from_feed': False,  # для Reels
                        'uniquify_content': uniquify_content,
                        'post_type': post_type,
                        'batch_index': batch_index,
                        'concurrent_threads': concurrent_threads,
                        'publish_delay': publish_delay,
                        'batch_id': batch_id  # Добавляем batch_id для группировки
                    }
                    
                    # Добавляем параметры для историй
                    if post_type == 'story':
                        additional_data['story_link'] = story_link
                    
                    # Формируем полный caption с хештегами
                    full_caption = caption
                    if hashtags and post_type in ['feed', 'reels']:
                        full_caption = f"{caption}\n\n{hashtags}".strip()
                    
                    # Создаем задачу
                    if publish_now:
                        success, task_id = create_publish_task(
                            account_id=account_id,
                            task_type=task_type,
                            media_path=media_path_data,  # Используем media_path_data вместо temp_path
                            caption=full_caption,
                            additional_data=json.dumps(additional_data)  # Изменено с options на additional_data
                        )
                        
                        if success:
                            # Добавляем задержку для имитации человеческого поведения
                            # Первый батч стартует сразу, остальные с задержками
                            base_delay = batch_index * publish_delay
                            random_delay = random.randint(0, int(publish_delay * 0.5))
                            total_delay = base_delay + random_delay
                            
                            # Добавляем задачу в очередь с задержкой
                            add_task_to_queue(task_id, 0, None, delay_seconds=total_delay)
                            
                            created_tasks.append({
                                'task_id': task_id,
                                'account_id': account_id,
                                'delay_seconds': total_delay,
                                'batch_index': batch_index
                            })
                            
                            task_counter += 1
                            logger.info(f"✅ Создана задача #{task_id} для аккаунта {account_id} (#{task_counter}/{len(account_ids)}, задержка: {total_delay}с)")
                        else:
                            failed_tasks.append({
                                'account_id': account_id,
                                'error': task_id  # В случае ошибки task_id содержит сообщение об ошибке
                            })
                            logger.error(f"❌ Ошибка создания задачи для аккаунта {account_id}: {task_id}")
                    else:
                        # Запланированная публикация - пока заглушка
                        failed_tasks.append({
                            'account_id': account_id,
                            'error': 'Запланированная публикация пока не поддерживается'
                        })
                        
                except Exception as e:
                    failed_tasks.append({
                        'account_id': account_id,
                        'error': str(e)
                    })
                    logger.error(f"❌ Ошибка обработки аккаунта {account_id}: {e}")
        
        if created_tasks:
            logger.info(f"🎉 Успешно создано {len(created_tasks)} задач на публикацию")
            
            # Рассчитываем примерное время завершения
            max_delay = max([task['delay_seconds'] for task in created_tasks]) if created_tasks else 0
            estimated_completion = max_delay + 300  # + 5 минут на саму публикацию
            
            return jsonify({
                'success': True,
                'message': f'Создано {len(created_tasks)} задач на публикацию с настройками: {concurrent_threads} потоков, задержка {publish_delay}с',
                'data': {
                    'created_tasks': created_tasks,
                    'failed_tasks': failed_tasks,
                    'total_accounts': len(account_ids),
                    'successful_tasks': len(created_tasks),
                    'concurrent_threads': concurrent_threads,
                    'publish_delay': publish_delay,
                    'batches_count': len(account_batches),
                    'estimated_completion_seconds': estimated_completion
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Не удалось создать ни одной задачи на публикацию',
                'failed_tasks': failed_tasks
            }), 400
    
    except Exception as e:
        logger.error(f"❌ Ошибка при создании поста: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/posts', methods=['GET'])
def get_posts():
    """Получить список задач публикации"""
    try:
        from database.db_manager import get_session
        from database.models import PublishTask, InstagramAccount
        from sqlalchemy.orm import joinedload
        
        session = get_session()
        
        # Получаем задачи с информацией об аккаунтах
        tasks = session.query(PublishTask).options(
            joinedload(PublishTask.account)
        ).order_by(PublishTask.created_at.desc()).limit(100).all()
        
        posts_data = []
        for task in tasks:
            # Извлекаем дополнительные данные
            additional_data = {}
            if task.options:
                try:
                    additional_data = json.loads(task.options) if isinstance(task.options, str) else task.options
                except:
                    additional_data = {}
            
            # Определяем тип поста из дополнительных данных или типа задачи
            post_type = additional_data.get('post_type', '')
            if not post_type:
                # Если нет в additional_data, определяем по типу задачи
                if task.task_type:
                    task_type_value = task.task_type.value if hasattr(task.task_type, 'value') else str(task.task_type)
                    if 'PHOTO' in task_type_value:
                        post_type = 'feed'
                    elif 'REEL' in task_type_value:
                        post_type = 'reels'
                    elif 'STORY' in task_type_value:
                        post_type = 'story'
                    elif 'CAROUSEL' in task_type_value:
                        post_type = 'carousel'
                    else:
                        post_type = 'feed'
            
            posts_data.append({
                'id': task.id,
                'account_id': task.account_id,
                'account_username': task.account.username if task.account else 'Unknown',
                'task_type': task.task_type.value if task.task_type else 'unknown',
                'post_type': post_type,
                'status': task.status.value if task.status else 'unknown',
                'caption': task.caption or '',
                'media_path': task.media_path or '',
                'scheduled_time': task.scheduled_time.isoformat() if task.scheduled_time else None,
                'completed_time': task.completed_time.isoformat() if task.completed_time else None,
                'error_message': task.error_message,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None,
                'batch_id': additional_data.get('batch_id'),
                'batch_index': additional_data.get('batch_index')
            })
        
        session.close()
        
        return jsonify({
            'success': True,
            'data': posts_data,
            'total': len(posts_data)
        })
    
    except Exception as e:
        logger.error(f"Ошибка при получении постов: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/posts/<int:task_id>', methods=['DELETE'])
def delete_post(task_id):
    """Удалить задачу публикации и сам пост из Instagram"""
    try:
        from database.db_manager import get_session
        from database.models import PublishTask, TaskStatus, TaskType
        
        session = get_session()
        task = session.query(PublishTask).filter(PublishTask.id == task_id).first()
        
        if not task:
            session.close()
            return jsonify({
                'success': False,
                'error': 'Задача не найдена'
            }), 404
        
        # Сохраняем данные для удаления из Instagram
        account_id = task.account_id
        status = task.status
        media_id = task.media_id  # Сначала пробуем взять из поля media_id
        
        logger.info(f"🔍 Проверка задачи #{task_id}: status={status}, media_id={media_id}, options={task.options}")
        
        # Если media_id не в поле, пробуем извлечь из options (для старых записей)
        if status == TaskStatus.COMPLETED and not media_id and task.options:
            try:
                options = json.loads(task.options) if isinstance(task.options, str) else task.options
                media_id = options.get('media_id')
                logger.info(f"📋 Извлечен media_id из options: {media_id}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось извлечь media_id из options: {e}")
                pass
        
        # Пытаемся удалить пост из Instagram если он был опубликован
        instagram_deleted = False
        instagram_error = None
        
        if status == TaskStatus.COMPLETED and media_id:
            try:
                logger.info(f"🗑️ Попытка удалить пост {media_id} из Instagram для аккаунта {account_id}")
                
                # Используем InstagramClient для удаления
                from instagram.client import InstagramClient
                client = InstagramClient(account_id)
                
                logger.info(f"🔄 Инициализирован клиент для аккаунта {account_id}")
                
                # Проверяем и выполняем вход если необходимо
                if not client.check_login():
                    logger.error(f"❌ Не удалось войти в аккаунт {account_id} для удаления поста")
                    instagram_error = "Не удалось войти в аккаунт Instagram"
                    raise Exception(instagram_error)
                
                # Удаляем пост из Instagram
                delete_result = client.client.media_delete(media_id)
                logger.info(f"📤 Результат удаления: {delete_result}")
                
                if delete_result:
                    instagram_deleted = True
                    logger.info(f"✅ Пост {media_id} успешно удален из Instagram")
                else:
                    logger.warning(f"⚠️ Не удалось удалить пост {media_id} из Instagram")
                    instagram_error = "Instagram вернул отрицательный ответ"
                    
            except Exception as e:
                instagram_error = str(e)
                logger.error(f"❌ Ошибка при удалении поста из Instagram: {e}")
        
        # Удаляем медиафайл, если он существует
        if task.media_path and os.path.exists(task.media_path):
            try:
                # Если это карусель, удаляем все файлы
                if task.task_type == TaskType.CAROUSEL:
                    try:
                        media_paths = json.loads(task.media_path)
                        for path in media_paths:
                            if os.path.exists(path):
                                os.remove(path)
                    except:
                        # Если не JSON, удаляем как обычный файл
                        os.remove(task.media_path)
                else:
                    os.remove(task.media_path)
            except Exception as e:
                logger.warning(f"Не удалось удалить медиафайл {task.media_path}: {e}")
        
        # Удаляем задачу из базы данных
        session.delete(task)
        session.commit()
        session.close()
        
        response_data = {
            'success': True,
            'message': 'Задача успешно удалена из базы данных'
        }
        
        if status == TaskStatus.COMPLETED:
            if instagram_deleted:
                response_data['message'] = 'Пост успешно удален из Instagram и базы данных'
                response_data['instagram_deleted'] = True
            elif instagram_error:
                response_data['message'] = 'Задача удалена из базы данных, но не удалось удалить пост из Instagram'
                response_data['instagram_deleted'] = False
                response_data['instagram_error'] = instagram_error
            else:
                response_data['message'] = 'Задача удалена из базы данных (пост не был найден в Instagram)'
                response_data['instagram_deleted'] = False
        
        return jsonify(response_data)
    
    except Exception as e:
        logger.error(f"Ошибка при удалении задачи {task_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/posts/task/<int:task_id>/status', methods=['GET'])
def get_task_status_api(task_id):
    """Получить статус задачи публикации"""
    try:
        from utils.task_queue import get_task_status
        
        status = get_task_status(task_id)
        if status:
            return jsonify({
                'success': True,
                'data': status
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Задача не найдена'
            }), 404
    
    except Exception as e:
        logger.error(f"Ошибка при получении статуса задачи {task_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/check-username', methods=['POST'])
def check_username():
    """Проверить доступность юзернейма в Instagram"""
    try:
        data = request.get_json()
        username = data.get('username', '').strip()
        
        if not username:
            return jsonify({
                'success': False,
                'error': 'Юзернейм не указан'
            }), 400
        
        # Валидация формата
        import re
        username_regex = re.compile(r'^[a-zA-Z0-9._]{1,30}$')
        if not username_regex.match(username):
            return jsonify({
                'success': False,
                'error': 'Неверный формат юзернейма',
                'available': False
            }), 400
        
        # Проверяем через Instagram API
        logger.info(f"🔍 Проверка доступности юзернейма: {username}")
        
        try:
            # Используем любой активный аккаунт для проверки
            accounts = get_instagram_accounts()
            active_account = next((acc for acc in accounts if acc.is_active), None)
            
            if not active_account:
                # Если нет активных аккаунтов, возвращаем предупреждение
                return jsonify({
                    'success': True,
                    'available': None,
                    'message': 'Нет активных аккаунтов для проверки',
                    'warning': True
                })
            
            # Используем InstagramClient для проверки
            from instagram.client import InstagramClient
            client = InstagramClient(active_account.id)
            
            if not client.check_login():
                return jsonify({
                    'success': True,
                    'available': None,
                    'message': 'Не удалось войти в Instagram для проверки',
                    'warning': True
                })
            
            # Проверяем доступность юзернейма
            try:
                user_info = client.client.user_info_by_username(username)
                # Если получили информацию о пользователе, значит юзернейм занят
                available = False
                suggestions = []
                
                # Генерируем предложения
                import random
                suffixes = ['_official', '_real', '_new', str(random.randint(100, 999)), '_pro']
                for suffix in suffixes[:3]:
                    suggestions.append(f"{username}{suffix}")
                    
            except Exception as e:
                # Если ошибка 404 или пользователь не найден - юзернейм свободен
                if "User not found" in str(e) or "404" in str(e):
                    available = True
                    suggestions = []
                else:
                    logger.warning(f"⚠️ Ошибка при проверке юзернейма: {e}")
                    return jsonify({
                        'success': True,
                        'available': None,
                        'message': 'Не удалось проверить доступность',
                        'warning': True
                    })
            
            return jsonify({
                'success': True,
                'available': available,
                'suggestions': suggestions
            })
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке юзернейма через Instagram: {e}")
            return jsonify({
                'success': True,
                'available': None,
                'message': 'Ошибка при проверке',
                'warning': True
            })
    
    except Exception as e:
        logger.error(f"❌ Ошибка в check_username: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/profiles/update', methods=['POST'])
def update_profiles():
    """Обновить профили для выбранных аккаунтов"""
    try:
        # Получаем данные из формы
        account_ids = request.form.getlist('account_ids[]')
        if not account_ids:
            return jsonify({
                'success': False,
                'error': 'Не выбраны аккаунты'
            }), 400
        
        # Конвертируем в числа
        try:
            account_ids = [int(acc_id) for acc_id in account_ids]
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Неверный формат ID аккаунтов'
            }), 400
        
        # Получаем данные профиля
        username = request.form.get('username', '').strip()
        display_names = request.form.getlist('display_names[]')  # Массив имен
        display_name = request.form.get('display_name', '').strip()  # Одно имя для всех
        bio = request.form.get('bio', '').strip()
        website = request.form.get('website', '').strip()
        
        # Параметры потоков
        thread_count = int(request.form.get('thread_count', 3))
        action_delay = int(request.form.get('action_delay', 5))
        
        # Параметры уникализации
        enable_uniquifier = request.form.get('enable_uniquifier', 'false').lower() == 'true'
        uniquify_avatar = request.form.get('uniquify_avatar', 'false').lower() == 'true'
        uniquify_bio = request.form.get('uniquify_bio', 'false').lower() == 'true'
        uniquify_name = request.form.get('uniquify_name', 'false').lower() == 'true'
        
        # Обработка аватаров
        avatar_paths = []
        
        # Одиночный аватар
        if 'avatar' in request.files:
            avatar_file = request.files['avatar']
            if avatar_file.filename != '':
                import uuid
                file_extension = avatar_file.filename.split('.')[-1].lower()
                temp_filename = f"{uuid.uuid4()}.{file_extension}"
                temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
                avatar_file.save(temp_path)
                avatar_paths.append(temp_path)
        
        # Множественные аватары
        if 'avatars[]' in request.files:
            avatar_files = request.files.getlist('avatars[]')
            for avatar_file in avatar_files:
                if avatar_file.filename != '':
                    import uuid
                    file_extension = avatar_file.filename.split('.')[-1].lower()
                    temp_filename = f"{uuid.uuid4()}.{file_extension}"
                    temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
                    avatar_file.save(temp_path)
                    avatar_paths.append(temp_path)
        
        # Опции распределения аватаров
        distribute_avatars = request.form.get('distribute_avatars', 'true').lower() == 'true'
        randomize_avatars = request.form.get('randomize_avatars', 'false').lower() == 'true'
        
        # Перемешиваем аватары если нужно
        if randomize_avatars and len(avatar_paths) > 1:
            import random
            random.shuffle(avatar_paths)
        
        logger.info(f"🎨 Обновление профилей для {len(account_ids)} аккаунтов")
        logger.info(f"   Полученные данные:")
        logger.info(f"   - Username: {username}")
        logger.info(f"   - Display name(s): {display_names or display_name}")
        logger.info(f"   - Bio: {bio}")
        logger.info(f"   - Website: {website}")
        logger.info(f"   - Avatars: {len(avatar_paths)} файлов")
        logger.info(f"   - Потоки: {thread_count}, задержка: {action_delay}с")
        logger.info(f"   - Уникализация: аватар={uniquify_avatar}, био={uniquify_bio}, имя={uniquify_name}")
        
        # Подготавливаем имена для аккаунтов
        if display_names and len(display_names) > 0:
            # Если переданы множественные имена
            names_for_accounts = []
            for i, acc_id in enumerate(account_ids):
                # Равномерное распределение имен
                name_index = i % len(display_names)
                names_for_accounts.append(display_names[name_index])
        else:
            # Используем одно имя для всех
            names_for_accounts = [display_name] * len(account_ids)
        
        # Результаты обновления
        results = {
            'success': [],
            'failed': []
        }
        
        success_count = 0
        failed_accounts = []
        
        # Функция для обработки одного аккаунта
        def process_account(i, account_id):
            nonlocal success_count, failed_accounts
            try:
                account = get_instagram_account(account_id)
                if not account:
                    results['failed'].append({
                        'account_id': account_id,
                        'error': 'Аккаунт не найден'
                    })
                    return
                
                # Подготавливаем данные для обновления
                update_data = {}
                
                # Имя профиля с учетом распределения
                if names_for_accounts[i]:
                    update_data['full_name'] = names_for_accounts[i]
                    if uniquify_name and len(account_ids) > 1:
                        # Добавляем уникальный суффикс
                        update_data['full_name'] += f" {i+1}"
                
                # Остальные поля
                if bio:
                    update_data['biography'] = bio
                    if uniquify_bio and len(account_ids) > 1:
                        # Добавляем эмодзи или номер
                        emojis = ['✨', '🌟', '💫', '⭐', '🌈', '🎯', '🚀', '💎']
                        emoji = emojis[i % len(emojis)]
                        update_data['biography'] = f"{emoji} {bio}"
                
                # Обновляем в базе данных
                if update_data:
                    update_instagram_account(account_id, **update_data)
                
                # Если нужно обновить юзернейм или аватар через Instagram API
                if (username or avatar_paths or names_for_accounts[i] or bio or website) and account.is_active:
                    try:
                        from instagram.client import InstagramClient
                        client = InstagramClient(account_id)
                        
                        if client.check_login():
                            # Подготавливаем данные для обновления через Instagram API
                            instagram_update_data = {}
                            
                            # Обновляем юзернейм
                            if username:
                                # Добавляем номер к юзернейму для уникальности
                                unique_username = username
                                if len(account_ids) > 1:
                                    unique_username = f"{username}{i+1}"
                                instagram_update_data['username'] = unique_username
                            
                            # Обновляем имя профиля
                            if names_for_accounts[i]:
                                full_name = names_for_accounts[i]
                                if uniquify_name and len(account_ids) > 1:
                                    full_name += f" {i+1}"
                                instagram_update_data['full_name'] = full_name
                            
                            # Обновляем описание профиля
                            if bio:
                                biography = bio
                                if uniquify_bio and len(account_ids) > 1:
                                    emojis = ['✨', '🌟', '💫', '⭐', '🌈', '🎯', '🚀', '💎']
                                    emoji = emojis[i % len(emojis)]
                                    biography = f"{emoji} {bio}"
                                instagram_update_data['biography'] = biography
                            
                            # Добавляем ссылку в биографию (так как Instagram не позволяет добавлять кликабельные ссылки через API)
                            if website and bio:
                                # Проверяем, не содержит ли биография уже эту ссылку
                                if website not in bio:
                                    # Добавляем ссылку в конец биографии
                                    bio_with_link = f"{bio}\n🔗 {website}"
                                    
                                    # Проверяем лимит в 150 символов
                                    if len(bio_with_link) <= 150:
                                        instagram_update_data['biography'] = bio_with_link
                                        logger.info(f"🔗 Добавляем ссылку в биографию: {website}")
                                    else:
                                        # Обрезаем биографию, чтобы влезла ссылка
                                        link_part = f"\n🔗 {website}"
                                        max_bio_length = 150 - len(link_part)
                                        truncated_bio = bio[:max_bio_length].rsplit(' ', 1)[0] + "..."
                                        instagram_update_data['biography'] = truncated_bio + link_part
                                        logger.info(f"🔗 Добавляем ссылку в биографию (с обрезкой текста): {website}")
                                else:
                                    logger.info(f"ℹ️ Ссылка {website} уже есть в биографии")
                            
                            # Отправляем обновления в Instagram
                            if instagram_update_data:
                                try:
                                    logger.info(f"📤 Отправка обновлений профиля в Instagram для {account.username}: {instagram_update_data}")
                                    
                                    # Устанавливаем таймаут для запросов
                                    original_timeout = client.client.request_timeout
                                    client.client.request_timeout = 30  # 30 секунд таймаут
                                    
                                    logger.info(f"   Отправляем запрос account_edit...")
                                    
                                    # Используем стандартный метод account_edit
                                    client.client.account_edit(**instagram_update_data)
                                    logger.info(f"✅ Профиль успешно обновлен в Instagram для {account.username}")
                                    
                                    # Восстанавливаем оригинальный таймаут
                                    client.client.request_timeout = original_timeout
                                    
                                    # Обновляем локальную базу данных с новыми данными
                                    local_update_data = {}
                                    if 'username' in instagram_update_data:
                                        local_update_data['username'] = instagram_update_data['username']
                                    if 'full_name' in instagram_update_data:
                                        local_update_data['full_name'] = instagram_update_data['full_name']
                                    if 'biography' in instagram_update_data:
                                        local_update_data['biography'] = instagram_update_data['biography']
                                    if 'external_url' in instagram_update_data:
                                        local_update_data['website'] = instagram_update_data['external_url']
                                    
                                    if local_update_data:
                                        update_instagram_account(account_id, **local_update_data)
                                        logger.info(f"✅ Локальная база данных обновлена для {account.username}")
                                    
                                    success_count += 1
                                except TimeoutError as e:
                                    logger.error(f"⏱️ Таймаут при обновлении профиля {account.username}: {str(e)}")
                                    failed_accounts.append({
                                        'username': account.username,
                                        'error': f'Таймаут запроса: {str(e)}'
                                    })
                                except Exception as e:
                                    logger.error(f"❌ Ошибка при обновлении профиля через API для {account.username}: {str(e)}")
                                    logger.error(f"   Тип ошибки: {type(e).__name__}")
                                    import traceback
                                    logger.error(f"   Traceback: {traceback.format_exc()}")
                                    
                                    # Обработка специфичных ошибок Instagram
                                    error_message = str(e)
                                    if "Another account is using the same email" in error_message:
                                        error_message = "Этот email уже используется другим аккаунтом Instagram"
                                    elif "Another account is using the same phone number" in error_message:
                                        error_message = "Этот номер телефона уже используется другим аккаунтом Instagram"
                                    elif "You need an email or confirmed phone number" in error_message:
                                        error_message = "Для изменения данных требуется подтвержденный email или номер телефона"
                                    elif "Please wait a few minutes before you try again" in error_message:
                                        error_message = "Слишком много попыток. Подождите несколько минут"
                                    elif "invalid phone number" in error_message.lower():
                                        error_message = "Неверный формат номера телефона"
                                    elif "invalid email" in error_message.lower():
                                        error_message = "Неверный формат email"
                                    
                                    failed_accounts.append({
                                        'username': account.username,
                                        'error': error_message
                                    })
                            
                            # Обновляем аватар отдельно
                            if avatar_paths:
                                try:
                                    # Определяем какой аватар использовать
                                    if distribute_avatars and len(avatar_paths) > 1:
                                        # Равномерное распределение аватаров
                                        avatar_index = i % len(avatar_paths)
                                        avatar_to_use = avatar_paths[avatar_index]
                                    else:
                                        # Используем первый аватар для всех
                                        avatar_to_use = avatar_paths[0]
                                    
                                    avatar_num = (avatar_index + 1) if (distribute_avatars and len(avatar_paths) > 1) else 1
                                    logger.info(f"📸 Обновление аватара для {account.username} (файл {avatar_num} из {len(avatar_paths)})")
                                    
                                    # Если нужна уникализация аватара
                                    avatar_to_upload = avatar_to_use
                                    if uniquify_avatar and len(account_ids) > 1:
                                        from utils.image_utils import uniquify_image
                                        avatar_to_upload = uniquify_image(avatar_to_use, i)
                                        logger.info(f"🎨 Аватар уникализирован для {account.username}")
                                    
                                    # Устанавливаем таймаут
                                    original_timeout = client.client.request_timeout
                                    client.client.request_timeout = 30
                                    
                                    client.client.account_change_picture(avatar_to_upload)
                                    logger.info(f"✅ Аватар успешно обновлен для {account.username}")
                                    
                                    # Восстанавливаем таймаут
                                    client.client.request_timeout = original_timeout
                                    
                                    # Удаляем временный уникализированный файл
                                    if avatar_to_upload != avatar_to_use and os.path.exists(avatar_to_upload):
                                        try:
                                            os.remove(avatar_to_upload)
                                        except:
                                            pass
                                            
                                except TimeoutError as e:
                                    logger.error(f"⏱️ Таймаут при обновлении аватара для {account.username}: {str(e)}")
                                except Exception as e:
                                    logger.error(f"❌ Ошибка при обновлении аватара для {account.username}: {str(e)}")
                                    import traceback
                                    logger.error(f"   Traceback: {traceback.format_exc()}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка при работе с Instagram API для {account.username}: {e}")
                        results['failed'].append({
                            'account_id': account_id,
                            'username': account.username,
                            'error': str(e)
                        })
                        return
                
                # Если были ошибки при обновлении через API, но локальное обновление прошло
                if any(fa['username'] == account.username for fa in failed_accounts):
                    error_info = next(fa for fa in failed_accounts if fa['username'] == account.username)
                    results['failed'].append({
                        'account_id': account_id,
                        'username': account.username,
                        'error': error_info['error']
                    })
                else:
                    # Если дошли до этого места, значит обновление прошло успешно
                    results['success'].append({
                        'account_id': account_id,
                        'username': account.username,
                        'display_name': names_for_accounts[i]
                    })
                
            except Exception as e:
                results['failed'].append({
                    'account_id': account_id,
                    'error': str(e)
                })
                logger.error(f"❌ Ошибка при обновлении аккаунта {account_id}: {e}")
        
        # Обработка аккаунтов с использованием потоков
        import concurrent.futures
        import time
        
        logger.info(f"🚀 Запуск обработки с {thread_count} потоками")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Создаем задачи для каждого аккаунта
            futures = []
            for i, account_id in enumerate(account_ids):
                # Добавляем задержку между запусками
                if i > 0:
                    time.sleep(action_delay / thread_count)
                
                future = executor.submit(process_account, i, account_id)
                futures.append(future)
            
            # Ждем завершения всех задач
            concurrent.futures.wait(futures)
        
        # Удаляем временные файлы аватаров
        for avatar_path in avatar_paths:
            if os.path.exists(avatar_path):
                try:
                    os.remove(avatar_path)
                except:
                    pass
        
        return jsonify({
            'success': True,
            'message': f'Обновлено {len(results["success"])} из {len(account_ids)} аккаунтов',
            'results': results
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка в update_profiles: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/groups', methods=['GET'])
def get_groups():
    """Получить группы аккаунтов (заглушка для совместимости с фронтендом)"""
    try:
        # Пока возвращаем простые группы на основе статуса аккаунтов
        accounts = get_instagram_accounts()
        
        groups = [
            {
                'id': 'all',
                'name': 'Все аккаунты',
                'account_count': len(accounts)
            },
            {
                'id': 'active',
                'name': 'Активные аккаунты',
                'account_count': len([acc for acc in accounts if acc.is_active])
            },
            {
                'id': 'inactive',
                'name': 'Неактивные аккаунты',
                'account_count': len([acc for acc in accounts if not acc.is_active])
            }
        ]
        
        return jsonify({
            'success': True,
            'data': groups
        })
    
    except Exception as e:
        logger.error(f"Ошибка при получении групп: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# Маршрут для медиа файлов
# =============================================================================

@app.route('/media/<path:filename>')
def serve_media(filename):
    """Обслуживание медиа файлов"""
    try:
        # Ищем файл в разных возможных местах
        possible_paths = [
            os.path.join('media', filename),
            os.path.join('/tmp', filename),
            os.path.join(tempfile.gettempdir(), filename),
            filename  # Абсолютный путь
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return send_file(path)
        
        # Если файл не найден, возвращаем заглушку
        return '', 404
    except Exception as e:
        logger.error(f"Ошибка при обслуживании медиа файла {filename}: {e}")
        return '', 404

@app.route('/api/warmup/settings', methods=['GET', 'POST'])
def warmup_settings():
    """Получить или сохранить настройки прогрева"""
    try:
        settings_file = 'warmup_settings.json'
        
        if request.method == 'GET':
            # Читаем настройки из файла или возвращаем значения по умолчанию
            if os.path.exists(settings_file):
                with open(settings_file, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
            else:
                settings = {
                    'minPostsPerDay': 1,
                    'maxPostsPerDay': 3,
                    'minStoriesPerDay': 2,
                    'maxStoriesPerDay': 5,
                    'minFollowsPerDay': 10,
                    'maxFollowsPerDay': 30,
                    'minLikesPerDay': 20,
                    'maxLikesPerDay': 50,
                    'minCommentsPerDay': 5,
                    'maxCommentsPerDay': 15,
                    'actionDelay': 30,
                    'sessionDuration': 120,
                    'nightPauseStart': '23:00',
                    'nightPauseEnd': '07:00'
                }
            
            return jsonify({
                'success': True,
                'data': settings
            })
        
        else:  # POST
            # Сохраняем настройки
            data = request.get_json()
            
            # Проверяем наличие основных полей
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'Нет данных для сохранения'
                }), 400
            
            # Преобразуем формат данных из фронтенда в простой формат для хранения
            settings = {
                'minPostsPerDay': 1,
                'maxPostsPerDay': 3,
                'minStoriesPerDay': 2,
                'maxStoriesPerDay': 5,
                'minFollowsPerDay': 10,
                'maxFollowsPerDay': 30,
                'minLikesPerDay': 20,
                'maxLikesPerDay': 50,
                'minCommentsPerDay': 5,
                'maxCommentsPerDay': 15,
                'actionDelay': 30,
                'sessionDuration': 120,
                'nightPauseStart': '23:00',
                'nightPauseEnd': '07:00'
            }
            
            # Извлекаем данные из структуры фронтенда
            if 'phases' in data:
                # Фаза 1 - подписки
                if 'phase1' in data['phases'] and data['phases']['phase1'].get('enabled'):
                    settings['minFollowsPerDay'] = data['phases']['phase1'].get('min_daily', 10)
                    settings['maxFollowsPerDay'] = data['phases']['phase1'].get('max_daily', 30)
                
                # Фаза 2 - лайки
                if 'phase2' in data['phases'] and data['phases']['phase2'].get('enabled'):
                    settings['minLikesPerDay'] = data['phases']['phase2'].get('min_daily', 20)
                    settings['maxLikesPerDay'] = data['phases']['phase2'].get('max_daily', 50)
            
            # Рабочие часы
            if 'working_hours' in data:
                settings['nightPauseStart'] = data['working_hours'].get('start', '09:00')
                settings['nightPauseEnd'] = data['working_hours'].get('end', '21:00')
            
            # Интервалы между действиями
            if 'action_intervals' in data:
                settings['actionDelay'] = data['action_intervals'].get('min', 30)
            
            # Сохраняем полные настройки для дальнейшего использования
            settings['full_settings'] = data
            
            # Сохраняем в файл
            with open(settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Настройки прогрева сохранены: {len(data.get('accounts', []))} аккаунтов")
            
            return jsonify({
                'success': True,
                'message': 'Настройки успешно сохранены'
            })
    
    except Exception as e:
        logger.error(f"❌ Ошибка в warmup_settings: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/warmup/start', methods=['POST'])
def start_warmup():
    """Запустить прогрев для выбранных аккаунтов"""
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        settings = data.get('settings', {}) or {}
        user_id = data.get('user_id')
        if user_id:
            settings['user_id'] = user_id

        # Добавляем поддержку настроек целевых аккаунтов
        target_accounts = data.get('target_accounts', '')
        unique_follows = data.get('unique_follows', True)
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': 'Не выбраны аккаунты'
            }), 400
        
        # Если настройки переданы в запросе, используем их
        if settings:
            logger.info(f"📋 Используем настройки из запроса")
            # Добавляем настройки целевых аккаунтов
            settings['target_accounts'] = target_accounts
            settings['unique_follows'] = unique_follows
        else:
            # Загружаем настройки из файла
            settings_file = 'warmup_settings.json'
            if os.path.exists(settings_file):
                with open(settings_file, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                logger.info(f"📋 Загружены настройки из файла")
                # Добавляем настройки целевых аккаунтов
                settings['target_accounts'] = target_accounts
                settings['unique_follows'] = unique_follows
            else:
                return jsonify({
                    'success': False,
                    'error': 'Настройки прогрева не найдены'
                }), 400
        
        # Проверяем наличие целевых аккаунтов
        if not target_accounts.strip():
            return jsonify({
                'success': False,
                'error': 'Не указаны аккаунты для подписки'
            }), 400
        
        # Подсчитываем количество целевых аккаунтов
        target_accounts_list = [acc.strip() for acc in target_accounts.split('\n') if acc.strip()]
        logger.info(f"🎯 Целевых аккаунтов: {len(target_accounts_list)}")
        logger.info(f"🔄 Уникальные подписки: {'включены' if unique_follows else 'отключены'}")
        
        # Проверяем, что включена хотя бы одна фаза
        if 'phases' in settings:
            enabled_phases = []
            for phase_name, phase_data in settings['phases'].items():
                if phase_data.get('enabled', False):
                    enabled_phases.append(phase_name)
            
            if not enabled_phases:
                return jsonify({
                    'success': False,
                    'error': 'Не включена ни одна фаза прогрева'
                }), 400
            
            logger.info(f"🎯 Включенные фазы: {', '.join(enabled_phases)}")
        
        # Создаем задачи прогрева для каждого аккаунта
        from database.db_manager import get_session
        from database.models import WarmupTask, WarmupStatus
        from datetime import datetime
        
        session = get_session()
        created_tasks = []
        
        try:
            for account_id in account_ids:
                # Проверяем, нет ли уже активной задачи для этого аккаунта
                existing_task = session.query(WarmupTask).filter(
                    WarmupTask.account_id == account_id,
                    WarmupTask.status.in_([WarmupStatus.PENDING, WarmupStatus.RUNNING])
                ).first()
                
                if existing_task:
                    logger.warning(f"⚠️ Для аккаунта {account_id} уже есть активная задача прогрева")
                    continue
                
                # Создаем новую задачу
                task = WarmupTask(
                    account_id=account_id,
                    settings=json.dumps(settings),
                    status=WarmupStatus.PENDING,
                    created_at=datetime.now()
                )
                session.add(task)
                created_tasks.append(account_id)
                if user_id:
                    log_user_info(user_id, f"🔥 Запущен прогрев (task создается) для аккаунта ID={account_id}")
            
            session.commit()
            
            # Запускаем асинхронный обработчик задач прогрева
            from utils.async_warmup_queue import start_async_warmup_queue
            
            # Получаем количество потоков из настроек
            max_workers = settings.get('max_concurrent_accounts', 3)
            start_async_warmup_queue(max_workers=max_workers)
            if user_id:
                log_user_info(user_id, f"🚀 Прогрев запущен для {len(created_tasks)} аккаунтов (max_workers={max_workers})")
            
            return jsonify({
                'success': True,
                'message': f'Прогрев запущен для {len(created_tasks)} аккаунтов',
                'started_accounts': created_tasks,
                'target_accounts_count': len(target_accounts_list),
                'unique_follows_enabled': unique_follows,
                'max_concurrent_accounts': max_workers
            })
            
        except Exception as e:
            if user_id:
                log_user_error(user_id, f"❌ Ошибка запуска прогрева: {e}")
            raise
    except Exception as e:
        logger.error(f"Ошибка запуска прогрева: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/warmup/status', methods=['GET'])
def get_warmup_status():
    """Получить статус прогрева аккаунтов"""
    try:
        from database.db_manager import get_session
        from database.models import WarmupTask, InstagramAccount
        
        session = get_session()
        
        # Получаем все задачи прогрева с информацией об аккаунтах
        tasks = session.query(
            WarmupTask,
            InstagramAccount.username
        ).join(
            InstagramAccount,
            WarmupTask.account_id == InstagramAccount.id
        ).order_by(
            WarmupTask.created_at.desc()
        ).all()
        
        # Формируем ответ
        tasks_data = []
        for task, username in tasks:
            # Логируем статус для отладки
            logger.debug(f"Task {task.id}: status={task.status}, value={task.status.value}")
            
            task_data = {
                'id': task.id,
                'account_id': task.account_id,
                'username': username,
                'status': task.status.value,
                'progress': task.progress or {},
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                'error': task.error
            }
            
            # Добавляем статистику из прогресса
            if task.progress:
                task_data['stats'] = {
                    'posts': task.progress.get('posts_created', 0),
                    'stories': task.progress.get('stories_created', 0),
                    'follows': task.progress.get('follows_made', 0),
                    'likes': task.progress.get('likes_given', 0),
                    'comments': task.progress.get('comments_made', 0)
                }
            else:
                task_data['stats'] = {
                    'posts': 0,
                    'stories': 0,
                    'follows': 0,
                    'likes': 0,
                    'comments': 0
                }
            
            tasks_data.append(task_data)
        
        session.close()
        
        return jsonify({
            'success': True,
            'data': tasks_data
        })
    
    except Exception as e:
        logger.error(f"❌ Ошибка при получении статуса прогрева: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/warmup/stop', methods=['POST'])
def stop_warmup():
    """Остановить прогрев для выбранных аккаунтов"""
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': 'Не выбраны аккаунты'
            }), 400
        
        from database.db_manager import get_session
        from database.models import WarmupTask, WarmupStatus
        from datetime import datetime
        
        session = get_session()
        stopped_count = 0
        
        try:
            for account_id in account_ids:
                # Находим активные задачи для аккаунта
                tasks = session.query(WarmupTask).filter(
                    WarmupTask.account_id == account_id,
                    WarmupTask.status.in_([WarmupStatus.PENDING, WarmupStatus.RUNNING])
                ).all()
                
                for task in tasks:
                    task.status = WarmupStatus.CANCELLED
                    task.completed_at = datetime.now()
                    stopped_count += 1
            
            session.commit()
            
            # Проверяем, остались ли еще активные задачи
            remaining_tasks = session.query(WarmupTask).filter(
                WarmupTask.status.in_([WarmupStatus.PENDING, WarmupStatus.RUNNING])
            ).count()
            
            # Если активных задач не осталось, останавливаем асинхронную очередь
            if remaining_tasks == 0:
                from utils.async_warmup_queue import stop_async_warmup_queue
                stop_async_warmup_queue()
                logger.info("🛑 Асинхронная очередь прогрева остановлена")
            
            return jsonify({
                'success': True,
                'message': f'Остановлено {stopped_count} задач прогрева'
            })
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"❌ Ошибка при остановке прогрева: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для автоподписок
# =============================================================================

@app.route('/api/follow/tasks', methods=['GET'])
def get_follow_tasks():
    """Получить список задач автоподписок"""
    try:
        from database.db_manager import get_session
        from database.models import FollowTask, InstagramAccount
        
        session = get_session()
        
        # Получаем все задачи с информацией об аккаунтах
        tasks = session.query(
            FollowTask,
            InstagramAccount.username
        ).join(
            InstagramAccount,
            FollowTask.account_id == InstagramAccount.id
        ).order_by(
            FollowTask.created_at.desc()
        ).all()
        
        # Формируем ответ
        tasks_data = []
        for task, username in tasks:
            task_data = {
                'id': task.id,
                'name': task.name,
                'account_id': task.account_id,
                'account_username': username,  # Исправлено с 'account'
                'source_type': task.source_type.value,
                'source_value': task.source_value,
                'status': task.status.value,
                'follows_per_hour': task.follows_per_hour,
                'follow_limit': task.follow_limit,
                'followed_count': task.followed_count,
                'skipped_count': task.skipped_count,
                'failed_count': task.failed_count,
                'filters': task.filters or {},
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                'last_action_at': task.last_action_at.isoformat() if task.last_action_at else None,
                'error': task.error
            }
            tasks_data.append(task_data)
        
        session.close()
        
        return jsonify({
            'success': True,
            'tasks': tasks_data  # Изменено с 'data' на 'tasks' для соответствия JavaScript
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении задач автоподписок: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/follow/tasks', methods=['POST'])
def create_follow_task():
    """Создать новую задачу автоподписки"""
    try:
        data = request.get_json()
        
        # Валидация данных
        required_fields = ['name', 'source_type', 'source_value']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'Отсутствует обязательное поле: {field}'
                }), 400
        
        # Получаем список аккаунтов
        account_ids = data.get('account_ids', [])
        if not account_ids:
            # Для обратной совместимости проверяем одиночный account_id
            single_account_id = data.get('account_id')
            if single_account_id:
                account_ids = [single_account_id]
            else:
                return jsonify({
                    'success': False,
                    'error': 'Не выбраны аккаунты'
                }), 400
        
        from database.db_manager import get_session
        from database.models import FollowTask, FollowSourceType, FollowTaskStatus, InstagramAccount
        from datetime import datetime
        
        session = get_session()
        created_tasks = []
        
        try:
            # Проверяем тип источника
            try:
                source_type = FollowSourceType(data['source_type'])
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': f'Неверный тип источника: {data["source_type"]}'
                }), 400
            
            # Получаем целевые аккаунты (если указаны)
            target_accounts = data.get('target_accounts', [])
            unique_follows = data.get('unique_follows', True)
            task_mode = data.get('task_mode', 'multiple')  # По умолчанию создаем отдельные задачи
            
            # Для прямых подписок на целевые аккаунты
            if source_type == 'direct' or (target_accounts and len(target_accounts) > 0):
                if task_mode == 'single':
                    # Режим "одна задача на аккаунт" - создаем одну задачу со всеми целями
                    for account_id in account_ids:
                        # Проверяем существование аккаунта
                        account = session.query(InstagramAccount).filter_by(id=account_id).first()
                        if not account:
                            logger.warning(f"Аккаунт {account_id} не найден")
                            continue
                        
                        # Создаем одну задачу со всеми целевыми аккаунтами
                        task_name = f"{data['name']} - @{account.username} ({len(target_accounts)} целей)"
                        
                        # Создаем задачу
                        task = FollowTask(
                            name=task_name,
                            account_id=account_id,
                            source_type=FollowSourceType.FOLLOWERS,
                            source_value='batch_follow',  # Специальное значение для пакетной подписки
                            follows_per_hour=data.get('follows_per_hour', 20),
                            follow_limit=len(target_accounts),  # Лимит равен количеству целей
                            filters={
                                **data.get('filters', {}),
                                'threads': data.get('threads', 1),
                                'delay_min': data.get('delay_min', 30),
                                'delay_max': data.get('delay_max', 90),
                                'target_accounts': target_accounts,  # Сохраняем список целей в фильтрах
                                'unique_follows': unique_follows
                            },
                            status=FollowTaskStatus.PENDING,
                            created_at=datetime.now()
                        )
                        
                        session.add(task)
                        created_tasks.append({
                            'account_id': account_id,
                            'account_username': account.username,
                            'targets_count': len(target_accounts)
                        })
                else:
                    # Режим "отдельная задача для каждой цели" (старое поведение)
                    for account_id in account_ids:
                        # Проверяем существование аккаунта
                        account = session.query(InstagramAccount).filter_by(id=account_id).first()
                        if not account:
                            logger.warning(f"Аккаунт {account_id} не найден")
                            continue
                        
                        for target_account in target_accounts:
                            # Создаем уникальное имя задачи
                            task_name = f"{data['name']} - @{account.username} → @{target_account}"
                            
                            # Создаем задачу
                            task = FollowTask(
                                name=task_name,
                                account_id=account_id,
                                source_type=FollowSourceType.FOLLOWERS,  # Используем followers как тип для прямых подписок
                                source_value=f"@{target_account}",  # Добавляем @ если его нет
                                follows_per_hour=data.get('follows_per_hour', 20),
                                follow_limit=1,  # Для прямой подписки лимит всегда 1
                                filters={
                                    **data.get('filters', {}),
                                    'threads': data.get('threads', 1),
                                    'delay_min': data.get('delay_min', 30),
                                    'delay_max': data.get('delay_max', 90)
                                },
                                status=FollowTaskStatus.PENDING,
                                created_at=datetime.now()
                            )
                            
                            session.add(task)
                            created_tasks.append({
                                'account_id': account_id,
                                'account_username': account.username,
                                'target': target_account
                            })
            else:
                # Обычная логика для подписок на подписчиков/подписки аккаунта
                if target_accounts and source_type in [FollowSourceType.FOLLOWERS, FollowSourceType.FOLLOWING]:
                    # Создаем задачу для каждого аккаунта и каждого целевого аккаунта
                    for account_id in account_ids:
                        # Проверяем существование аккаунта
                        account = session.query(InstagramAccount).filter_by(id=account_id).first()
                        if not account:
                            logger.warning(f"Аккаунт {account_id} не найден")
                            continue
                        
                        for target_account in target_accounts:
                            # Создаем уникальное имя задачи
                            task_name = f"{data['name']} - @{account.username} → {target_account}"
                            
                            # Создаем задачу
                            task = FollowTask(
                                name=task_name,
                                account_id=account_id,
                                source_type=source_type,
                                source_value=target_account,
                                follows_per_hour=data.get('follows_per_hour', 20),
                                follow_limit=data.get('follow_limit', 500),
                                filters={
                                    **data.get('filters', {}),
                                    'threads': data.get('threads', 1),
                                    'delay_min': data.get('delay_min', 30),
                                    'delay_max': data.get('delay_max', 90)
                                },
                                status=FollowTaskStatus.PENDING,
                                created_at=datetime.now()
                            )
                            
                            session.add(task)
                            created_tasks.append({
                                'account_id': account_id,
                                'account_username': account.username,
                                'target': target_account
                            })
                else:
                    # Создаем обычные задачи для каждого аккаунта
                    for account_id in account_ids:
                        # Проверяем существование аккаунта
                        account = session.query(InstagramAccount).filter_by(id=account_id).first()
                        if not account:
                            logger.warning(f"Аккаунт {account_id} не найден")
                            continue
                        
                        # Создаем уникальное имя задачи
                        task_name = f"{data['name']} - @{account.username}"
                        
                        # Создаем задачу
                        task = FollowTask(
                            name=task_name,
                            account_id=account_id,
                            source_type=source_type,
                            source_value=data['source_value'],
                            follows_per_hour=data.get('follows_per_hour', 20),
                            follow_limit=data.get('follow_limit', 500),
                            filters={
                                **data.get('filters', {}),
                                'threads': data.get('threads', 1),
                                'delay_min': data.get('delay_min', 30),
                                'delay_max': data.get('delay_max', 90)
                            },
                            status=FollowTaskStatus.PENDING,
                            created_at=datetime.now()
                        )
                        
                        session.add(task)
                        created_tasks.append({
                            'account_id': account_id,
                            'account_username': account.username,
                            'source': data['source_value']
                        })
            
            session.commit()
            
            # Запускаем асинхронную очередь если она не запущена
            from utils.async_follow_queue import start_async_follow_queue
            # Используем максимальное количество потоков из настроек
            max_threads = data.get('threads', 5)
            start_async_follow_queue(max_workers=max_threads)
            
            logger.info(f"✅ Создано {len(created_tasks)} задач автоподписки")
            
            return jsonify({
                'success': True,
                'message': f'Создано задач: {len(created_tasks)}',
                'created_tasks': created_tasks
            })
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка при создании задачи автоподписки: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/follow/tasks/<int:task_id>', methods=['PUT'])
def update_follow_task_status(task_id):
    """Обновить статус задачи (пауза/возобновление/остановка)"""
    try:
        data = request.get_json()
        status = data.get('status')
        
        from database.db_manager import get_session
        from database.models import FollowTask, FollowTaskStatus
        from datetime import datetime
        
        session = get_session()
        
        try:
            task = session.query(FollowTask).filter_by(id=task_id).first()
            if not task:
                return jsonify({
                    'success': False,
                    'error': 'Задача не найдена'
                }), 404
            
            # Преобразуем статус в нижний регистр для enum
            if status:
                status_lower = status.lower()
                try:
                    new_status = FollowTaskStatus(status_lower)
                    task.status = new_status
                    
                    # Обновляем временные метки
                    if new_status == FollowTaskStatus.RUNNING and not task.started_at:
                        task.started_at = datetime.now()
                    elif new_status in [FollowTaskStatus.COMPLETED, FollowTaskStatus.FAILED, FollowTaskStatus.CANCELLED]:
                        task.completed_at = datetime.now()
                    
                    message = f'Статус задачи обновлен на {status}'
                except ValueError:
                    return jsonify({
                        'success': False,
                        'error': f'Неверный статус: {status}'
                    }), 400
            else:
                # Обратная совместимость со старым API
                action = data.get('action')
                if action == 'pause':
                    if task.status == FollowTaskStatus.RUNNING:
                        task.status = FollowTaskStatus.PAUSED
                        message = 'Задача приостановлена'
                    else:
                        return jsonify({
                            'success': False,
                            'error': 'Задача не активна'
                        }), 400
                elif action == 'resume':
                    if task.status == FollowTaskStatus.PAUSED:
                        task.status = FollowTaskStatus.RUNNING
                        message = 'Задача возобновлена'
                    else:
                        return jsonify({
                            'success': False,
                            'error': 'Задача не на паузе'
                        }), 400
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Не указан статус или действие'
                    }), 400
            
            session.commit()
            
            return jsonify({
                'success': True,
                'message': message
            })
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении статуса задачи: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/follow/tasks/<int:task_id>', methods=['DELETE'])
def delete_follow_task(task_id):
    """Удалить задачу автоподписки"""
    try:
        from database.db_manager import get_session
        from database.models import FollowTask, FollowTaskStatus
        from datetime import datetime
        
        session = get_session()
        
        try:
            task = session.query(FollowTask).filter_by(id=task_id).first()
            if not task:
                return jsonify({
                    'success': False,
                    'error': 'Задача не найдена'
                }), 404
            
            # Если задача активна, сначала отменяем её
            if task.status in [FollowTaskStatus.RUNNING, FollowTaskStatus.PENDING]:
                task.status = FollowTaskStatus.CANCELLED
                task.completed_at = datetime.now()
                session.commit()
                
                # Даём время на завершение
                import time
                time.sleep(1)
            
            # Удаляем задачу
            session.delete(task)
            session.commit()
            
            # Проверяем, остались ли активные задачи
            active_tasks = session.query(FollowTask).filter(
                FollowTask.status.in_([FollowTaskStatus.PENDING, FollowTaskStatus.RUNNING])
            ).count()
            
            # Если активных задач не осталось, останавливаем очередь
            if active_tasks == 0:
                from utils.async_follow_queue import stop_async_follow_queue
                stop_async_follow_queue()
                logger.info("🛑 Асинхронная очередь автоподписок остановлена")
            
            return jsonify({
                'success': True,
                'message': 'Задача успешно удалена'
            })
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении задачи: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/follow/tasks/stop-all', methods=['POST'])
def stop_all_follow_tasks():
    """Остановить все активные задачи автоподписок"""
    try:
        from database.db_manager import get_session
        from database.models import FollowTask, FollowTaskStatus
        from datetime import datetime
        
        session = get_session()
        
        try:
            # Получаем все активные задачи
            active_tasks = session.query(FollowTask).filter(
                FollowTask.status.in_([
                    FollowTaskStatus.RUNNING, 
                    FollowTaskStatus.PENDING,
                    FollowTaskStatus.PAUSED
                ])
            ).all()
            
            stopped_count = 0
            
            # Останавливаем каждую задачу
            for task in active_tasks:
                task.status = FollowTaskStatus.STOPPED
                task.completed_at = datetime.now()
                if task.status == FollowTaskStatus.PENDING and not task.started_at:
                    task.started_at = datetime.now()
                stopped_count += 1
            
            session.commit()
            
            # Если остановили все задачи, останавливаем очередь
            if stopped_count > 0:
                from utils.async_follow_queue import stop_async_follow_queue
                stop_async_follow_queue()
                logger.info(f"🛑 Остановлено {stopped_count} задач автоподписок")
            
            return jsonify({
                'success': True,
                'stopped_count': stopped_count,
                'message': f'Остановлено {stopped_count} задач'
            })
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка при остановке всех задач: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/follow/stats', methods=['GET'])
def get_follow_stats():
    """Получить статистику автоподписок"""
    try:
        from database.db_manager import get_session
        from database.models import FollowTask, FollowHistory, FollowTaskStatus
        from datetime import datetime, timedelta
        from sqlalchemy import func
        
        session = get_session()
        
        # Статистика по задачам
        active_tasks = session.query(FollowTask).filter(
            FollowTask.status == FollowTaskStatus.RUNNING
        ).count()
        
        # Подписок сегодня
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_follows = session.query(FollowHistory).filter(
            FollowHistory.followed_at >= today_start
        ).count()
        
        # Всего подписок
        total_followed = session.query(func.sum(FollowTask.followed_count)).scalar() or 0
        
        # Всего обработано (подписки + пропущенные + ошибки)
        total_processed = session.query(
            func.sum(FollowTask.followed_count + FollowTask.skipped_count + FollowTask.failed_count)
        ).scalar() or 0
        
        # Процент успешных подписок
        if total_processed > 0:
            success_rate = (total_followed / total_processed) * 100
        else:
            success_rate = 0
        
        session.close()
        
        return jsonify({
            'success': True,
            'active_tasks': active_tasks,
            'today_follows': today_follows,
            'total_followed': total_followed,
            'success_rate': round(success_rate, 1)
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении статистики: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# API для валидации аккаунтов
# =============================================================================

@app.route('/api/accounts/validate', methods=['POST'])
def validate_accounts():
    """Запустить проверку валидности аккаунтов"""
    try:
        from utils.smart_validator_service import get_smart_validator, ValidationPriority
        from database.db_manager import get_instagram_accounts
        
        # Получаем сервис
        validator = get_smart_validator()
        
        # Получаем все аккаунты и добавляем в очередь проверки с высоким приоритетом
        accounts = get_instagram_accounts()
        for account in accounts:
            validator.request_validation(account.id, ValidationPriority.HIGH)
        
        logger.info(f"🔍 Добавлено {len(accounts)} аккаунтов в очередь проверки")
        
        # Получаем текущую статистику
        stats = validator.get_stats()
        
        return jsonify({
            'success': True,
            'message': f'Добавлено {len(accounts)} аккаунтов в очередь проверки',
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске проверки валидности: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/validate/status', methods=['GET'])
def get_validation_status():
    """Получить статус и результаты валидации"""
    try:
        from utils.smart_validator_service import get_smart_validator
        
        validator = get_smart_validator()
        stats = validator.get_stats()
        load = validator.get_system_load()
        
        return jsonify({
            'success': True,
            'is_running': validator.is_running,
            'check_interval_minutes': validator.check_interval // 60,
            'auto_repair': True,  # Всегда включено в умном валидаторе
            'stats': stats,
            'system_load': {
                'cpu': load.cpu_usage,
                'memory': load.memory_usage,
                'is_high': load.is_high_load
            },
            'last_results': {
                'valid': stats['status_counts'].get('valid', 0),
                'invalid': stats['status_counts'].get('invalid', 0),
                'repaired': stats['status_counts'].get('valid', 0),  # Считаем восстановленные как валидные
                'failed_repair': stats['status_counts'].get('failed', 0)
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении статуса валидации: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/validate/settings', methods=['PUT'])
def update_validation_settings():
    """Обновить настройки сервиса валидации"""
    try:
        data = request.get_json()
        from utils.smart_validator_service import get_smart_validator
        
        validator = get_smart_validator()
        
        # Обновляем настройки
        if 'check_interval_minutes' in data:
            validator.check_interval = data['check_interval_minutes'] * 60
            
        # Другие настройки можно добавить здесь
        # auto_repair всегда включен в умном валидаторе
        
        # Не нужно перезапускать сервис - он автоматически применит новые настройки
        
        return jsonify({
            'success': True,
            'message': 'Настройки обновлены',
            'settings': {
                'check_interval_minutes': validator.check_interval // 60,
                'auto_repair': True,
                'max_concurrent_checks': validator.max_concurrent_checks,
                'max_concurrent_recoveries': validator.max_concurrent_recoveries
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении настроек валидации: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/<int:account_id>/check', methods=['POST'])
def check_single_account(account_id):
    """Проверить валидность одного аккаунта"""
    try:
        from utils.smart_validator_service import get_smart_validator, ValidationPriority
        from database.db_manager import get_instagram_account
        
        # Получаем аккаунт
        account = get_instagram_account(account_id)
        if not account:
            return jsonify({
                'success': False,
                'error': 'Аккаунт не найден'
            }), 404
        
        logger.info(f"🔍 Запуск проверки аккаунта @{account.username}")
        
        # Получаем сервис валидации
        validator = get_smart_validator()
        
        # Запрашиваем проверку с критическим приоритетом
        validator.request_validation(account_id, ValidationPriority.CRITICAL)
        
        # Ждем результат (максимум 10 секунд)
        import time
        for _ in range(10):
            status = validator.get_account_status(account_id)
            if status and status.value != 'checking':
                is_valid = validator.is_account_valid(account_id)
                
                return jsonify({
                    'success': True,
                    'is_valid': is_valid,
                    'status': status.value,
                    'username': account.username
                })
            time.sleep(1)
        
        # Если не дождались результата
        return jsonify({
            'success': True,
            'is_valid': None,
            'status': 'checking',
            'username': account.username,
            'message': 'Проверка добавлена в очередь'
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке аккаунта: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/accounts/validate/stop', methods=['POST'])
def stop_validation():
    """Остановить сервис валидации"""
    try:
        from utils.smart_validator_service import get_smart_validator
        
        validator = get_smart_validator()
        validator.stop()
        
        return jsonify({
            'success': True,
            'message': 'Сервис валидации остановлен'
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при остановке валидации: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# =============================================================================
# Запуск сервера
# =============================================================================

if __name__ == '__main__':
    logger.info("Запуск Web API сервера...")
    
    # Проверяем, что директория веб-дашборда существует
    if not os.path.exists('web-dashboard'):
        logger.error("Директория web-dashboard не найдена!")
        sys.exit(1)
    
    # Запускаем обработчик очереди задач
    from utils.task_queue import start_task_queue
    start_task_queue()
    
    # Запускаем умный фоновый сервис проверки валидности аккаунтов
    try:
        from utils.smart_validator_service import get_smart_validator
        smart_validator = get_smart_validator()
        smart_validator.start()
        logger.info("✅ Умный сервис проверки аккаунтов запущен")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось запустить умный сервис проверки аккаунтов: {e}")
    
    # Запускаем сервер
    app.run(
        host='0.0.0.0',
        port=8080,
        debug=True,
        use_reloader=False  # Отключаем reloader чтобы избежать двойного запуска
    ) 