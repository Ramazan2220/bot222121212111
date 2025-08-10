# -*- coding: utf-8 -*-
"""
Утилиты для асинхронной обработки handlers без блокировки
"""

import threading
import logging
from functools import wraps
from telegram import Update, CallbackQuery
from telegram.ext import CallbackContext
import time

logger = logging.getLogger(__name__)

def async_handler(show_loading=True, loading_text="⏳ Обработка..."):
    """
    Декоратор для выполнения handler'а в отдельном потоке
    Предотвращает подвисание кнопок
    Поддерживает как обычные функции, так и методы класса
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Определяем является ли это методом класса или обычной функцией
            if len(args) >= 3:
                # Метод класса: self, update, context
                self_arg, update, context = args[0], args[1], args[2]
                remaining_args = args[3:]
            elif len(args) >= 2:
                # Обычная функция: update, context
                self_arg = None
                update, context = args[0], args[1]
                remaining_args = args[2:]
            else:
                # Недостаточно аргументов
                return func(*args, **kwargs)
            
            query = update.callback_query
            
            # Сразу отвечаем на callback, чтобы убрать часики
            if query:
                query.answer()
                
                # Показываем индикатор загрузки
                if show_loading:
                    original_text = query.message.text
                    original_markup = query.message.reply_markup
                    
                    try:
                        query.edit_message_text(loading_text)
                    except:
                        pass  # Игнорируем ошибки редактирования
            
            # Запускаем обработку в отдельном потоке
            def run_async():
                try:
                    # Выполняем основную функцию с правильными аргументами
                    if self_arg is not None:
                        # Метод класса
                        result = func(self_arg, update, context, *remaining_args, **kwargs)
                    else:
                        # Обычная функция
                        result = func(update, context, *remaining_args, **kwargs)
                    
                    # Если была ошибка и показывали загрузку - восстанавливаем
                    if query and show_loading and result is False:
                        try:
                            query.edit_message_text(
                                original_text,
                                reply_markup=original_markup
                            )
                        except:
                            pass
                            
                except Exception as e:
                    logger.error(f"Ошибка в async_handler: {e}")
                    
                    # Показываем ошибку пользователю
                    if query:
                        try:
                            query.edit_message_text(
                                f"❌ Произошла ошибка:\n{str(e)[:200]}"
                            )
                        except:
                            pass
            
            thread = threading.Thread(target=run_async)
            thread.daemon = True
            thread.start()
            
        return wrapper
    return decorator


def progress_handler(total_steps=100):
    """
    Декоратор для отображения прогресса выполнения
    """
    def decorator(func):
        @wraps(func)
        def wrapper(update: Update, context: CallbackContext):
            query = update.callback_query
            if query:
                query.answer()
            
            # Создаем объект для отслеживания прогресса
            progress = {
                'current': 0,
                'total': total_steps,
                'last_update': 0
            }
            
            def update_progress(current, message=""):
                """Обновить прогресс"""
                progress['current'] = current
                
                # Обновляем не чаще раза в секунду
                now = time.time()
                if now - progress['last_update'] < 1:
                    return
                    
                progress['last_update'] = now
                
                # Рассчитываем процент
                percent = int((current / progress['total']) * 100)
                
                # Создаем прогресс-бар
                bar_length = 20
                filled = int(bar_length * percent / 100)
                bar = '█' * filled + '░' * (bar_length - filled)
                
                text = f"⏳ Выполнение: {percent}%\n[{bar}]\n"
                if message:
                    text += f"\n{message}"
                
                try:
                    query.edit_message_text(text)
                except:
                    pass
            
            # Передаем функцию обновления прогресса
            context.user_data['update_progress'] = update_progress
            
            # Выполняем основную функцию
            return func(update, context)
            
        return wrapper
    return decorator


def chunked_handler(chunk_size=10):
    """
    Декоратор для обработки больших списков по частям
    Предотвращает timeout'ы
    """
    def decorator(func):
        @wraps(func)
        def wrapper(update: Update, context: CallbackContext):
            query = update.callback_query
            if query:
                query.answer()
            
            # Получаем список для обработки
            items = context.user_data.get('items_to_process', [])
            if not items:
                return func(update, context)
            
            # Обрабатываем по частям
            total_items = len(items)
            processed = 0
            
            for i in range(0, total_items, chunk_size):
                chunk = items[i:i + chunk_size]
                context.user_data['current_chunk'] = chunk
                
                # Обновляем прогресс
                processed += len(chunk)
                percent = int((processed / total_items) * 100)
                
                try:
                    query.edit_message_text(
                        f"⏳ Обработано: {processed}/{total_items} ({percent}%)"
                    )
                except:
                    pass
                
                # Вызываем функцию для обработки chunk'а
                func(update, context)
                
                # Небольшая пауза между chunk'ами
                time.sleep(0.1)
            
            # Очищаем данные
            context.user_data.pop('items_to_process', None)
            context.user_data.pop('current_chunk', None)
            
        return wrapper
    return decorator


class LoadingContext:
    """Контекст-менеджер для индикатора загрузки"""
    
    def __init__(self, query: CallbackQuery, text="⏳ Загрузка..."):
        self.query = query
        self.loading_text = text
        self.original_text = None
        self.original_markup = None
        
    def __enter__(self):
        if self.query:
            self.query.answer()
            self.original_text = self.query.message.text
            self.original_markup = self.query.message.reply_markup
            
            try:
                self.query.edit_message_text(self.loading_text)
            except:
                pass
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Если была ошибка - восстанавливаем оригинальное сообщение
        if exc_type is not None and self.query:
            try:
                self.query.edit_message_text(
                    self.original_text,
                    reply_markup=self.original_markup
                )
            except:
                pass
        return False


def answer_callback_async(query: CallbackQuery, text: str = None, show_alert: bool = False):
    """Асинхронный ответ на callback"""
    def answer():
        try:
            query.answer(text=text, show_alert=show_alert)
        except:
            pass
    
    thread = threading.Thread(target=answer)
    thread.daemon = True
    thread.start() 