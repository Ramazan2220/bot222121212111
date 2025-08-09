import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

from database.db_manager import get_instagram_account
from instagram.profile_manager import ProfileManager
from profile_setup import EDIT_LINKS

logger = logging.getLogger(__name__)

def edit_profile_links(update: Update, context: CallbackContext) -> int:
    """Запрашивает новые ссылки профиля"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    # Отправляем сообщение о загрузке
    loading_message = query.message.reply_text("⏳ Подключение к Instagram... Пожалуйста, подождите.")

    # Получаем текущие ссылки профиля
    try:
        profile_manager = ProfileManager(account_id)
        current_link = profile_manager.get_profile_links()
        
        # Проверяем возможность добавления ссылок
        can_add_links = profile_manager.check_account_eligibility()

        # Удаляем сообщение о загрузке
        loading_message.delete()

        current_link_text = "Не указана" if not current_link else current_link
        
        # Формируем сообщение с учетом возможностей аккаунта
        if can_add_links:
            message_text = (
                f"✅ Ваш аккаунт поддерживает внешние ссылки!\n\n"
                f"Текущая ссылка в профиле: {current_link_text}\n\n"
                f"Введите новую ссылку для профиля Instagram (например, example.com):"
            )
        else:
            message_text = (
                f"⚠️ Ваш аккаунт может не поддерживать внешние ссылки в профиле.\n"
                f"Обычно Instagram разрешает ссылки только:\n"
                f"• Бизнес-аккаунтам\n"
                f"• Верифицированным аккаунтам\n"
                f"• Аккаунтам с 10k+ подписчиков\n\n"
                f"Текущая ссылка: {current_link_text}\n\n"
                f"Мы попробуем добавить ссылку или разместить её в описании профиля.\n"
                f"Введите ссылку:"
            )

        query.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
            ])
        )

        return EDIT_LINKS
    except Exception as e:
        logger.error(f"Ошибка при получении ссылок профиля: {e}")

        # Удаляем сообщение о загрузке
        loading_message.delete()

        query.message.reply_text(
            "❌ Произошла ошибка при получении ссылок профиля. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )
        return ConversationHandler.END

def save_profile_links(update: Update, context: CallbackContext) -> int:
    """Сохраняет новые ссылки профиля"""
    links_text = update.message.text
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление ссылок профиля...")

    try:
        # Берем только первую ссылку, так как Instagram поддерживает только одну
        link = links_text.strip()
        if '|' in link:
            _, url = link.split('|', 1)
            link = url.strip()

        # Добавляем протокол если отсутствует
        if not link.startswith(('http://', 'https://')):
            link = f"https://{link}"

        # Создаем менеджер профиля и обновляем ссылку
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_profile_links(link)

        if success:
            # Отправляем сообщение об успехе
            success_message = "✅ Ссылка профиля успешно обновлена!"
            
            # Проверяем, была ли ссылка добавлена в био
            if "описание профиля" in result:
                success_message += "\n\n💡 Ссылка была добавлена в описание профиля, так как ваш аккаунт может не поддерживать прямые внешние ссылки."
            
            update.message.reply_text(
                success_message,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке с дополнительными рекомендациями
            error_message = f"❌ Ошибка при обновлении ссылки профиля: {result}\n\n"
            error_message += "💡 Возможные решения:\n"
            error_message += "• Убедитесь, что аккаунт авторизован\n"
            error_message += "• Проверьте корректность ссылки\n"
            error_message += "• Попробуйте конвертировать аккаунт в бизнес-аккаунт\n"
            error_message += "• Свяжитесь с поддержкой, если проблема повторяется"
            
            # Добавляем кнопку для конвертации в бизнес-аккаунт
            keyboard = [
                [InlineKeyboardButton("🏢 Конвертировать в бизнес-аккаунт", callback_data=f"convert_business_{account_id}")],
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ]
            
            update.message.reply_text(
                error_message,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении ссылки профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def convert_to_business_account(update: Update, context: CallbackContext) -> None:
    """Конвертирует аккаунт в бизнес-аккаунт"""
    query = update.callback_query
    query.answer()
    
    # Извлекаем account_id из callback_data
    account_id = int(query.data.split('_')[-1])
    
    # Отправляем сообщение о начале процесса
    query.edit_message_text("⏳ Конвертация аккаунта в бизнес-аккаунт...")
    
    try:
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.convert_to_business_account()
        
        if success:
            query.edit_message_text(
                f"✅ {result}\n\n"
                f"Теперь ваш аккаунт может поддерживать внешние ссылки в профиле!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Попробовать добавить ссылку", callback_data="profile_edit_links")],
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            query.edit_message_text(
                f"❌ {result}\n\n"
                f"💡 Альтернативы:\n"
                f"• Попробуйте конвертировать аккаунт через мобильное приложение Instagram\n"
                f"• Наберите 10k+ подписчиков для получения доступа к ссылкам\n"
                f"• Попробуйте верифицировать аккаунт",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
            
    except Exception as e:
        logger.error(f"Ошибка при конвертации в бизнес-аккаунт: {e}")
        query.edit_message_text(
            "❌ Произошла ошибка при конвертации аккаунта. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

def update_profile_link_with_text(update, context):
    """Обновление ссылки профиля с текстовым вводом"""
    message = update.message
    account_id = context.user_data.get('account_id')
    link = message.text.strip()
    
    # Показываем сообщение о процессе
    processing_message = message.reply_text("⏳ Обновляю ссылку профиля...")
    
    try:
        # Получаем менеджер ссылок
        links_manager = LinksManager(account_id)
        
        # Обновляем ссылку
        success, result = links_manager.set_external_url(link)
        
        if success:
            success_message = f"✅ Ссылка профиля успешно обновлена!\n\n"
            success_message += f"🔗 Новая ссылка: {link}\n\n"
            success_message += "Изменения могут появиться в профиле через несколько минут."
            
            update.message.reply_text(
                success_message,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке с дополнительными рекомендациями
            error_message = f"❌ Ошибка при обновлении ссылки профиля: {result}\n\n"
            error_message += "💡 Возможные решения:\n"
            error_message += "• Убедитесь, что аккаунт авторизован\n"
            error_message += "• Проверьте корректность ссылки\n"
            error_message += "• Попробуйте конвертировать аккаунт в бизнес-аккаунт\n"
            error_message += "• Свяжитесь с поддержкой, если проблема повторяется"
            
            # Добавляем кнопку для конвертации в бизнес-аккаунт
            keyboard = [
                [InlineKeyboardButton("🏢 Конвертировать в бизнес-аккаунт", callback_data=f"convert_business_{account_id}")],
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ]
            
            update.message.reply_text(
                error_message,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении ссылки профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    processing_message.delete()
