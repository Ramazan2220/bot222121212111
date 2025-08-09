import logging
import imaplib
import email as email_lib
from email.header import decode_header
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from .imap_pool import get_imap_connection

logger = logging.getLogger(__name__)

class OptimizedIMAPLogger:
    """Минимальное логирование для продакшена"""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.stats = {}
    
    def log_search_start(self, email_addr: str, attempt: int):
        logger.info(f"🔍 Поиск кода для {email_addr} (попытка {attempt})")
        self.stats[email_addr] = {'start_time': datetime.now(), 'searches': 0, 'emails_found': 0}
    
    def log_folder_result(self, email_addr: str, folder: str, count: int):
        if count > 0:
            logger.info(f"📧 Найдено {count} писем в {folder}")
            self.stats[email_addr]['emails_found'] += count
        elif self.verbose:
            logger.debug(f"🔍 Папка {folder}: пусто")
        self.stats[email_addr]['searches'] += 1
    
    def log_code_found(self, email_addr: str, code: str):
        stats = self.stats[email_addr]
        duration = (datetime.now() - stats['start_time']).total_seconds()
        logger.info(f"✅ Код найден для {email_addr}: {code} (за {duration:.1f}с, {stats['searches']} поисков)")
    
    def log_code_not_found(self, email_addr: str, attempt: int, max_attempts: int):
        stats = self.stats[email_addr]
        duration = (datetime.now() - stats['start_time']).total_seconds()
        
        if attempt >= max_attempts:
            logger.warning(f"❌ Код НЕ найден для {email_addr} после {attempt} попыток "
                         f"({stats['searches']} поисков, {stats['emails_found']} писем, {duration:.1f}с)")
        elif self.verbose:
            logger.debug(f"⏳ Попытка {attempt}/{max_attempts} неудачна")
    
    def log_error(self, email_addr: str, error_type: str, error_msg: str):
        logger.error(f"⚠️ IMAP ошибка для {email_addr} ({error_type}): {error_msg}")

def extract_verification_code_optimized(subject: str, text: str, html: str = None) -> Optional[str]:
    """Оптимизированное извлечение кода верификации"""
    
    # Исключенные коды
    excluded_codes = {'262626', '999999', '000000', '123456', '111111'}
    
    # Основные паттерны (объединяем самые важные)
    patterns = [
        r'[Кк]од(?:\s+подтверждения|\s+безопасности)?:\s*<?(\d{6})>?',
        r'[Cc]ode:\s*<?(\d{6})>?',
        r'[Вв]ерификационный\s+код:\s*<?(\d{6})>?',
        r'verification\s+code(?:\s+is)?:\s*<?(\d{6})>?',
        r'security\s+code(?:\s+is)?:\s*<?(\d{6})>?',
        r'confirm(?:ation)?\s+code(?:\s+is)?:\s*<?(\d{6})>?',
        r'(\d{6})\s*(?:is\s+your|ваш)\s+(?:verification|security|confirmation|код)',
        r'(\d{6})\s*-\s*ваш\s+код',
        r'code\s+(\d{6})',
        r'(\d{6})',  # Общий поиск любого 6-значного числа
    ]
    
    # Ищем в тексте и HTML
    for content in [subject, text, html or ""]:
        if not content:
            continue
            
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.isdigit() and len(match) == 6 and match not in excluded_codes:
                    return match
    
    return None

def get_verification_code_optimized(email_address: str, email_password: str, 
                                  since_time: datetime = None, max_attempts: int = 3, 
                                  verbose: bool = False) -> Optional[str]:
    """
    Оптимизированная версия получения кода верификации с пулом соединений
    
    Args:
        email_address: Адрес электронной почты
        email_password: Пароль от почты  
        since_time: Время начала поиска (по умолчанию - 3 минуты назад)
        max_attempts: Количество попыток (по умолчанию 3)
        verbose: Включить детальное логирование
    
    Returns:
        Код верификации или None
    """
    
    imap_logger = OptimizedIMAPLogger(verbose=verbose)
    
    if since_time is None:
        since_time = datetime.now(timezone.utc) - timedelta(minutes=3)
    
    for attempt in range(1, max_attempts + 1):
        imap_logger.log_search_start(email_address, attempt)
        
        try:
            # Используем пул соединений
            with get_imap_connection(email_address, email_password) as connection:
                if not connection:
                    imap_logger.log_error(email_address, "connection", "Не удалось получить IMAP соединение")
                    continue
                
                mail = connection.mail
                code = None
                
                # Поиск в основных папках
                folders = ['INBOX', 'Junk']  # Убираем Spam так как его часто нет
                
                for folder in folders:
                    try:
                        status, _ = mail.select(folder)
                        if status != 'OK':
                            continue
                        
                        # Оптимизированные критерии поиска (только самые важные)
                        since_date = since_time.strftime("%d-%b-%Y")
                        search_criteria = [
                            f'(SINCE "{since_date}" FROM "security@mail.instagram.com")',
                            f'(SINCE "{since_date}" SUBJECT "Instagram")',
                            f'(SINCE "{since_date}" SUBJECT "security code")',
                            f'(SINCE "{since_date}")',  # Общий поиск как fallback
                        ]
                        
                        email_ids = set()
                        
                        # Выполняем поиски (без детального логирования каждого)
                        for criteria in search_criteria:
                            try:
                                status, messages = mail.search(None, criteria)
                                if status == 'OK' and messages[0]:
                                    new_ids = messages[0].split()
                                    email_ids.update(new_ids)
                                    if verbose and new_ids:
                                        logger.debug(f"🔍 {criteria}: {len(new_ids)} писем")
                            except Exception as e:
                                if verbose:
                                    logger.debug(f"⚠️ Ошибка поиска {criteria}: {e}")
                        
                        imap_logger.log_folder_result(email_address, folder, len(email_ids))
                        
                        if not email_ids:
                            continue
                        
                        # Обрабатываем письма (сортировка по дате)
                        emails_data = []
                        for email_id in email_ids:
                            try:
                                # Получаем заголовки для фильтрации
                                status, msg_data = mail.fetch(email_id, "(BODY[HEADER.FIELDS (SUBJECT DATE FROM)])")
                                if status == 'OK':
                                    header_msg = email_lib.message_from_bytes(msg_data[0][1])
                                    
                                    # Парсим дату
                                    date_str = header_msg.get('Date')
                                    email_date = None
                                    if date_str:
                                        try:
                                            email_date = email_lib.utils.parsedate_to_datetime(date_str)
                                        except:
                                            pass
                                    
                                    emails_data.append({
                                        'id': email_id,
                                        'date': email_date or datetime.min.replace(tzinfo=timezone.utc),
                                        'from': header_msg.get('From', ''),
                                        'subject': header_msg.get('Subject', '')
                                    })
                            except Exception as e:
                                if verbose:
                                    logger.debug(f"⚠️ Ошибка обработки письма {email_id}: {e}")
                        
                        # Сортируем по дате (новые сначала)
                        emails_data.sort(key=lambda x: x['date'], reverse=True)
                        
                        # Проверяем письма
                        for email_data in emails_data:
                            try:
                                # Проверяем время письма
                                if email_data['date'] < since_time:
                                    if verbose:
                                        logger.debug(f"⏰ Письмо слишком старое: {email_data['date']}")
                                    continue
                                
                                # Получаем полное содержимое письма
                                status, msg_data = mail.fetch(email_data['id'], "(RFC822)")
                                if status != 'OK':
                                    continue
                                
                                full_msg = email_lib.message_from_bytes(msg_data[0][1])
                                
                                # Извлекаем тему
                                subject = ""
                                subject_header = full_msg.get("Subject", "")
                                if subject_header:
                                    decoded = decode_header(subject_header)
                                    for part, charset in decoded:
                                        if isinstance(part, bytes):
                                            subject += part.decode(charset or 'utf-8', 'replace')
                                        else:
                                            subject += part
                                
                                # Извлекаем текст
                                text_content = ""
                                html_content = ""
                                
                                if full_msg.is_multipart():
                                    for part in full_msg.walk():
                                        content_type = part.get_content_type()
                                        if content_type == "text/plain":
                                            text_content = part.get_payload(decode=True).decode('utf-8', 'replace')
                                        elif content_type == "text/html":
                                            html_content = part.get_payload(decode=True).decode('utf-8', 'replace')
                                else:
                                    text_content = full_msg.get_payload(decode=True).decode('utf-8', 'replace')
                                
                                # Ищем код
                                code = extract_verification_code_optimized(subject, text_content, html_content)
                                
                                if code:
                                    imap_logger.log_code_found(email_address, code)
                                    return code
                                
                                if verbose:
                                    logger.debug(f"🔍 Код не найден в письме: {subject[:50]}...")
                            
                            except Exception as e:
                                if verbose:
                                    logger.debug(f"⚠️ Ошибка проверки письма: {e}")
                        
                        # Если нашли код в этой папке, выходим
                        if code:
                            break
                    
                    except Exception as e:
                        imap_logger.log_error(email_address, f"folder_{folder}", str(e))
                
                if code:
                    return code
        
        except Exception as e:
            imap_logger.log_error(email_address, "connection", str(e))
        
        # Логируем неудачу
        imap_logger.log_code_not_found(email_address, attempt, max_attempts)
        
        # Пауза перед следующей попыткой
        if attempt < max_attempts:
            time.sleep(15)
    
    return None

# Обратная совместимость с существующим кодом
def get_verification_code_from_email(email, password, max_attempts=3, delay_between_attempts=5):
    """Обратная совместимость со старым API"""
    return get_verification_code_optimized(
        email_address=email,
        email_password=password, 
        max_attempts=max_attempts,
        verbose=False  # Минимальное логирование для продакшена
    ) 