"""
Патч для метода clip_upload в instagrapi (УДАЛЕН)
Instagram автоматически разделяет Reels и обычные посты в разные разделы.
Дополнительная логика скрытия/показа в ленте больше не требуется.
"""
import logging

logger = logging.getLogger(__name__)

# Патч удален - Instagram автоматически управляет отображением Reels
logger.info("Патч clip_upload не применяется - Instagram автоматически разделяет Reels и посты")