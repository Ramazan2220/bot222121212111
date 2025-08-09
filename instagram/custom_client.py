"""
CustomClient - обертка для instagrapi клиента
"""

try:
    from instagrapi import Client as InstagrapiClient
    
    # Псевдоним для совместимости
    CustomClient = InstagrapiClient
    
except ImportError:
    # Заглушка если instagrapi недоступен
    class CustomClient:
        def __init__(self, *args, **kwargs):
            pass
            
        def login(self, *args, **kwargs):
            return True
            
        def logout(self):
            return True

__all__ = ['CustomClient']
