import os
import random
from PIL import Image, ImageEnhance, ImageFilter
import logging

logger = logging.getLogger(__name__)

def uniquify_image(image_path, index=0):
    """
    Уникализирует изображение путем небольших изменений
    
    Args:
        image_path: путь к исходному изображению
        index: индекс для определения типа изменений
        
    Returns:
        str: путь к уникализированному изображению
    """
    try:
        # Открываем изображение
        img = Image.open(image_path)
        
        # Конвертируем в RGB если нужно
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Применяем различные изменения в зависимости от индекса
        modifications = [
            lambda img: adjust_brightness(img, 1.05 + (index * 0.02)),  # Яркость
            lambda img: adjust_contrast(img, 1.05 + (index * 0.02)),    # Контраст
            lambda img: adjust_saturation(img, 1.05 + (index * 0.02)),  # Насыщенность
            lambda img: apply_slight_blur(img, 0.3 + (index * 0.1)),    # Легкое размытие
            lambda img: adjust_sharpness(img, 1.05 + (index * 0.02)),   # Резкость
            lambda img: rotate_slightly(img, 0.5 * (index % 3 - 1)),    # Легкий поворот
            lambda img: crop_slightly(img, 2 + index),                  # Легкая обрезка
        ]
        
        # Выбираем тип модификации
        mod_index = index % len(modifications)
        img = modifications[mod_index](img)
        
        # Также добавляем небольшой случайный шум для большей уникальности
        img = add_slight_noise(img, index)
        
        # Сохраняем уникализированное изображение
        base_name = os.path.basename(image_path)
        name, ext = os.path.splitext(base_name)
        unique_name = f"{name}_unique_{index}{ext}"
        unique_path = os.path.join(os.path.dirname(image_path), unique_name)
        
        img.save(unique_path, quality=95)
        logger.info(f"✅ Изображение уникализировано: {unique_path}")
        
        return unique_path
        
    except Exception as e:
        logger.error(f"❌ Ошибка при уникализации изображения: {e}")
        return image_path

def adjust_brightness(img, factor):
    """Изменяет яркость изображения"""
    enhancer = ImageEnhance.Brightness(img)
    return enhancer.enhance(factor)

def adjust_contrast(img, factor):
    """Изменяет контраст изображения"""
    enhancer = ImageEnhance.Contrast(img)
    return enhancer.enhance(factor)

def adjust_saturation(img, factor):
    """Изменяет насыщенность изображения"""
    enhancer = ImageEnhance.Color(img)
    return enhancer.enhance(factor)

def adjust_sharpness(img, factor):
    """Изменяет резкость изображения"""
    enhancer = ImageEnhance.Sharpness(img)
    return enhancer.enhance(factor)

def apply_slight_blur(img, radius):
    """Применяет легкое размытие"""
    return img.filter(ImageFilter.GaussianBlur(radius=radius))

def rotate_slightly(img, angle):
    """Слегка поворачивает изображение"""
    return img.rotate(angle, expand=False, fillcolor='white')

def crop_slightly(img, pixels):
    """Слегка обрезает изображение по краям"""
    width, height = img.size
    if width > pixels * 2 and height > pixels * 2:
        return img.crop((pixels, pixels, width - pixels, height - pixels))
    return img

def add_slight_noise(img, seed):
    """Добавляет незаметный шум к изображению"""
    import numpy as np
    
    # Конвертируем в numpy array
    img_array = np.array(img)
    
    # Генерируем шум
    random.seed(seed)
    noise = np.random.normal(0, 1, img_array.shape)
    
    # Добавляем очень слабый шум
    img_array = img_array + noise * 0.5
    
    # Ограничиваем значения
    img_array = np.clip(img_array, 0, 255).astype(np.uint8)
    
    return Image.fromarray(img_array) 