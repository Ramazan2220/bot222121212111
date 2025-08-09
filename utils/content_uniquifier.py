import os
import random
import logging
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import cv2
import numpy as np
from typing import List, Tuple, Optional, Union
import tempfile
import json
import re
from datetime import datetime, timedelta
import piexif

logger = logging.getLogger(__name__)

class ContentUniquifier:
    """Универсальный класс для уникализации контента"""
    
    def __init__(self):
        self.image_extensions = ['.jpg', '.jpeg', '.png', '.webp']
        self.video_extensions = ['.mp4', '.mov', '.avi', '.mkv']
        
    def uniquify_content(self, file_path: Union[str, List[str]], content_type: str, caption: str = "") -> Tuple[Union[str, List[str]], str]:
        """
        Главный метод для уникализации контента
        
        Args:
            file_path: Путь к файлу или список путей (для карусели)
            content_type: Тип контента ('photo', 'video', 'carousel', 'story', 'reel')
            caption: Текст описания
            
        Returns:
            Tuple[путь к уникализированному файлу/файлам, уникализированный caption]
        """
        try:
            # Уникализация текста
            unique_caption = self.uniquify_text(caption) if caption else ""
            
            # Уникализация медиа
            if isinstance(file_path, list):
                # Карусель - обрабатываем каждый файл
                unique_files = []
                for path in file_path:
                    unique_file = self._uniquify_single_file(path, content_type)
                    unique_files.append(unique_file)
                return unique_files, unique_caption
            else:
                # Одиночный файл
                unique_file = self._uniquify_single_file(file_path, content_type)
                return unique_file, unique_caption
                
        except Exception as e:
            logger.error(f"Ошибка при уникализации контента: {e}")
            # В случае ошибки возвращаем оригинальные файлы
            return file_path, caption
    
    def _uniquify_single_file(self, file_path: str, content_type: str) -> str:
        """Уникализация одного файла"""
        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            return file_path
            
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext in self.image_extensions:
            return self.uniquify_image(file_path, content_type)
        elif file_ext in self.video_extensions:
            return self.uniquify_video(file_path, content_type)
        else:
            logger.warning(f"Неподдерживаемый тип файла: {file_ext}")
            return file_path
    
    def uniquify_image(self, image_path: str, content_type: str) -> str:
        """Уникализация изображения"""
        try:
            # Открываем изображение
            img = Image.open(image_path)
            
            # Конвертируем в RGB если нужно
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Применяем случайные трансформации
            transformations = []
            
            # 1. Небольшое изменение размера (98-102%)
            if random.random() > 0.3:
                scale = random.uniform(0.98, 1.02)
                new_size = (int(img.width * scale), int(img.height * scale))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                transformations.append(f"resize_{scale:.2f}")
            
            # 2. Поворот на небольшой угол (-2 до +2 градусов)
            if random.random() > 0.4:
                angle = random.uniform(-2, 2)
                img = img.rotate(angle, expand=True, fillcolor='white')
                transformations.append(f"rotate_{angle:.1f}")
            
            # 3. Изменение яркости (90-110%)
            if random.random() > 0.3:
                brightness = random.uniform(0.9, 1.1)
                enhancer = ImageEnhance.Brightness(img)
                img = enhancer.enhance(brightness)
                transformations.append(f"brightness_{brightness:.2f}")
            
            # 4. Изменение контраста (90-110%)
            if random.random() > 0.3:
                contrast = random.uniform(0.9, 1.1)
                enhancer = ImageEnhance.Contrast(img)
                img = enhancer.enhance(contrast)
                transformations.append(f"contrast_{contrast:.2f}")
            
            # 5. Изменение насыщенности (90-110%)
            if random.random() > 0.3:
                saturation = random.uniform(0.9, 1.1)
                enhancer = ImageEnhance.Color(img)
                img = enhancer.enhance(saturation)
                transformations.append(f"saturation_{saturation:.2f}")
            
            # 6. Небольшое размытие или резкость
            if random.random() > 0.5:
                if random.random() > 0.5:
                    # Легкое размытие
                    img = img.filter(ImageFilter.GaussianBlur(radius=random.uniform(0.1, 0.5)))
                    transformations.append("blur")
                else:
                    # Легкая резкость
                    enhancer = ImageEnhance.Sharpness(img)
                    img = enhancer.enhance(random.uniform(1.1, 1.3))
                    transformations.append("sharpen")
            
            # 7. Обрезка краев (1-3 пикселя)
            if random.random() > 0.3:
                pixels = random.randint(1, 3)
                img = img.crop((pixels, pixels, img.width - pixels, img.height - pixels))
                transformations.append(f"crop_{pixels}px")
            
            # 8. Зеркальное отражение (только для не-текстовых изображений)
            if random.random() > 0.7 and content_type in ['story', 'carousel']:
                img = ImageOps.mirror(img)
                transformations.append("mirror")
            
            # Сохраняем уникализированное изображение
            output_path = self._get_unique_output_path(image_path)
            
            # Сохраняем с случайным качеством (92-98)
            quality = random.randint(92, 98)
            
            # Подготавливаем EXIF данные
            exif_bytes = self._generate_unique_exif()
            
            # Сохраняем с уникальными метаданными
            if exif_bytes:
                img.save(output_path, quality=quality, optimize=True, exif=exif_bytes)
            else:
                img.save(output_path, quality=quality, optimize=True)
            
            # Дополнительно изменяем метаданные файла
            self._modify_file_metadata(output_path)
            
            logger.info(f"✅ Изображение уникализировано: {' + '.join(transformations)}")
            return output_path
            
        except Exception as e:
            logger.error(f"Ошибка при уникализации изображения: {e}")
            return image_path
    
    def uniquify_video(self, video_path: str, content_type: str) -> str:
        """Уникализация видео"""
        try:
            # Открываем видео
            cap = cv2.VideoCapture(video_path)
            
            # Получаем параметры видео
            fps = int(cap.get(cv2.CAP_PROP_FPS))
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            
            # Создаем выходной файл
            output_path = self._get_unique_output_path(video_path)
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            
            # Применяем трансформации
            transformations = []
            
            # 1. Небольшое изменение размера
            scale = random.uniform(0.98, 1.02)
            new_width = int(width * scale)
            new_height = int(height * scale)
            transformations.append(f"scale_{scale:.2f}")
            
            # 2. Изменение скорости (95-105%)
            speed_factor = random.uniform(0.95, 1.05)
            new_fps = int(fps * speed_factor)
            transformations.append(f"speed_{speed_factor:.2f}")
            
            out = cv2.VideoWriter(output_path, fourcc, new_fps, (new_width, new_height))
            
            # Параметры цветокоррекции
            brightness = random.uniform(-10, 10)
            contrast = random.uniform(0.9, 1.1)
            saturation = random.uniform(0.9, 1.1)
            
            frame_count = 0
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Изменяем размер
                frame = cv2.resize(frame, (new_width, new_height))
                
                # Применяем цветокоррекцию
                frame = cv2.convertScaleAbs(frame, alpha=contrast, beta=brightness)
                
                # Изменяем насыщенность
                hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV).astype(np.float32)
                hsv[:, :, 1] = hsv[:, :, 1] * saturation
                hsv[:, :, 1][hsv[:, :, 1] > 255] = 255
                frame = cv2.cvtColor(hsv.astype(np.uint8), cv2.COLOR_HSV2BGR)
                
                # Записываем кадр
                out.write(frame)
                frame_count += 1
                
                # Для длинных видео - обрабатываем каждый N-й кадр для ускорения
                if frame_count % 100 == 0:
                    logger.debug(f"Обработано кадров: {frame_count}")
            
            # Освобождаем ресурсы
            cap.release()
            out.release()
            cv2.destroyAllWindows()
            
            # Изменяем метаданные видео файла
            self._modify_video_metadata(output_path)
            self._modify_file_metadata(output_path)
            
            logger.info(f"✅ Видео уникализировано: {' + '.join(transformations)}")
            return output_path
            
        except Exception as e:
            logger.error(f"Ошибка при уникализации видео: {e}")
            return video_path
    
    def uniquify_text(self, text: str) -> str:
        """Уникализация текста"""
        if not text:
            return text
            
        try:
            # Словарь синонимов и вариаций
            synonyms = {
                'красивый': ['прекрасный', 'великолепный', 'чудесный', 'восхитительный'],
                'хороший': ['отличный', 'замечательный', 'превосходный', 'классный'],
                'новый': ['свежий', 'недавний', 'современный'],
                'день': ['денек', 'сутки'],
                'фото': ['фотография', 'снимок', 'кадр'],
                'видео': ['ролик', 'клип', 'запись'],
                'смотри': ['посмотри', 'взгляни', 'гляди'],
                'привет': ['приветик', 'хай', 'здравствуй'],
                'пока': ['до встречи', 'увидимся', 'до скорого'],
            }
            
            # Вариации эмодзи
            emoji_variations = {
                '❤️': ['💕', '💖', '💗', '💝', '❤️‍🔥'],
                '😊': ['😄', '😃', '🙂', '☺️'],
                '👍': ['👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿'],
                '🔥': ['🔥', '💥', '⚡', '✨'],
                '😍': ['🥰', '😘', '💋'],
                '🎉': ['🎊', '🥳', '🎈'],
            }
            
            unique_text = text
            
            # 1. Заменяем некоторые слова синонимами
            for word, synonyms_list in synonyms.items():
                if word in unique_text.lower() and random.random() > 0.5:
                    replacement = random.choice(synonyms_list)
                    unique_text = re.sub(rf'\b{word}\b', replacement, unique_text, flags=re.IGNORECASE)
            
            # 2. Изменяем эмодзи
            for emoji, variations in emoji_variations.items():
                if emoji in unique_text and random.random() > 0.5:
                    unique_text = unique_text.replace(emoji, random.choice(variations))
            
            # 3. Добавляем невидимые символы (zero-width space)
            if random.random() > 0.5:
                words = unique_text.split()
                if len(words) > 3:
                    # Вставляем невидимый символ в случайные места
                    for _ in range(random.randint(1, 3)):
                        pos = random.randint(1, len(words) - 1)
                        words[pos] = '\u200b' + words[pos]
                    unique_text = ' '.join(words)
            
            # 4. Перемешиваем хештеги
            hashtags = re.findall(r'#\w+', unique_text)
            if len(hashtags) > 2:
                # Извлекаем все хештеги
                text_without_hashtags = unique_text
                for tag in hashtags:
                    text_without_hashtags = text_without_hashtags.replace(tag, '', 1)
                
                # Перемешиваем
                random.shuffle(hashtags)
                
                # Добавляем обратно
                unique_text = text_without_hashtags.strip() + '\n\n' + ' '.join(hashtags)
            
            # 5. Изменяем регистр некоторых букв (очень осторожно)
            if random.random() > 0.8 and len(unique_text) > 20:
                # Меняем регистр только у 1-2 букв
                text_list = list(unique_text)
                for _ in range(random.randint(1, 2)):
                    pos = random.randint(0, len(text_list) - 1)
                    if text_list[pos].isalpha():
                        text_list[pos] = text_list[pos].swapcase()
                unique_text = ''.join(text_list)
            
            return unique_text
            
        except Exception as e:
            logger.error(f"Ошибка при уникализации текста: {e}")
            return text
    
    def _get_unique_output_path(self, original_path: str) -> str:
        """Генерирует путь для сохранения уникализированного файла"""
        dir_path = os.path.dirname(original_path)
        filename = os.path.basename(original_path)
        name, ext = os.path.splitext(filename)
        
        # Создаем уникальное имя
        import uuid
        unique_name = f"{name}_unique_{uuid.uuid4().hex[:8]}{ext}"
        
        return os.path.join(dir_path, unique_name)
    
    def _generate_unique_exif(self) -> bytes:
        """Генерирует уникальные EXIF метаданные"""
        try:
            # Создаем базовый EXIF словарь
            exif_dict = {
                "0th": {},
                "Exif": {},
                "GPS": {},
                "1st": {},
            }
            
            # Список популярных камер для рандомизации
            camera_models = [
                ("Apple", "iPhone 13 Pro"),
                ("Apple", "iPhone 14 Pro Max"),
                ("Apple", "iPhone 15 Pro"),
                ("Samsung", "Galaxy S23 Ultra"),
                ("Samsung", "Galaxy S24"),
                ("Google", "Pixel 8 Pro"),
                ("OnePlus", "11 Pro"),
                ("Xiaomi", "13 Pro"),
            ]
            
            make, model = random.choice(camera_models)
            
            # Основные EXIF теги
            exif_dict["0th"][piexif.ImageIFD.Make] = make.encode()
            exif_dict["0th"][piexif.ImageIFD.Model] = model.encode()
            exif_dict["0th"][piexif.ImageIFD.Software] = f"{make} Camera {random.randint(1, 10)}.{random.randint(0, 9)}".encode()
            
            # Генерируем случайную дату в последние 30 дней
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            photo_date = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            date_str = photo_date.strftime("%Y:%m:%d %H:%M:%S")
            
            exif_dict["0th"][piexif.ImageIFD.DateTime] = date_str.encode()
            exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = date_str.encode()
            exif_dict["Exif"][piexif.ExifIFD.DateTimeDigitized] = date_str.encode()
            
            # Параметры камеры
            # ISO
            exif_dict["Exif"][piexif.ExifIFD.ISOSpeedRatings] = random.choice([100, 200, 400, 800, 1600])
            
            # Выдержка (от 1/2000 до 1/30)
            shutter_speeds = [(1, 2000), (1, 1000), (1, 500), (1, 250), (1, 125), (1, 60), (1, 30)]
            exif_dict["Exif"][piexif.ExifIFD.ExposureTime] = random.choice(shutter_speeds)
            
            # Диафрагма
            apertures = [(18, 10), (20, 10), (24, 10), (28, 10), (35, 10), (40, 10), (56, 10)]  # f/1.8, f/2.0, etc
            exif_dict["Exif"][piexif.ExifIFD.FNumber] = random.choice(apertures)
            
            # Фокусное расстояние
            focal_lengths = [(24, 1), (35, 1), (50, 1), (85, 1), (135, 1)]
            exif_dict["Exif"][piexif.ExifIFD.FocalLength] = random.choice(focal_lengths)
            
            # GPS координаты (опционально, с 30% вероятностью)
            if random.random() > 0.7:
                # Генерируем случайные координаты в пределах популярных городов
                cities = [
                    (40.7128, -74.0060),  # New York
                    (51.5074, -0.1278),   # London
                    (48.8566, 2.3522),    # Paris
                    (35.6762, 139.6503),  # Tokyo
                    (34.0522, -118.2437), # Los Angeles
                    (41.8781, -87.6298),  # Chicago
                    (37.7749, -122.4194), # San Francisco
                ]
                
                lat, lon = random.choice(cities)
                # Добавляем небольшое смещение
                lat += random.uniform(-0.1, 0.1)
                lon += random.uniform(-0.1, 0.1)
                
                # Конвертируем в формат GPS
                lat_deg = int(abs(lat))
                lat_min = int((abs(lat) - lat_deg) * 60)
                lat_sec = int(((abs(lat) - lat_deg) * 60 - lat_min) * 60 * 100)
                
                lon_deg = int(abs(lon))
                lon_min = int((abs(lon) - lon_deg) * 60)
                lon_sec = int(((abs(lon) - lon_deg) * 60 - lon_min) * 60 * 100)
                
                exif_dict["GPS"][piexif.GPSIFD.GPSLatitudeRef] = b'N' if lat >= 0 else b'S'
                exif_dict["GPS"][piexif.GPSIFD.GPSLatitude] = [(lat_deg, 1), (lat_min, 1), (lat_sec, 100)]
                exif_dict["GPS"][piexif.GPSIFD.GPSLongitudeRef] = b'E' if lon >= 0 else b'W'
                exif_dict["GPS"][piexif.GPSIFD.GPSLongitude] = [(lon_deg, 1), (lon_min, 1), (lon_sec, 100)]
            
            # Конвертируем в байты
            exif_bytes = piexif.dump(exif_dict)
            return exif_bytes
            
        except Exception as e:
            logger.warning(f"Не удалось создать EXIF данные: {e}")
            return None
    
    def _modify_file_metadata(self, file_path: str):
        """Изменяет метаданные файла (время создания и модификации)"""
        try:
            # Генерируем случайное время в последние 30 дней
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            seconds_ago = random.randint(0, 59)
            
            new_time = datetime.now() - timedelta(
                days=days_ago, 
                hours=hours_ago, 
                minutes=minutes_ago,
                seconds=seconds_ago
            )
            
            # Конвертируем в timestamp
            timestamp = new_time.timestamp()
            
            # Изменяем время доступа и модификации файла
            os.utime(file_path, (timestamp, timestamp))
            
            logger.debug(f"Метаданные файла изменены: {new_time}")
            
        except Exception as e:
            logger.warning(f"Не удалось изменить метаданные файла: {e}")
    
    def _modify_video_metadata(self, video_path: str):
        """Изменяет метаданные видео файла"""
        try:
            # Для изменения метаданных видео используем ffmpeg через moviepy
            from moviepy.editor import VideoFileClip
            import tempfile
            
            # Загружаем видео
            video = VideoFileClip(video_path)
            
            # Генерируем случайные метаданные
            creation_time = datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            # Список устройств для метаданных
            devices = [
                "iPhone 13 Pro",
                "iPhone 14 Pro Max", 
                "iPhone 15 Pro",
                "Samsung Galaxy S23 Ultra",
                "Google Pixel 8 Pro",
            ]
            
            # Создаем временный файл для вывода
            temp_output = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
            temp_output.close()
            
            # Записываем видео с новыми метаданными
            video.write_videofile(
                temp_output.name,
                codec='libx264',
                audio_codec='aac',
                temp_audiofile=tempfile.mktemp('.m4a'),
                remove_temp=True,
                logger=None,  # Отключаем вывод moviepy
                metadata={
                    'creation_time': creation_time.isoformat(),
                    'encoder': f'{random.choice(devices)} Camera',
                    'comment': f'Recorded with {random.choice(devices)}',
                    'title': '',
                    'artist': '',
                    'album': ''
                }
            )
            
            # Закрываем видео
            video.close()
            
            # Заменяем оригинальный файл
            import shutil
            shutil.move(temp_output.name, video_path)
            
            logger.debug(f"Метаданные видео изменены")
            
        except Exception as e:
            logger.warning(f"Не удалось изменить метаданные видео: {e}")
            # Продолжаем даже если не удалось изменить метаданные


# Глобальный экземпляр уникализатора
uniquifier = ContentUniquifier()


def uniquify_for_publication(file_path: Union[str, List[str]], content_type: str, caption: str = "") -> Tuple[Union[str, List[str]], str]:
    """
    Удобная функция для уникализации контента перед публикацией
    
    Args:
        file_path: Путь к файлу или список путей
        content_type: Тип контента ('photo', 'video', 'carousel', 'story', 'reel')
        caption: Текст описания
        
    Returns:
        Tuple[уникализированные файлы, уникализированный текст]
    """
    return uniquifier.uniquify_content(file_path, content_type, caption) 