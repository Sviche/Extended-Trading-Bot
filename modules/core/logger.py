"""
Logger module for Extended Bot
"""

import sys
import os
import requests
import time
from loguru import logger as loguru_logger

# ============================================================================
# ИНИЦИАЛИЗАЦИЯ ЦВЕТНОГО ВЫВОДА ДЛЯ WINDOWS
# ============================================================================
try:
    import colorama
    colorama.just_fix_windows_console()  # Автоматически включает ANSI для Windows
except ImportError:
    pass  # colorama не установлена, цвета могут не работать в Windows CMD

# Устанавливаем UTF-8 кодировку для корректного отображения эмодзи
if sys.platform == 'win32':
    try:
        # Устанавливаем кодовую страницу UTF-8 для Windows консоли
        os.system('chcp 65001 >nul 2>&1')
        # Устанавливаем UTF-8 для stdout/stderr
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass


class Logger:
    """
    Красивый логгер с поддержкой цветов, эмодзи и Telegram уведомлений
    """

    def __init__(self, telegram_api: str = None, telegram_chat_id: str = None):
        """
        Инициализация логгера

        Args:
            telegram_api: Telegram Bot API токен (опционально)
            telegram_chat_id: Telegram Chat ID для уведомлений (опционально)
        """
        self._logger = loguru_logger
        self._logger.remove()

        self.telegram_api = telegram_api
        self.telegram_chat_id = telegram_chat_id

        # Консольный вывод с красивым форматированием
        self._logger.add(
            sys.stderr,
            format="<green>{time:MM-DD HH:mm:ss}</green> | <level>{message}</level>",
            level="INFO",
            colorize=True  # ВАЖНО: явно включаем цвета
        )

        # Файловый вывод с ротацией
        log_file = './database/logs.log'
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self._logger.add(
            log_file,
            rotation="100 MB",
            retention="30 days",
            level="DEBUG",
            format="{time:DD-MM HH:mm:ss} | {level: <8} | {message}"
        )

        # Telegram настройки - если не заполнены, просто не отправляем уведомления
        # (без вывода сообщения в лог)

        self._telegram_emojis = {
            'info': 'ℹ️',
            'success': '✅',
            'warning': '⚠️',
            'error': '❌',
            'debug': '⚙️'
        }

    def _send_telegram(self, message: str) -> None:
        """
        Отправка сообщения в Telegram

        Args:
            message: Текст сообщения
        """
        if not (self.telegram_api and self.telegram_chat_id):
            return

        for _ in range(3):
            try:
                # Разбиваем длинные сообщения
                texts = [message[i:i + 1900] for i in range(0, len(message), 1900)]

                for text in texts:
                    response = requests.post(
                        f'https://api.telegram.org/bot{self.telegram_api}/sendMessage',
                        json={
                            'chat_id': self.telegram_chat_id,
                            'text': text,
                            'disable_web_page_preview': True
                        },
                        timeout=10
                    )
                    if not response.ok:
                        raise Exception(f'Telegram API error: {response.text}')
                break
            except Exception as e:
                self._logger.error(f"Failed to send Telegram message: {e}")
                time.sleep(60)

    def _log(self, level: str, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """
        Внутренний метод логирования

        Args:
            level: Уровень логирования
            message: Сообщение
            telegram: Отправлять ли в Telegram
            exc_info: Добавлять ли информацию об исключении (stack trace)
        """
        if exc_info:
            # Используем opt(exception=True) для логирования с traceback
            self._logger.opt(exception=True).log(level.upper(), message)
        else:
            getattr(self._logger, level)(message)

        if telegram and (self.telegram_api and self.telegram_chat_id):
            telegram_message = message.lstrip('\n')
            emoji = self._telegram_emojis.get(level, 'ℹ️')
            self._send_telegram(f"{emoji} {telegram_message}")

    def info(self, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """Информационное сообщение"""
        self._log('info', message, telegram, exc_info)

    def success(self, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """Сообщение об успехе"""
        self._log('success', message, telegram, exc_info)

    def warning(self, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """Предупреждение"""
        self._log('warning', message, telegram, exc_info)

    def error(self, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """Сообщение об ошибке"""
        self._log('error', message, telegram, exc_info)

    def debug(self, message: str, telegram: bool = False, exc_info: bool = False) -> None:
        """Отладочное сообщение"""
        self._log('debug', message, telegram, exc_info)

    def __getattr__(self, name: str) -> callable:
        """Проксируем все остальные методы loguru"""
        return getattr(self._logger, name)


def setup_logger(name="ExtendedBot", telegram_api: str = None, telegram_chat_id: str = None):
    """
    Создание и настройка логгера

    Args:
        name: Имя логгера (не используется в loguru, но оставлено для совместимости)
        telegram_api: Telegram Bot API токен
        telegram_chat_id: Telegram Chat ID

    Returns:
        Настроенный экземпляр Logger
    """
    return Logger(telegram_api=telegram_api, telegram_chat_id=telegram_chat_id)
