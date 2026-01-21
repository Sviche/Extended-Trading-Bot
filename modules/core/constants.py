"""
Extended Bot - Internal Constants
Технические настройки, которые не должны изменяться пользователем
"""

from pathlib import Path

# === Пути к файлам ===
USER_DATA_DIR = Path("user_data")
PRIVATE_KEYS_FILE = USER_DATA_DIR / "private_keys.txt"
PROXIES_FILE = USER_DATA_DIR / "proxies.txt"
ACCOUNTS_JSON = USER_DATA_DIR / "accounts.json"
DATABASE_FILE = Path("database") / "extended_bot.db"

# === Настройки Retry ===
RETRY_SETTINGS = {
    'max_retries': 3,
    'base_delay': 1.0,  # Базовая задержка (exponential backoff)
}

# === WebSocket настройки ===
WEBSOCKET_CONFIG = {
    'enabled': True,                # Использовать WebSocket для получения цен
    'cache_max_age': 2.0,           # Макс возраст данных в кеше (сек)
    'fallback_to_rest': True,       # Fallback на REST API если WebSocket недоступен
    'check_interval': 3,            # Проверять статус каждые 3 сек
}

# === Настройки Onboarding (автоматические) ===
ONBOARDING_CONFIG = {
    'auto_onboard_enabled': True,        # Автоматический onboarding при запуске
    'network': 'mainnet',                # Сеть для onboarding (mainnet/testnet)
    'delay_between_accounts': 10.0,      # Задержка между регистрацией аккаунтов (сек)
    'apply_referral_after_onboard': True,  # Применять реферал через REST API после onboarding
    'use_proxy_for_referral': False,     # Использовать прокси для применения реферала (False = напрямую)
}

# === Обратная совместимость (для старых модулей) ===
# Эти настройки теперь в TRADING_SETTINGS в settings.py, но для совместимости дублируем здесь
LIMIT_ORDER_CONFIG = {
    'websocket_enabled': WEBSOCKET_CONFIG['enabled'],
    'websocket_cache_max_age': WEBSOCKET_CONFIG['cache_max_age'],
    'websocket_fallback_to_rest': WEBSOCKET_CONFIG['fallback_to_rest'],
    'check_interval': WEBSOCKET_CONFIG['check_interval'],
    'use_market_fallback': True,  # Закрывать маркет-ордером если лимитки не сработали
}

# === Оркестратор (обратная совместимость) ===
# Эти настройки теперь в TRADING_SETTINGS в settings.py
ORCHESTRATOR_CONFIG = {
    'use_balanced_pool': True,  # Всегда используем балансировку
}
