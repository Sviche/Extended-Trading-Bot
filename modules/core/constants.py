"""
Extended Bot - Internal Constants
Technical settings that should not be modified by the user
"""

from pathlib import Path

# === File Paths ===
USER_DATA_DIR = Path("user_data")
PRIVATE_KEYS_FILE = USER_DATA_DIR / "private_keys.txt"
PROXIES_FILE = USER_DATA_DIR / "proxies.txt"
ACCOUNTS_JSON = USER_DATA_DIR / "accounts.json"
DATABASE_FILE = Path("database") / "extended_bot.db"

# === Retry Settings ===
RETRY_SETTINGS = {
    'max_retries': 3,
    'base_delay': 1.0,  # Base delay (exponential backoff)
}

# === WebSocket Settings ===
WEBSOCKET_CONFIG = {
    'enabled': True,                # Use WebSocket for price data
    'cache_max_age': 2.0,           # Max data age in cache (sec)
    'fallback_to_rest': True,       # Fallback to REST API if WebSocket unavailable
    'check_interval': 3,            # Check status every 3 sec
}

# === Onboarding Settings (automatic) ===
ONBOARDING_CONFIG = {
    'auto_onboard_enabled': True,        # Auto onboarding on startup
    'network': 'mainnet',                # Network for onboarding (mainnet/testnet)
    'delay_between_accounts': 10.0,      # Delay between account registrations (sec)
    'apply_referral_after_onboard': True,  # Apply referral via REST API after onboarding
    'use_proxy_for_referral': False,     # Use proxy for referral application (False = direct)
}

# === Backward Compatibility (for legacy modules) ===
# These settings are now in TRADING_SETTINGS in settings.py, but duplicated here for compatibility
LIMIT_ORDER_CONFIG = {
    'websocket_enabled': WEBSOCKET_CONFIG['enabled'],
    'websocket_cache_max_age': WEBSOCKET_CONFIG['cache_max_age'],
    'websocket_fallback_to_rest': WEBSOCKET_CONFIG['fallback_to_rest'],
    'check_interval': WEBSOCKET_CONFIG['check_interval'],
    'use_market_fallback': True,  # Close with market order if limit orders fail
}

# === Orchestrator (backward compatibility) ===
# These settings are now in TRADING_SETTINGS in settings.py
ORCHESTRATOR_CONFIG = {
    'use_balanced_pool': True,  # Always use balancing
}
