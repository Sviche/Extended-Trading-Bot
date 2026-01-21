"""
Extended Bot - Settings and Configuration
"""

# === Trading Settings ===
TRADING_SETTINGS = {
    # Trading mode
    'order_mode': 'MARKET',  # LIMIT or MARKET, LIMIT gives very few points
    # LIMIT: Fee savings, slower, retry logic
    # MARKET: Fast volume farming, instant execution, higher fees (~0.05%)

    'markets': ['BTC', 'ETH'],  # List of coins to trade (without -USD)

    # Account batch settings
    'batch_size_range': [3, 3],  # Range of accounts per batch [min, max]
    'long_accounts_range': [1, 1],  # Number of long accounts per batch [min, max]
    # IMPORTANT: Number of shorts = total_accounts - long_accounts
    # Example: 6 accounts, 2 longs => 4 shorts (longs ≠ shorts ✓)

    'batch_size_usd': [1000, 1200],  # Range of TOTAL BATCH size in USD [min, max]
    # Longs split half, shorts split the other half (for hedging)
    # IMPORTANT: With 10x leverage, a $10 position requires $1 margin
    # At batch_size=30: half=15, long=15 (margin $1.5), short=7.5 each (margin $0.75)
    # At batch_size=50: half=25, long=25 (margin $2.5), short=12.5 each (margin $1.25)

    # Order size randomization (anti-sybil)
    'order_size_variation': [0.1, 0.4],  # Size deviation range from average [min, max]
    # Example: batch_size=120, 2 longs, 3 shorts, variation=0.1-0.4
    # Longs: $60 total → one ~$36, another ~$24 (20% deviation)
    # Shorts: $60 total → $15, $20, $25 (different deviations)
    # Sum of longs = sum of shorts = batch_size/2 ✓

    'leverage': {
        'BTC': 50,
        'ETH': 50,
        'SOL': 15,
    },

    # === LIMIT mode settings (only used when order_mode='LIMIT') ===
    'limit_order_offset_percent': 0.0001,  # 0.01% from price for limit orders (LIMIT mode only)
    'use_adaptive_offset': True,           # Auto-reduce offset if spread is narrow (LIMIT mode only)

    # Timeouts and retry (LIMIT mode only)
    'order_execution_timeout': 100,  # Wait for position open (sec) (LIMIT mode only)
    'position_close_timeout': 100,   # Wait for position close (sec) (LIMIT mode only)
    'max_open_retries': 5,           # Max attempts to open position (LIMIT mode only)
    'max_close_retries': 5,          # Max attempts to close position (LIMIT mode only)
    'max_batch_retries': 300,        # Max attempts to open entire batch on partial success (LIMIT mode only)

    # Workers
    'num_workers': 1,                      # Number of parallel workers (3-5 optimal)
    'account_cooldown_range': [60, 150],  # Account cooldown after trade [min, max] (sec)
    'generation_interval': 5.0,            # Interval for creating new batches (seconds)
    'max_queue_size': 10,                  # Maximum task queue size
    'max_consecutive_errors': 5,           # Max consecutive errors before disabling account
}

# === Position Management ===
POSITION_MANAGEMENT = {
    'holding_time_range': [60, 200],  # Position holding time [min, max] (sec)
    'monitor_interval_sec': 200,      # Check positions every N sec
}

# === Delays ===
DELAYS = {
    'between_orders': [3, 5],    # Delay between orders [min, max] (sec)
    'between_accounts': [3, 5],  # Delay between accounts [min, max] (sec)
    'on_error': 60,              # Delay on error (sec)
}

# === Logging ===
LOG_SETTINGS = {
    'level': 'INFO',      # DEBUG, INFO, WARNING, ERROR
    'file_max_mb': 100,   # Maximum log file size (MB)
    'stats_interval_sec': 300,  # Orchestrator stats output interval (sec)
}
