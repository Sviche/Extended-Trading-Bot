"""
OrderBook Cache - кеширование актуальных bid/ask цен из WebSocket
Обеспечивает мгновенный доступ к ценам без задержек на API запросы
"""

import time
from typing import Optional, Dict, Tuple
from decimal import Decimal

from modules.core.logger import setup_logger

logger = setup_logger()


class OrderBookCache:
    """
    Глобальный кеш для хранения актуальных bid/ask цен

    Использует синглтон паттерн для единого экземпляра на все приложение
    Обновляется из WebSocket в реальном времени (каждые 10ms)
    """

    _instance = None

    def __new__(cls):
        """Создаем единственный экземпляр класса (синглтон)"""
        if cls._instance is None:
            cls._instance = super(OrderBookCache, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Инициализация кеша"""
        if self._initialized:
            return

        self._cache: Dict[str, Dict] = {}
        self._initialized = True
        # Инициализация кеша без вывода в лог (техническая информация)

    def update_orderbook(self, market: str, bids: list, asks: list):
        """
        Обновляет кешированные цены для рынка

        Args:
            market: Название рынка (BTC-USD, ETH-USD и т.д.)
            bids: Список bid ордеров [{"p": "...", "q": "..."}]
            asks: Список ask ордеров [{"p": "...", "q": "..."}]
        """
        if not bids or not asks:
            logger.debug(f"📊 {market}: пустой стакан (bids={len(bids)}, asks={len(asks)})")
            return

        # WebSocket возвращает отсортированный стакан:
        # bids[0] = лучший bid (highest price)
        # asks[0] = лучший ask (lowest price)

        try:
            best_bid = Decimal(str(bids[0]['p']))
            best_ask = Decimal(str(asks[0]['p']))

            spread = best_ask - best_bid
            spread_percent = (spread / best_bid) * Decimal('100')
            mid_price = (best_bid + best_ask) / Decimal('2')

            self._cache[market.upper()] = {
                'bid': best_bid,
                'ask': best_ask,
                'timestamp': time.time(),
                'spread': spread,
                'spread_percent': spread_percent,
                'mid_price': mid_price
            }

            # Не логируем каждое обновление - это спам (обновления каждые 10ms)

        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"📊 {market}: ошибка парсинга стакана: {e}")

    def get_prices(self, market: str, max_age_seconds: float = 2.0) -> Optional[Tuple[Decimal, Decimal]]:
        """
        Получает актуальные bid/ask цены из кеша

        Args:
            market: Название рынка (BTC-USD или просто BTC)
            max_age_seconds: Максимальный возраст данных в секундах

        Returns:
            Tuple[bid, ask] если данные свежие, иначе None
        """
        # Нормализуем название рынка (BTC → BTC-USD)
        if '-' not in market:
            market = f"{market}-USD"

        market = market.upper()

        if market not in self._cache:
            logger.debug(f"📊 {market}: нет в кеше")
            return None

        cache_entry = self._cache[market]
        age = time.time() - cache_entry['timestamp']

        if age > max_age_seconds:
            logger.debug(f"📊 {market}: данные устарели ({age:.1f}s > {max_age_seconds}s)")
            return None

        return (cache_entry['bid'], cache_entry['ask'])

    def get_spread_percent(self, market: str) -> Optional[Decimal]:
        """
        Возвращает процент спреда для рынка

        Args:
            market: Название рынка

        Returns:
            Процент спреда или None
        """
        # Нормализуем название рынка
        if '-' not in market:
            market = f"{market}-USD"

        market = market.upper()

        if market not in self._cache:
            return None

        return self._cache[market].get('spread_percent', None)

    def get_cache_status(self, market: str) -> Optional[Dict]:
        """
        Возвращает полную информацию о кешированных данных рынка

        Args:
            market: Название рынка

        Returns:
            Dict с полной информацией или None
        """
        # Нормализуем название рынка
        if '-' not in market:
            market = f"{market}-USD"

        market = market.upper()

        if market not in self._cache:
            return None

        cache_entry = self._cache[market].copy()
        cache_entry['age'] = time.time() - cache_entry['timestamp']
        return cache_entry

    def get_all_cached_markets(self) -> list:
        """Возвращает список всех рынков в кеше"""
        return list(self._cache.keys())

    def clear(self):
        """Очищает весь кеш"""
        self._cache.clear()
        logger.info("📊 OrderBook Cache очищен")


# Глобальный экземпляр кеша
orderbook_cache = OrderBookCache()
