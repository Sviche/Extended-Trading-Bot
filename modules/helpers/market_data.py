"""
Market Data - Получение рыночных данных из Extended API
Поддерживает подключение через прокси (HTTP/SOCKS5)
"""

import aiohttp
import traceback
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

try:
    from aiohttp_socks import ProxyConnector
    PROXY_SUPPORT = True
except ImportError:
    PROXY_SUPPORT = False

from modules.core.logger import setup_logger


@dataclass
class OrderbookLevel:
    """Уровень в стакане"""
    price: Decimal
    qty: Decimal


@dataclass
class Orderbook:
    """Стакан ордеров"""
    market: str
    bids: List[OrderbookLevel]  # Сортированы по убыванию цены
    asks: List[OrderbookLevel]  # Сортированы по возрастанию цены

    def best_bid(self) -> Optional[Decimal]:
        """Лучшая цена покупки"""
        return self.bids[0].price if self.bids else None

    def best_ask(self) -> Optional[Decimal]:
        """Лучшая цена продажи"""
        return self.asks[0].price if self.asks else None

    def mid_price(self) -> Optional[Decimal]:
        """Средняя цена"""
        bid = self.best_bid()
        ask = self.best_ask()
        if bid and ask:
            return (bid + ask) / Decimal('2')
        return None


@dataclass
class MarketStats:
    """Статистика рынка"""
    market: str
    last_price: Decimal
    mark_price: Decimal
    index_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    volume_24h: Decimal
    price_change_24h_percent: Decimal
    high_24h: Decimal
    low_24h: Decimal
    funding_rate: Decimal


class MarketDataProvider:
    """
    Провайдер маркет-данных Extended

    Получает данные через публичные REST API endpoints
    """

    def __init__(
        self,
        base_url: str = "https://api.starknet.extended.exchange/api/v1",
        testnet: bool = False,
        proxy: Optional[str] = None,
        logger=None
    ):
        """
        Инициализация провайдера

        Args:
            base_url: Базовый URL API (можно переопределить)
            testnet: Использовать тестнет
            proxy: URL прокси (http://user:pass@host:port или socks5://...)
            logger: Логгер
        """
        if testnet:
            base_url = "https://api.starknet.sepolia.extended.exchange/api/v1"

        self.base_url = base_url
        self.proxy = proxy
        self.logger = logger or setup_logger()
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать aiohttp сессию (с прокси если задан)"""
        if self.session is None or self.session.closed:
            connector = None

            # Создаем ProxyConnector если задан прокси
            if self.proxy:
                if not PROXY_SUPPORT:
                    self.logger.error("❌ aiohttp-socks не установлен! Выполните: pip install aiohttp-socks")
                    raise ImportError("aiohttp-socks not installed")

                try:
                    # Нормализуем прокси URL (добавляем http:// если отсутствует схема)
                    normalized_proxy = self._normalize_proxy_url(self.proxy)

                    # rdns=True - резолвить DNS через прокси (только для SOCKS)
                    # Для HTTP прокси параметр rdns не поддерживается
                    use_rdns = normalized_proxy.lower().startswith('socks')

                    connector = ProxyConnector.from_url(normalized_proxy, rdns=use_rdns)
                    self.logger.debug(f"🌐 MarketData: использую прокси {self._mask_proxy(normalized_proxy)}")
                except Exception as e:
                    self.logger.error(f"❌ Ошибка создания прокси коннектора: {e}")
                    raise

            # trust_env=False - игнорировать системные прокси (VPN), использовать только заданный прокси
            self.session = aiohttp.ClientSession(connector=connector, trust_env=False)
        return self.session

    def _normalize_proxy_url(self, proxy_url: str) -> str:
        """
        Нормализует URL прокси - добавляет схему и порт если отсутствуют

        Поддерживаемые форматы:
        - host:port
        - host:port:username:password  (специальный формат)
        - user:pass@host:port
        - http://user:pass@host:port
        - socks5://user:pass@host:port

        Args:
            proxy_url: URL прокси

        Returns:
            Нормализованный URL с схемой и портом
        """
        from urllib.parse import urlparse

        proxy_url = proxy_url.strip()

        # Обрабатываем специальный формат host:port:username:password
        # Пример: gate.nodemaven.com:8080:user:pass
        if '://' not in proxy_url and proxy_url.count(':') >= 3:
            parts = proxy_url.split(':', 3)  # Разделяем на максимум 4 части
            if len(parts) == 4:
                host, port, username, password = parts
                proxy_url = f'http://{username}:{password}@{host}:{port}'
                self.logger.debug(f"🔄 Конвертирован формат host:port:user:pass в стандартный URL")

        # Проверяем есть ли схема (http://, https://, socks5://, etc)
        if '://' not in proxy_url:
            # Добавляем http:// по умолчанию
            proxy_url = f'http://{proxy_url}'

        # Парсим URL
        try:
            parsed = urlparse(proxy_url)

            # Если порт отсутствует, добавляем по умолчанию
            if not parsed.port:
                default_port = 1080 if 'socks' in parsed.scheme.lower() else 8080
                # Пересобираем URL с портом
                if parsed.username and parsed.password:
                    proxy_url = f"{parsed.scheme}://{parsed.username}:{parsed.password}@{parsed.hostname}:{default_port}"
                else:
                    proxy_url = f"{parsed.scheme}://{parsed.hostname}:{default_port}"

                self.logger.warning(f"⚠️ Порт не указан в прокси, использую порт по умолчанию: {default_port}")
        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга прокси URL '{proxy_url}': {e}")
            raise

        return proxy_url

    def _mask_proxy(self, proxy_url: str) -> str:
        """Маскирует пароль в URL прокси для логирования"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            if parsed.password:
                return proxy_url.replace(f":{parsed.password}@", ":****@")
            return proxy_url
        except:
            return proxy_url

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict:
        """
        Выполнить HTTP запрос к API

        Args:
            method: HTTP метод (GET, POST, etc)
            endpoint: Путь endpoint (без base_url)
            params: Query параметры
            headers: Дополнительные заголовки

        Returns:
            Данные из ответа

        Raises:
            Exception при ошибках
        """
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"

        default_headers = {
            'User-Agent': 'Extended-Bot/0.1'
        }

        if headers:
            default_headers.update(headers)

        try:
            async with session.request(
                method,
                url,
                params=params,
                headers=default_headers
            ) as response:
                response.raise_for_status()
                data = await response.json()

                # Проверка статуса в ответе Extended API
                if data.get('status') == 'error':
                    error_msg = data.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"API Error: {error_msg}")

                # Возвращаем data или пустой словарь (НЕ список, т.к. разные endpoint'ы возвращают разное)
                return data.get('data', {})

        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP error: {type(e).__name__}: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            self.logger.error(f"Request error: {type(e).__name__}: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def get_orderbook(self, market: str) -> Orderbook:
        """
        Получить стакан ордеров

        Args:
            market: Название рынка (например "BTC-USD")

        Returns:
            Orderbook объект
        """
        try:
            data = await self._request(
                'GET',
                f'/info/markets/{market}/orderbook'
            )

            # Парсинг bids
            bids = []
            for level in data.get('bid', []):
                bids.append(OrderbookLevel(
                    price=Decimal(level['price']),
                    qty=Decimal(level['qty'])
                ))

            # Парсинг asks
            asks = []
            for level in data.get('ask', []):
                asks.append(OrderbookLevel(
                    price=Decimal(level['price']),
                    qty=Decimal(level['qty'])
                ))

            orderbook = Orderbook(
                market=market,
                bids=bids,
                asks=asks
            )

            self.logger.debug(
                f"Orderbook {market}: "
                f"bid={orderbook.best_bid()}, "
                f"ask={orderbook.best_ask()}"
            )

            return orderbook

        except Exception as e:
            self.logger.error(f"Ошибка получения orderbook {market}: {type(e).__name__}: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def get_market_stats(self, market: str) -> MarketStats:
        """
        Получить статистику рынка

        Args:
            market: Название рынка (например "BTC-USD")

        Returns:
            MarketStats объект
        """
        try:
            data = await self._request(
                'GET',
                f'/info/markets/{market}/stats'
            )

            stats = MarketStats(
                market=market,
                last_price=Decimal(data.get('lastPrice', '0')),
                mark_price=Decimal(data.get('markPrice', '0')),
                index_price=Decimal(data.get('indexPrice', '0')),
                bid_price=Decimal(data.get('bidPrice', '0')),
                ask_price=Decimal(data.get('askPrice', '0')),
                volume_24h=Decimal(data.get('volume24h', '0')),
                price_change_24h_percent=Decimal(data.get('priceChange24hPercent', '0')),
                high_24h=Decimal(data.get('high24h', '0')),
                low_24h=Decimal(data.get('low24h', '0')),
                funding_rate=Decimal(data.get('fundingRate', '0'))
            )

            self.logger.debug(
                f"Stats {market}: last={stats.last_price}, "
                f"mark={stats.mark_price}"
            )

            return stats

        except Exception as e:
            self.logger.error(f"Ошибка получения stats {market}: {e}")
            raise

    async def get_all_markets(self) -> List[Dict]:
        """
        Получить информацию о всех доступных рынках

        Returns:
            Список рынков с их конфигурацией
        """
        try:
            data = await self._request('GET', '/info/markets')
            markets = data if isinstance(data, list) else []

            self.logger.debug(f"Получено рынков: {len(markets)}")
            return markets

        except Exception as e:
            self.logger.error(f"Ошибка получения списка рынков: {e}")
            raise

    async def get_market_price_for_order(
        self,
        market: str,
        side: str,  # "BUY" или "SELL"
        aggressive: bool = True
    ) -> Decimal:
        """
        Получить цену для размещения маркет-ордера

        ВАЖНО: Использует market_stats вместо orderbook, т.к. orderbook endpoint 
        не работает стабильно через прокси (таймауты).

        Args:
            market: Название рынка
            side: Направление ("BUY" или "SELL")
            aggressive: Если True - агрессивная цена для гарантированного исполнения

        Returns:
            Цена для ордера
        """
        try:
            # Получаем market stats (вместо orderbook - работает быстрее и стабильнее)
            stats = await self.get_market_stats(market)

            if side.upper() == "BUY":
                # Покупка - берем ask_price из stats
                base_price = stats.ask_price
                if base_price is None or base_price == 0:
                    # Fallback на mark_price если ask отсутствует
                    base_price = stats.mark_price

                # Агрессивная цена: +1% от ask для гарантированного исполнения
                if aggressive:
                    price = base_price * Decimal('1.01')
                else:
                    price = base_price

            else:  # SELL
                # Продажа - берем bid_price из stats
                base_price = stats.bid_price
                if base_price is None or base_price == 0:
                    # Fallback на mark_price если bid отсутствует
                    base_price = stats.mark_price

                # Агрессивная цена: -1% от bid для гарантированного исполнения
                if aggressive:
                    price = base_price * Decimal('0.99')
                else:
                    price = base_price

            self.logger.debug(
                f"Цена для {side} {market}: {price} "
                f"(base: {base_price}, aggressive: {aggressive})"
            )

            return price

        except Exception as e:
            self.logger.error(
                f"Ошибка получения цены для {side} {market}: {type(e).__name__}: {str(e)}"
            )
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def get_positions_rest(
        self,
        api_key: str,
        market: Optional[str] = None,
        side: Optional[str] = None
    ) -> List[Dict]:
        """
        Получить позиции аккаунта через REST API

        Args:
            api_key: API ключ пользователя
            market: Фильтр по рынку (опционально)
            side: Фильтр по стороне LONG/SHORT (опционально)

        Returns:
            Список позиций
        """
        try:
            params = {}
            if market:
                params['market'] = market
            if side:
                params['side'] = side

            headers = {
                'X-Api-Key': api_key
            }

            data = await self._request(
                'GET',
                '/user/positions',
                params=params,
                headers=headers
            )

            # _request возвращает содержимое поля 'data' из API ответа
            # Это может быть:
            # 1. Список позиций: [{...}, {...}]
            # 2. Пустой список: []
            # 3. Словарь с ключом 'positions': {'positions': [...]}
            # 4. Пустой словарь: {} (когда поле 'data' отсутствует в ответе)

            if isinstance(data, list):
                # Прямой список позиций
                positions = data
            elif isinstance(data, dict):
                # Словарь - ищем вложенные позиции
                positions = data.get('positions', data.get('data', []))
                # Если и этого нет - возможно сам словарь является одной позицией
                if not positions and ('market' in data or 'side' in data):
                    positions = [data]
                elif not positions:
                    # Пустой результат
                    positions = []
            else:
                # Неожиданный тип - возвращаем пустой список
                self.logger.warning(f"REST API positions: неожиданный тип данных {type(data)}")
                positions = []

            self.logger.debug(
                f"REST API positions: получено {len(positions)} позиций"
            )

            return positions

        except Exception as e:
            self.logger.error(f"Ошибка получения позиций через REST API: {e}")
            raise

    async def get_order_status_rest(
        self,
        api_key: str,
        order_id: str
    ) -> Optional[Dict]:
        """
        Получить статус ордера по ID через REST API

        Args:
            api_key: API ключ пользователя
            order_id: ID ордера

        Returns:
            Информация об ордере или None если не найден
        """
        try:
            headers = {
                'X-Api-Key': api_key
            }

            data = await self._request(
                'GET',
                f'/user/orders/{order_id}',
                headers=headers
            )

            self.logger.debug(
                f"REST API order {order_id}: status={data.get('status')}, "
                f"filledQty={data.get('filledQty')}/{data.get('qty')}"
            )

            return data

        except Exception as e:
            self.logger.warning(f"Ошибка получения статуса ордера {order_id}: {e}")
            return None

    async def get_open_orders_rest(
        self,
        api_key: str,
        market: Optional[str] = None
    ) -> List[Dict]:
        """
        Получить открытые ордера через REST API

        Args:
            api_key: API ключ пользователя
            market: Фильтр по рынку (опционально)

        Returns:
            Список открытых ордеров
        """
        try:
            params = {}
            if market:
                params['market'] = market

            headers = {
                'X-Api-Key': api_key
            }

            data = await self._request(
                'GET',
                '/user/orders',
                params=params,
                headers=headers
            )

            # API может вернуть словарь или список
            if isinstance(data, dict):
                orders = data.get('orders', data.get('data', []))
            else:
                orders = data if isinstance(data, list) else []

            self.logger.debug(
                f"REST API open orders: получено {len(orders)} ордеров"
            )

            return orders

        except Exception as e:
            self.logger.error(f"Ошибка получения открытых ордеров через REST API: {e}")
            raise

    async def close(self):
        """Закрыть HTTP сессию"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
                # Небольшая задержка для корректного закрытия всех соединений
                import asyncio
                await asyncio.sleep(0.25)
                self.logger.debug("HTTP сессия закрыта")
            except Exception as e:
                self.logger.debug(f"Ошибка закрытия HTTP сессии: {e}")

        # Явно очищаем ссылку на сессию
        self.session = None
