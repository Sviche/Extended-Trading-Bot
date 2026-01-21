"""
WebSocket Manager - управление WebSocket подключениями к Extended API
Подписывается на orderbook каналы и обновляет кеш в реальном времени
Поддерживает подключение через прокси (HTTP/SOCKS5)
"""

import asyncio
import json
import websockets
from typing import List, Optional
from decimal import Decimal
from urllib.parse import urlparse

# Импорт для прокси поддержки
try:
    import aiohttp
    PROXY_SUPPORT = True
except ImportError:
    PROXY_SUPPORT = False

from modules.core.logger import setup_logger
from modules.helpers.orderbook_cache import orderbook_cache

logger = setup_logger()


class ExtendedWebSocketManager:
    """
    Менеджер WebSocket подключений для получения orderbook обновлений

    Запускает отдельные WebSocket подключения для каждого рынка
    и обновляет глобальный кеш в реальном времени.
    Поддерживает подключение через прокси (HTTP/SOCKS5).
    """

    def __init__(self, markets: List[str], testnet: bool = False, proxies: Optional[List[str]] = None):
        """
        Инициализация менеджера

        Args:
            markets: Список рынков для подписки (например ['BTC-USD', 'ETH-USD'])
            testnet: Использовать тестовую сеть
            proxies: Список прокси для ротации (формат: http://user:pass@host:port или socks5://...)
        """
        self.markets = [m.upper() for m in markets]
        self.testnet = testnet
        self.proxies = proxies or []
        self.proxy_index = 0  # Текущий индекс для ротации прокси
        self.ws_connections = {}
        self.running = False
        self.reconnect_delay = 2  # Быстрое переподключение (2 секунды)
        self.reconnect_count = {}  # Счетчик переподключений для каждого рынка

        # WebSocket URL из документации Extended
        if testnet:
            # Testnet Sepolia
            self.ws_base_url = "wss://api.starknet.sepolia.extended.exchange"
        else:
            # Mainnet
            self.ws_base_url = "wss://api.starknet.extended.exchange"

        # Логируем информацию о прокси
        if self.proxies:
            logger.info(f"🌐 WebSocket Manager: {len(self.proxies)} прокси для подключений")
        else:
            logger.warning("⚠️ WebSocket Manager: прокси не заданы, подключение напрямую")

    async def start(self):
        """Запускает WebSocket подключения для всех рынков"""
        if self.running:
            logger.warning("⚠️ WebSocket Manager уже запущен")
            return

        self.running = True
        # WebSocket подключения запускаются в фоне

        # Запускаем отдельную задачу для каждого рынка
        tasks = []
        for market in self.markets:
            task = asyncio.create_task(self._run_websocket_for_market(market))
            tasks.append(task)

        # Ждем завершения всех задач (они будут работать бесконечно с переподключениями)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        """Останавливает все WebSocket подключения"""
        logger.debug("Остановка WebSocket Manager...")
        self.running = False

        # Закрываем все активные соединения
        for market, ws in self.ws_connections.items():
            if ws:
                try:
                    # Проверяем тип WebSocket (aiohttp или websockets)
                    if hasattr(ws, '_aiohttp_session'):
                        # aiohttp WebSocket
                        if not ws.closed:
                            await ws.close()
                        # Закрываем сессию
                        await ws._aiohttp_session.close()
                    else:
                        # websockets WebSocket
                        if not ws.closed:
                            await ws.close()
                    logger.debug(f"✅ WebSocket для {market} закрыт")
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка закрытия WebSocket для {market}: {e}")

        self.ws_connections.clear()
        logger.debug("WebSocket Manager остановлен")

    def _get_next_proxy(self) -> Optional[str]:
        """
        Получить следующий прокси из списка (ротация по кругу)

        Returns:
            Строка прокси или None если прокси не заданы
        """
        if not self.proxies:
            return None

        proxy = self.proxies[self.proxy_index % len(self.proxies)]
        self.proxy_index += 1
        return proxy

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
        proxy_url = proxy_url.strip()

        # Обрабатываем специальный формат host:port:username:password
        # Пример: gate.nodemaven.com:8080:user:pass
        if '://' not in proxy_url and proxy_url.count(':') >= 3:
            parts = proxy_url.split(':', 3)  # Разделяем на максимум 4 части
            if len(parts) == 4:
                host, port, username, password = parts
                proxy_url = f'http://{username}:{password}@{host}:{port}'
                logger.debug(f"🔄 Конвертирован формат host:port:user:pass в стандартный URL")

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

                logger.warning(f"⚠️ Порт не указан в прокси, использую порт по умолчанию: {default_port}")
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга прокси URL '{proxy_url}': {e}")
            raise

        return proxy_url

    def _parse_proxy(self, proxy_url: str) -> dict:
        """
        Парсит URL прокси в компоненты

        Args:
            proxy_url: URL прокси (http://user:pass@host:port или socks5://...)

        Returns:
            Dict с компонентами прокси
        """
        parsed = urlparse(proxy_url)

        # Определяем тип прокси
        scheme = parsed.scheme.lower()
        if scheme in ('socks5', 'socks5h'):
            proxy_type = 'SOCKS5'
        elif scheme in ('socks4', 'socks4a'):
            proxy_type = 'SOCKS4'
        else:
            # HTTP/HTTPS прокси используем как HTTP CONNECT
            proxy_type = 'HTTP'

        return {
            'type': proxy_type,
            'host': parsed.hostname,
            'port': parsed.port or (1080 if 'socks' in scheme else 8080),
            'username': parsed.username,
            'password': parsed.password,
        }

    async def _connect_with_proxy(self, ws_url: str, proxy_url: str):
        """
        Подключиться к WebSocket через прокси (используя aiohttp)

        Args:
            ws_url: URL WebSocket сервера
            proxy_url: URL прокси

        Returns:
            WebSocket соединение (aiohttp ClientWebSocketResponse)
        """
        if not PROXY_SUPPORT:
            logger.error("❌ aiohttp не установлен! Выполните: pip install aiohttp")
            raise ImportError("aiohttp not installed")

        # Нормализуем прокси URL (добавляем http:// если отсутствует схема)
        normalized_proxy = self._normalize_proxy_url(proxy_url)

        # Создаем aiohttp сессию с прокси
        # aiohttp поддерживает WebSocket через прокси нативно
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        # Парсим прокси для aiohttp (требует BasicAuth если есть логин/пароль)
        parsed_proxy = urlparse(normalized_proxy)
        proxy_auth = None
        if parsed_proxy.username and parsed_proxy.password:
            proxy_auth = aiohttp.BasicAuth(
                login=parsed_proxy.username,
                password=parsed_proxy.password
            )
            # Убираем auth из URL для aiohttp
            proxy_url_clean = f"{parsed_proxy.scheme}://{parsed_proxy.hostname}:{parsed_proxy.port}"
        else:
            proxy_url_clean = normalized_proxy

        # Создаем сессию
        # trust_env=False - игнорировать системные прокси (VPN), использовать только заданный прокси
        session = aiohttp.ClientSession(timeout=timeout, trust_env=False)

        # Подключаемся к WebSocket через прокси
        ws = await session.ws_connect(
            ws_url,
            proxy=proxy_url_clean,
            proxy_auth=proxy_auth,
            heartbeat=15,  # Аналог ping_interval
            timeout=timeout.total,
        )

        # ВАЖНО: Сохраняем сессию в объекте ws для последующего закрытия
        ws._aiohttp_session = session

        return ws

    async def _run_websocket_for_market(self, market: str):
        """
        Запускает WebSocket подключение для конкретного рынка
        Автоматически переподключается при обрыве
        Поддерживает подключение через прокси

        Args:
            market: Название рынка (BTC-USD, ETH-USD и т.д.)
        """
        # Инициализируем счетчик переподключений
        if market not in self.reconnect_count:
            self.reconnect_count[market] = 0

        first_connection = True

        # Формируем URL для подписки на orderbook с depth=1 (только best bid/ask)
        # Обновления каждые 10ms вместо 100ms для полного стакана
        ws_url = f"{self.ws_base_url}/stream.extended.exchange/v1/orderbooks/{market}?depth=1"

        while self.running:
            ws = None
            ws_session = None
            proxy_url = self._get_next_proxy()
            is_aiohttp = False

            try:
                if first_connection:
                    first_connection = False
                    if proxy_url:
                        # Маскируем пароль в логе
                        masked_proxy = self._mask_proxy_password(proxy_url)
                        logger.debug(f"🌐 {market}: подключение через прокси {masked_proxy}")

                # Подключаемся к WebSocket (через прокси или напрямую)
                if proxy_url:
                    ws = await self._connect_with_proxy(ws_url, proxy_url)
                    is_aiohttp = True  # aiohttp WebSocket
                else:
                    ws = await websockets.connect(
                        ws_url,
                        ping_interval=15,  # Отправляем ping каждые 15 сек (как ожидает сервер)
                        ping_timeout=10,   # Ждем pong 10 сек (как требует сервер)
                        close_timeout=5
                    )
                    is_aiohttp = False  # websockets WebSocket

                # Сохраняем соединение
                self.ws_connections[market] = ws

                # Обрабатываем сообщения (разные интерфейсы для websockets и aiohttp)
                if is_aiohttp:
                    # aiohttp WebSocket
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(market, msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"❌ {market}: WebSocket error")
                            break
                else:
                    # websockets WebSocket
                    async for message in ws:
                        await self._handle_message(market, message)

            except websockets.exceptions.ConnectionClosed as e:
                self.reconnect_count[market] += 1
                logger.debug(f"🔌 {market}: WebSocket закрыт ({e.code}: {e.reason})")

                # Логируем только каждое 10-е переподключение
                if self.reconnect_count[market] % 10 == 0:
                    logger.info(f"🔄 {market}: Переподключений: {self.reconnect_count[market]}")

                await asyncio.sleep(self.reconnect_delay)

            except asyncio.TimeoutError:
                # Timeout - это нормально, переподключаемся
                self.reconnect_count[market] += 1
                logger.debug(f"⏱️ {market}: Timeout, переподключение...")
                await asyncio.sleep(self.reconnect_delay)

            except Exception as e:
                self.reconnect_count[market] += 1
                logger.error(f"❌ {market}: Неожиданная ошибка: {type(e).__name__}: {e}")
                await asyncio.sleep(self.reconnect_delay)

            finally:
                # Закрываем WebSocket и сессию (если aiohttp)
                if ws:
                    try:
                        if is_aiohttp:
                            # aiohttp WebSocket
                            if not ws.closed:
                                await ws.close()
                            # Закрываем сессию
                            if hasattr(ws, '_aiohttp_session'):
                                await ws._aiohttp_session.close()
                        else:
                            # websockets WebSocket
                            if not ws.closed:
                                await ws.close()
                    except:
                        pass

        logger.info(f"🛑 WebSocket для {market} остановлен")

    async def _handle_message(self, market: str, message: str):
        """
        Обрабатывает входящие WebSocket сообщения

        Args:
            market: Название рынка
            message: JSON сообщение от сервера
        """
        try:
            data = json.loads(message)

            # Extended WebSocket возвращает сообщения в формате:
            # {
            #   "ts": 1701563440000,
            #   "type": "SNAPSHOT|DELTA",
            #   "data": {
            #     "m": "BTC-USD",
            #     "b": [{"p": "25670", "q": "0.1"}],
            #     "a": [{"p": "25770", "q": "0.1"}]
            #   },
            #   "seq": 1
            # }

            msg_type = data.get('type')
            msg_data = data.get('data', {})

            if msg_type in ['SNAPSHOT', 'DELTA']:
                bids = msg_data.get('b', [])
                asks = msg_data.get('a', [])

                if bids and asks:
                    # Обновляем кеш молча (обновления каждые 10ms)
                    orderbook_cache.update_orderbook(market, bids, asks)
            else:
                logger.debug(f"🔍 {market}: Неизвестный тип сообщения: {msg_type}")

        except json.JSONDecodeError as e:
            logger.error(f"❌ {market}: Ошибка парсинга JSON: {e}")
        except Exception as e:
            logger.error(f"❌ {market}: Ошибка обработки сообщения: {e}")

    def _mask_proxy_password(self, proxy_url: str) -> str:
        """
        Маскирует пароль в URL прокси для безопасного логирования

        Args:
            proxy_url: URL прокси

        Returns:
            URL с замаскированным паролем
        """
        try:
            parsed = urlparse(proxy_url)
            if parsed.password:
                # Заменяем пароль на ****
                masked = proxy_url.replace(f":{parsed.password}@", ":****@")
                return masked
            return proxy_url
        except:
            return proxy_url

    def get_connection_status(self) -> dict:
        """
        Возвращает статус всех WebSocket подключений

        Returns:
            Dict с информацией о статусе подключений
        """
        status = {
            'running': self.running,
            'markets': {},
            'total_markets': len(self.markets),
            'active_connections': 0
        }

        for market in self.markets:
            ws = self.ws_connections.get(market)
            is_connected = ws is not None and not ws.closed

            if is_connected:
                status['active_connections'] += 1

            cache_status = orderbook_cache.get_cache_status(market)

            status['markets'][market] = {
                'connected': is_connected,
                'has_cache': cache_status is not None,
                'cache_age': cache_status.get('age', None) if cache_status else None,
                'reconnects': self.reconnect_count.get(market, 0)
            }

        return status
