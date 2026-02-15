"""
SDK Proxy Patch - Monkey-patch для Extended SDK с поддержкой per-account прокси

Проблема:
    Extended SDK (x10-python-trading-starknet) создает aiohttp сессии без поддержки прокси.
    Он полагается на глобальные переменные окружения HTTP_PROXY/HTTPS_PROXY,
    что не позволяет использовать разные прокси для разных аккаунтов.

Решение:
    Этот модуль патчит BaseModule.get_session() чтобы создавать сессии
    с ProxyConnector, используя прокси из thread-local хранилища.

Использование:
    from modules.helpers.sdk_proxy_patch import install_sdk_proxy_patch, set_current_proxy

    # Один раз при старте приложения
    install_sdk_proxy_patch()

    # Перед каждой SDK операцией устанавливаем прокси для текущего аккаунта
    set_current_proxy("http://user:pass@host:port")

    # Используем SDK как обычно
    client = PerpetualTradingClient(...)
    await client.markets.get_markets()

    # После операции очищаем прокси
    set_current_proxy(None)
"""

import threading
import aiohttp
from typing import Optional
from urllib.parse import urlparse

try:
    from aiohttp_socks import ProxyConnector
    PROXY_SUPPORT = True
except ImportError:
    PROXY_SUPPORT = False

from modules.core.logger import setup_logger

logger = setup_logger()

# Thread-local хранилище для текущего прокси
_proxy_local = threading.local()

# Флаг установки патча
_patch_installed = False

# Оригинальный метод get_session
_original_get_session = None


def normalize_proxy_url(proxy: str) -> str:
    """
    Нормализует URL прокси - добавляет схему и порт если отсутствуют.

    Поддерживаемые форматы:
    - host:port
    - host:port:username:password  (NodeMaven формат)
    - user:pass@host:port
    - http://user:pass@host:port
    """
    proxy = proxy.strip()

    # Формат host:port:username:password (NodeMaven)
    if '://' not in proxy and proxy.count(':') >= 3:
        parts = proxy.split(':', 3)
        if len(parts) == 4:
            host, port, username, password = parts
            return f'http://{username}:{password}@{host}:{port}'

    # Добавляем http:// если нет схемы
    if '://' not in proxy:
        proxy = f'http://{proxy}'

    # Добавляем порт по умолчанию если отсутствует
    try:
        parsed = urlparse(proxy)
        if not parsed.port:
            default_port = 1080 if 'socks' in parsed.scheme.lower() else 8080
            if parsed.username and parsed.password:
                proxy = f"{parsed.scheme}://{parsed.username}:{parsed.password}@{parsed.hostname}:{default_port}"
            else:
                proxy = f"{parsed.scheme}://{parsed.hostname}:{default_port}"
    except Exception:
        pass

    return proxy


def mask_proxy_url(proxy: str) -> str:
    """Маскирует пароль в URL прокси для логирования"""
    try:
        parsed = urlparse(proxy)
        if parsed.password:
            return proxy.replace(f":{parsed.password}@", ":****@")
    except:
        pass
    return proxy


def get_current_proxy() -> Optional[str]:
    """Получить текущий прокси для thread/task"""
    return getattr(_proxy_local, 'proxy', None)


def set_current_proxy(proxy: Optional[str]) -> None:
    """
    Установить прокси для текущего thread/task.

    Args:
        proxy: URL прокси или None для отключения
    """
    if proxy:
        _proxy_local.proxy = normalize_proxy_url(proxy)
    else:
        _proxy_local.proxy = None


def install_sdk_proxy_patch() -> bool:
    """
    Установить monkey-patch для SDK BaseModule.

    Должен вызываться один раз при старте приложения,
    ДО создания любых PerpetualTradingClient.

    Returns:
        True если патч установлен успешно
    """
    global _patch_installed, _original_get_session

    if _patch_installed:
        logger.debug("SDK proxy patch уже установлен")
        return True

    if not PROXY_SUPPORT:
        logger.error("aiohttp-socks не установлен! pip install aiohttp-socks")
        return False

    try:
        from x10.perpetual.trading_client.base_module import BaseModule
        from x10.utils.http import CLIENT_TIMEOUT

        # Сохраняем оригинальный метод
        _original_get_session = BaseModule.get_session

        # Создаем patched версию
        async def patched_get_session(self):
            """
            Patched версия get_session с поддержкой per-client сессий.

            НОВАЯ ЛОГИКА:
            1. Если у клиента есть self._custom_session (создан ExtendedClient), использует его
            2. Иначе использует старую логику с глобальным прокси (для обратной совместимости)
            
            Это устраняет race condition т.к. каждый клиент использует СВОЮ сессию.
            """
            # ПРИОРИТЕТ 1: Если есть кастомная сессия (per-client), используем её
            has_custom = hasattr(self, '_custom_session')
            logger.debug(f"patched_get_session вызван: self={type(self).__name__}, has_custom={has_custom}")
            
            if has_custom:
                custom_session = self._custom_session
                logger.debug(f"_custom_session найден: {custom_session is not None}, session_id={id(custom_session)}")
                if custom_session is not None:
                    # Логируем connector прокси если есть
                    connector = custom_session.connector
                    proxy_info = "unknown"
                    if connector and hasattr(connector, '_proxy_url'):
                        proxy_info = mask_proxy_url(str(connector._proxy_url)) if connector._proxy_url else "no proxy"
                    logger.debug(f"Использую per-client сессию id={id(custom_session)}, proxy={proxy_info}")
                    return custom_session
            
            # ПРИОРИТЕТ 2: Старая логика с глобальным прокси (для обратной совместимости)
            # Получаем текущий прокси из thread-local
            current_proxy = get_current_proxy()
            
            # Проверяем нужно ли пересоздать сессию
            session_proxy = getattr(self, '_session_proxy', None)
            need_recreate = (
                self._BaseModule__session is None or 
                session_proxy != current_proxy
            )
            
            if need_recreate:
                # Закрываем старую сессию если есть
                if self._BaseModule__session is not None:
                    try:
                        await self._BaseModule__session.close()
                        logger.debug("Старая SDK сессия закрыта")
                    except Exception as e:
                        logger.warning(f"Ошибка закрытия старой сессии: {e}")
                
                # Создаем новую сессию
                if current_proxy:
                    # Создаем сессию с прокси
                    try:
                        connector = ProxyConnector.from_url(current_proxy)
                        self._BaseModule__session = aiohttp.ClientSession(
                            connector=connector,
                            timeout=CLIENT_TIMEOUT,
                            trust_env=False  # Игнорируем системные прокси
                        )
                        logger.debug(f"SDK сессия создана с прокси: {mask_proxy_url(current_proxy)}")
                    except Exception as e:
                        logger.error(f"Ошибка создания SDK сессии с прокси: {e}")
                        # Fallback на оригинальную логику
                        self._BaseModule__session = aiohttp.ClientSession(timeout=CLIENT_TIMEOUT)
                else:
                    # Без прокси - оригинальная логика
                    self._BaseModule__session = aiohttp.ClientSession(timeout=CLIENT_TIMEOUT)
                    logger.debug("SDK сессия создана БЕЗ прокси")
                
                # Сохраняем текущий прокси для проверки в следующий раз
                self._session_proxy = current_proxy

            return self._BaseModule__session

        # Применяем патч
        BaseModule.get_session = patched_get_session
        _patch_installed = True

        logger.info("SDK proxy patch установлен успешно")
        return True

    except ImportError as e:
        logger.error(f"Не удалось импортировать SDK модули: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка установки SDK proxy patch: {e}")
        return False


def uninstall_sdk_proxy_patch() -> bool:
    """
    Удалить monkey-patch и восстановить оригинальный метод.

    Returns:
        True если патч удален успешно
    """
    global _patch_installed, _original_get_session

    if not _patch_installed:
        return True

    if _original_get_session is None:
        logger.warning("Оригинальный get_session не сохранен")
        return False

    try:
        from x10.perpetual.trading_client.base_module import BaseModule
        BaseModule.get_session = _original_get_session
        _patch_installed = False
        _original_get_session = None
        logger.info("SDK proxy patch удален")
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления SDK proxy patch: {e}")
        return False


class SDKProxyContext:
    """
    Context manager для установки прокси на время SDK операций.

    Использование:
        async with SDKProxyContext("http://user:pass@host:port"):
            await client.markets.get_markets()
    """

    def __init__(self, proxy: Optional[str]):
        self.proxy = proxy
        self.previous_proxy = None

    async def __aenter__(self):
        self.previous_proxy = get_current_proxy()
        set_current_proxy(self.proxy)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        set_current_proxy(self.previous_proxy)
        return False


# Синхронная версия контекст-менеджера
class SDKProxyContextSync:
    """Синхронный context manager для установки прокси"""

    def __init__(self, proxy: Optional[str]):
        self.proxy = proxy
        self.previous_proxy = None

    def __enter__(self):
        self.previous_proxy = get_current_proxy()
        set_current_proxy(self.proxy)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        set_current_proxy(self.previous_proxy)
        return False


# ============================================================================
# SDK Market Order Patch
# ============================================================================
# x10 SDK всегда отправляет type=OrderType.LIMIT для ВСЕХ ордеров.
# SDK также блокирует TimeInForce.FOK (Fill or Kill).
# StarkNet подпись НЕ включает поле type, поэтому замена type безопасна.
#
# Этот патч добавляет метод place_market_order_native() к PerpetualTradingClient,
# который создаёт ордер через стандартный SDK (валидная подпись),
# затем меняет type на MARKET перед отправкой на биржу.
# ============================================================================

_market_order_patch_installed = False


def install_sdk_market_order_patch() -> bool:
    """
    Установить monkey-patch для поддержки настоящих MARKET ордеров.

    x10 SDK (x10-python-trading-starknet) имеет два ограничения:
    1. create_order_object() всегда ставит type=OrderType.LIMIT
    2. TimeInForce.FOK явно заблокирован ValueError

    Этот патч добавляет метод place_market_order_native() к PerpetualTradingClient,
    который:
    1. Создаёт ордер через SDK (с IOC для прохождения валидации)
    2. Меняет type на MARKET через model_copy() (безопасно: не входит в StarkNet подпись)
    3. Отправляет на биржу через стандартный order management module

    Returns:
        True если патч установлен успешно
    """
    global _market_order_patch_installed

    if _market_order_patch_installed:
        logger.debug("SDK market order patch уже установлен")
        return True

    try:
        from x10.perpetual.trading_client import PerpetualTradingClient
        from x10.perpetual.order_object import create_order_object
        from x10.perpetual.orders import OrderType, TimeInForce
        from x10.utils.date import utc_now
        from datetime import timedelta

        async def place_market_order_native(
            self,
            market_name: str,
            amount_of_synthetic,
            price,
            side,
            reduce_only: bool = False,
            expire_time=None,
            stop_loss=None,
            tp_sl_type=None,
        ):
            """
            Разместить настоящий MARKET ордер на Extended Exchange.

            Процесс:
            1. Создаёт ордер через стандартный SDK (type=LIMIT, time_in_force=IOC)
            2. Модифицирует type на MARKET через model_copy()
               (безопасно т.к. type НЕ входит в StarkNet подпись/settlement)
            3. Отправляет модифицированный ордер через стандартный API

            Args:
                market_name: Рынок (например "BTC-USD")
                amount_of_synthetic: Количество базового актива
                price: Агрессивная цена для исполнения
                side: OrderSide.BUY или OrderSide.SELL
                reduce_only: Только закрытие позиции
                expire_time: Время истечения ордера

            Returns:
                WrappedApiResponse[PlacedOrderModel]
            """
            # Получаем приватные атрибуты через name mangling
            stark_account = self._PerpetualTradingClient__stark_account
            if not stark_account:
                raise ValueError("Stark account is not set")

            markets = self._PerpetualTradingClient__markets
            if not markets:
                markets = await self.markets_info.get_markets_dict()
                self._PerpetualTradingClient__markets = markets

            market = markets.get(market_name)
            if not market:
                raise ValueError(f"Market {market_name} not found")

            config = self._PerpetualTradingClient__config

            if expire_time is None:
                expire_time = utc_now() + timedelta(hours=1)

            # Шаг 1: Создаём ордер через SDK с IOC (SDK блокирует FOK)
            order = create_order_object(
                account=stark_account,
                market=market,
                amount_of_synthetic=amount_of_synthetic,
                price=price,
                side=side,
                post_only=False,
                expire_time=expire_time,
                time_in_force=TimeInForce.IOC,
                starknet_domain=config.starknet_domain,
                reduce_only=reduce_only,
                stop_loss=stop_loss,
                tp_sl_type=tp_sl_type,
            )

            # Шаг 2: Меняем type на MARKET
            # Безопасно: StarkNet подпись вычисляется из (amount, price, nonce, expiry, ...)
            # и НЕ включает поле type. Поле type — это только exchange-level метаданные.
            order = order.model_copy(update={
                'type': OrderType.MARKET,
            })

            # Шаг 3: Отправляем через стандартный order management module
            return await self.orders.place_order(order)

        # Добавляем метод к PerpetualTradingClient
        PerpetualTradingClient.place_market_order_native = place_market_order_native
        _market_order_patch_installed = True

        logger.info("SDK market order patch установлен успешно")
        return True

    except ImportError as e:
        logger.error(f"Не удалось импортировать SDK модули для market order patch: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка установки SDK market order patch: {e}")
        return False
