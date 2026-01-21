"""
Extended Client - Обертка для работы с Extended Protocol SDK
Поддерживает подключение через прокси (per-account прокси через SDK patch)
"""

import asyncio
import os
from decimal import Decimal
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG, TESTNET_CONFIG
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.orders import OrderSide, TimeInForce

from modules.core.logger import setup_logger
from modules.helpers.market_rules import market_rules
from modules.helpers.sdk_proxy_patch import (
    install_sdk_proxy_patch,
    set_current_proxy,
    get_current_proxy,
    normalize_proxy_url,
    mask_proxy_url
)
from settings import TRADING_SETTINGS

# Устанавливаем патч SDK при импорте модуля
_sdk_patch_installed = install_sdk_proxy_patch()


def setup_proxy_env(proxy: Optional[str]) -> None:
    """
    Установить прокси через переменные окружения для SDK

    ВАЖНО: SDK использует httpx который поддерживает HTTP_PROXY/HTTPS_PROXY.
    Эта функция устанавливает глобальные переменные окружения.

    Поддерживаемые форматы:
    - host:port
    - host:port:username:password  (специальный формат)
    - user:pass@host:port
    - http://user:pass@host:port

    Args:
        proxy: URL прокси (http://user:pass@host:port или socks5://...)
               None для отключения прокси
    """
    if proxy:
        from urllib.parse import urlparse

        proxy = proxy.strip()

        # Обрабатываем специальный формат host:port:username:password
        # Пример: gate.nodemaven.com:8080:user:pass
        if '://' not in proxy and proxy.count(':') >= 3:
            parts = proxy.split(':', 3)  # Разделяем на максимум 4 части
            if len(parts) == 4:
                host, port, username, password = parts
                proxy = f'http://{username}:{password}@{host}:{port}'

        # Нормализуем прокси URL (добавляем http:// если отсутствует схема)
        if '://' not in proxy:
            proxy = f'http://{proxy}'

        # Парсим и добавляем порт если отсутствует
        try:
            parsed = urlparse(proxy)
            if not parsed.port:
                default_port = 1080 if 'socks' in parsed.scheme.lower() else 8080
                # Пересобираем URL с портом
                if parsed.username and parsed.password:
                    proxy = f"{parsed.scheme}://{parsed.username}:{parsed.password}@{parsed.hostname}:{default_port}"
                else:
                    proxy = f"{parsed.scheme}://{parsed.hostname}:{default_port}"
        except Exception:
            pass  # Если парсинг не удался, используем как есть

        # httpx SDK поддерживает HTTP_PROXY и HTTPS_PROXY
        os.environ['HTTP_PROXY'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
        # Для SOCKS прокси httpx требует httpx-socks, но мы используем aiohttp-socks
        # Поэтому для SOCKS мы полагаемся на системный уровень
    else:
        # Удаляем прокси переменные
        os.environ.pop('HTTP_PROXY', None)
        os.environ.pop('HTTPS_PROXY', None)


@dataclass
class AccountConfig:
    """Конфигурация аккаунта Extended"""
    name: str
    private_key: str  # Stark private key
    public_key: str   # Stark public key
    api_key: str      # API key from Extended
    vault_id: int     # Vault ID / Position ID
    proxy: str        # Прокси для этого аккаунта


class ExtendedClient:
    """
    Клиент для работы с Extended Protocol

    Основные возможности:
    - Размещение маркет и лимит ордеров
    - Получение информации о позициях
    - Получение баланса
    - Управление leverage

    Каждый клиент использует свой прокси (per-account proxy).
    """

    def __init__(
        self,
        account_config: AccountConfig,
        testnet: bool = False,
        logger=None
    ):
        """
        Инициализация клиента

        Args:
            account_config: Конфигурация аккаунта
            testnet: Использовать тестовую сеть (по умолчанию mainnet)
            logger: Логгер (если None - создается новый)
        """
        self.account_config = account_config
        self.testnet = testnet
        self.logger = logger or setup_logger()

        # Прокси для этого аккаунта (нормализованный)
        self.proxy = normalize_proxy_url(account_config.proxy) if account_config.proxy else None

        # Выбор конфигурации окружения
        self.config = TESTNET_CONFIG if testnet else MAINNET_CONFIG

        # Создание Stark аккаунта
        self.stark_account = StarkPerpetualAccount(
            vault=account_config.vault_id,
            private_key=account_config.private_key,
            public_key=account_config.public_key,
            api_key=account_config.api_key
        )

        # Trading client будет создан асинхронно
        self.trading_client: Optional[PerpetualTradingClient] = None
        self._initialized = False

        proxy_info = f", proxy: {mask_proxy_url(self.proxy)}" if self.proxy else ", proxy: None"
        self.logger.debug(
            f"Extended client создан для {account_config.name} "
            f"(vault: {account_config.vault_id}, "
            f"testnet: {testnet}{proxy_info})"
        )

    def _set_proxy(self):
        """Установить прокси для текущего аккаунта перед SDK операцией"""
        set_current_proxy(self.proxy)

    def _clear_proxy(self):
        """Очистить прокси после SDK операции"""
        set_current_proxy(None)

    async def initialize(self):
        """Асинхронная инициализация клиента"""
        if self._initialized:
            return

        try:
            # Устанавливаем прокси перед созданием клиента
            self._set_proxy()

            self.trading_client = PerpetualTradingClient(
                endpoint_config=self.config,
                stark_account=self.stark_account
            )
            self._initialized = True
            self.logger.debug(f"{self.account_config.name} | Клиент инициализирован")
        except Exception as e:
            self.logger.error(f"{self.account_config.name} | Ошибка инициализации: {e}")
            raise
        finally:
            self._clear_proxy()

    async def get_balance(self) -> Dict[str, Any]:
        """
        Получить баланс аккаунта

        Returns:
            Dict с информацией о балансе
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            balance = await self.trading_client.account.get_balance()
            self.logger.debug(f"Баланс {self.account_config.name}: {balance}")
            return balance
        except Exception as e:
            self.logger.error(f"Ошибка получения баланса: {e}")
            raise
        finally:
            self._clear_proxy()

    async def get_positions(self, market: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получить текущие позиции

        Args:
            market: Фильтр по рынку (например "BTC-USD"), если None - все позиции

        Returns:
            Список позиций
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            # Вызываем SDK метод
            market_names = [market] if market else None
            self.logger.debug(
                f"{self.account_config.name}: запрос позиций для markets={market_names}"
            )

            positions_response = await self.trading_client.account.get_positions(
                market_names=market_names
            )

            self.logger.debug(
                f"{self.account_config.name}: получен ответ позиций, "
                f"тип: {type(positions_response)}, "
                f"hasattr model_dump: {hasattr(positions_response, 'model_dump')}"
            )

            # Конвертируем WrappedApiResponse в данные
            if hasattr(positions_response, 'model_dump'):
                data = positions_response.model_dump()
                self.logger.debug(
                    f"{self.account_config.name}: после model_dump: "
                    f"тип={type(data)}"
                )
            else:
                data = positions_response
                self.logger.debug(
                    f"{self.account_config.name}: positions_response напрямую: {type(data)}"
                )

            # API возвращает словарь, а не список!
            # Нужно извлечь список позиций из словаря
            positions_list = []

            if isinstance(data, dict):
                self.logger.debug(
                    f"{self.account_config.name}: data - словарь с ключами: {list(data.keys())}"
                )
                # Возможные варианты структуры:
                # 1. {'positions': [...]}
                # 2. {'data': [...]}
                # 3. Сам словарь является позицией
                if 'positions' in data:
                    positions_list = data['positions']
                elif 'data' in data:
                    positions_list = data['data']
                else:
                    # Возможно, сам словарь - это одна позиция
                    # Проверяем наличие ключевых полей позиции
                    if 'market' in data or 'side' in data:
                        positions_list = [data]
                    else:
                        # Иначе считаем что это пустой результат
                        positions_list = []
            elif isinstance(data, list):
                positions_list = data
            else:
                self.logger.warning(
                    f"{self.account_config.name}: неожиданный тип данных: {type(data)}"
                )
                positions_list = []

            self.logger.debug(
                f"{self.account_config.name}: извлечено позиций из ответа: {len(positions_list)}"
            )

            # Если это список моделей, конвертируем каждую
            if isinstance(positions_list, list):
                positions_list = [
                    p.model_dump() if hasattr(p, 'model_dump') else p
                    for p in positions_list
                ]
                self.logger.debug(
                    f"{self.account_config.name}: после конвертации списка моделей: {len(positions_list)} позиций"
                )

            # Проверяем структуру позиций
            if positions_list:
                self.logger.debug(
                    f"{self.account_config.name}: пример структуры первой позиции: {positions_list[0]}"
                )
                # Проверяем наличие критических полей
                required_fields = ['side', 'size', 'market']
                for pos in positions_list:
                    if isinstance(pos, dict):
                        missing_fields = [f for f in required_fields if f not in pos]
                        if missing_fields:
                            self.logger.warning(
                                f"{self.account_config.name}: позиция без полей {missing_fields}: {pos}"
                            )

            self.logger.debug(
                f"{self.account_config.name}: итого позиций: {len(positions_list)}"
            )
            return positions_list

        except Exception as e:
            import traceback
            self.logger.error(
                f"{self.account_config.name}: ошибка получения позиций: {e}\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            raise
        finally:
            self._clear_proxy()

    async def place_market_order(
        self,
        market: str,
        side: str,  # "BUY" или "SELL"
        amount: Decimal,
        market_data_provider,  # MarketDataProvider instance
        reduce_only: bool = False,
        suppress_missing_position_error: bool = False,
        silent: bool = False
    ) -> Dict[str, Any]:
        """
        Разместить маркет-ордер

        На Extended нет полноценных маркет-ордеров, поэтому используем
        лимитный ордер с IOC и агрессивной ценой

        Args:
            market: Рынок (например "BTC-USD")
            side: Направление ("BUY" или "SELL")
            amount: Размер позиции в базовом активе
            market_data_provider: Провайдер для получения цен
            reduce_only: Только закрытие позиции
            suppress_missing_position_error: Не логировать ошибку 1137 "Position is missing"
            silent: Подавить все логирование (для mass операций)

        Returns:
            Информация о размещенном ордере
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            # Конвертируем строку в OrderSide enum
            order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL

            # Получаем агрессивную цену для гарантированного исполнения
            price = await market_data_provider.get_market_price_for_order(
                market=market,
                side=side,
                aggressive=True
            )

            # Округляем цену до правильной точности (min_price_change из market rules)
            price = market_rules.round_price_to_min_change(market, price)

            # Округляем цену для вывода
            price_display = float(price)

            if not silent:
                self.logger.info(
                    f"{self.account_config.name} | Размещение маркет-ордера: "
                    f"{market} {side} {amount} @ ~${price_display:.2f}"
                )

            # Размещаем как лимитный ордер с IOC
            placed_order = await self.trading_client.place_order(
                market_name=market,
                amount_of_synthetic=amount,
                price=price,
                side=order_side,
                post_only=False,
                reduce_only=reduce_only,
                time_in_force=TimeInForce.IOC  # Immediate or Cancel
            )

            # Конвертируем WrappedApiResponse в словарь
            order_dict = placed_order.model_dump() if hasattr(placed_order, 'model_dump') else placed_order

            # Попытка получить ID из разных полей
            order_id = 'unknown'
            if isinstance(order_dict, dict):
                order_id = order_dict.get('id') or order_dict.get('order_id') or order_dict.get('orderId', 'unknown')
            elif hasattr(placed_order, 'id'):
                order_id = placed_order.id
            elif hasattr(placed_order, 'order_id'):
                order_id = placed_order.order_id

            self.logger.debug(
                f"{self.account_config.name} | Ордер размещен: ID={order_id}"
            )

            return order_dict

        except Exception as e:
            # Если это ошибка "Position is missing" (1137) и мы хотим ее подавить
            error_msg = str(e)
            is_missing_position = '1137' in error_msg or 'position is missing' in error_msg.lower()

            if is_missing_position and suppress_missing_position_error:
                # Тихо пробрасываем исключение без логирования
                raise
            else:
                # Логируем все остальные ошибки
                self.logger.error(f"{self.account_config.name} | Ошибка размещения ордера: {e}")
                raise
        finally:
            self._clear_proxy()

    async def place_limit_order(
        self,
        market: str,
        side: str,  # "BUY" или "SELL"
        amount: Decimal,
        price: Decimal,
        post_only: bool = False,
        reduce_only: bool = False,
        time_in_force: str = "GTT"
    ) -> Dict[str, Any]:
        """
        Разместить лимитный ордер

        Args:
            market: Рынок (например "BTC-USD")
            side: Направление ("BUY" или "SELL")
            amount: Размер позиции в базовом активе
            price: Цена ордера
            post_only: Только maker (не исполнять немедленно)
            reduce_only: Только закрытие позиции
            time_in_force: Тип срока действия ("GTT", "IOC")

        Returns:
            Информация о размещенном ордере
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            # Конвертируем строку в OrderSide enum
            order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL

            # Конвертируем time_in_force
            tif = TimeInForce.GTT if time_in_force == "GTT" else TimeInForce.IOC

            # Округляем цену до правильной точности (min_price_change из market rules)
            price = market_rules.round_price_to_min_change(market, price)

            self.logger.debug(
                f"Размещение лимит-ордера {self.account_config.name}: "
                f"{market} {side} {amount} @ {price}"
            )

            # Используем метод SDK для создания и размещения ордера
            placed_order = await self.trading_client.place_order(
                market_name=market,
                amount_of_synthetic=amount,
                price=price,
                side=order_side,
                post_only=post_only,
                reduce_only=reduce_only,
                time_in_force=tif
            )

            # Конвертируем WrappedApiResponse в словарь
            order_dict = placed_order.model_dump() if hasattr(placed_order, 'model_dump') else placed_order

            # Debug: Логируем структуру ответа
            self.logger.debug(
                f"{self.account_config.name} | Структура ответа: тип={type(order_dict)}, "
                f"ключи={list(order_dict.keys()) if isinstance(order_dict, dict) else 'N/A'}"
            )

            # Попытка получить ID из разных полей
            order_id = 'unknown'
            if isinstance(order_dict, dict):
                # Проверяем все возможные варианты ключей для ID
                order_id = (
                    order_dict.get('id') or
                    order_dict.get('order_id') or
                    order_dict.get('orderId') or
                    order_dict.get('clientOrderId') or
                    order_dict.get('client_order_id') or
                    'unknown'
                )
            elif hasattr(placed_order, 'id'):
                order_id = placed_order.id
            elif hasattr(placed_order, 'order_id'):
                order_id = placed_order.order_id

            self.logger.debug(
                f"Ордер размещен: ID={order_id}"
            )

            return order_dict

        except Exception as e:
            # Подробное логирование ошибки с типом и traceback
            import traceback
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.logger.error(f"Ошибка размещения лимит-ордера: {error_msg}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            self._clear_proxy()

    async def get_open_orders(self, market: str = None, market_data_provider=None) -> List[Dict[str, Any]]:
        """
        Получить список открытых ордеров через REST API

        Args:
            market: Рынок (опционально, для фильтрации)
            market_data_provider: MarketDataProvider для REST запросов (опционально)

        Returns:
            Список открытых ордеров
        """
        await self._ensure_initialized()

        try:
            # Если нет market_data_provider, импортируем и создаём его
            if market_data_provider is None:
                from modules.helpers.market_data import MarketDataProvider
                network = 'mainnet'  # По умолчанию mainnet
                market_data_provider = MarketDataProvider(network=network)

            # Получаем открытые ордера через REST API
            orders_list = await market_data_provider.get_open_orders_rest(
                api_key=self.account_config.api_key,
                market=market
            )

            # Фильтруем только открытые ордера (NEW, PARTIALLY_FILLED, PENDING)
            open_orders = [
                o for o in orders_list
                if isinstance(o, dict) and o.get('status') in ['NEW', 'PARTIALLY_FILLED', 'PENDING']
            ]

            self.logger.debug(
                f"{self.account_config.name} | Открытых ордеров: {len(open_orders)}"
                + (f" для {market}" if market else "")
            )

            return open_orders

        except Exception as e:
            self.logger.error(
                f"{self.account_config.name} | Ошибка получения открытых ордеров: {e}"
            )
            return []

    async def cancel_order(self, order_id: str) -> bool:
        """
        Отменить ордер по ID

        Args:
            order_id: ID ордера

        Returns:
            True если отменен успешно
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            await self.trading_client.orders.cancel_order(order_id=order_id)
            self.logger.debug(f"{self.account_config.name} | Ордер {order_id} отменен")
            return True
        except Exception as e:
            self.logger.debug(f"{self.account_config.name} | Ошибка отмены ордера {order_id}: {e}")
            return False
        finally:
            self._clear_proxy()

    async def cancel_all_orders(self, market: str = None, market_data_provider=None) -> int:
        """
        Отменить все открытые ордера

        Args:
            market: Рынок (опционально, для фильтрации)
            market_data_provider: MarketDataProvider для REST запросов (опционально)

        Returns:
            Количество отмененных ордеров
        """
        open_orders = await self.get_open_orders(market=market, market_data_provider=market_data_provider)

        if not open_orders:
            return 0

        self.logger.debug(
            f"{self.account_config.name} | Отмена {len(open_orders)} ордеров"
            + (f" для {market}" if market else "")
        )

        cancelled_count = 0
        for order in open_orders:
            order_id = order.get('id') or order.get('orderId') or order.get('order_id')
            if order_id:
                if await self.cancel_order(order_id):
                    cancelled_count += 1
                # Небольшая задержка между отменами
                await asyncio.sleep(0.2)

        return cancelled_count

    async def get_leverage(self, market: str) -> Dict[str, Any]:
        """
        Получить текущий leverage для рынка

        Args:
            market: Рынок (например "BTC-USD")

        Returns:
            Информация о leverage
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            leverage_info = await self.trading_client.account.get_leverage()
            # Фильтруем по рынку если нужно
            if market and isinstance(leverage_info, dict):
                return leverage_info.get(market, {})
            return leverage_info
        except Exception as e:
            self.logger.error(f"Ошибка получения leverage: {e}")
            raise
        finally:
            self._clear_proxy()

    async def update_leverage(self, market: str, leverage: int) -> bool:
        """
        Обновить leverage для рынка

        Args:
            market: Рынок (например "BTC-USD")
            leverage: Новое значение leverage

        Returns:
            True если обновлено успешно
        """
        await self._ensure_initialized()

        try:
            self._set_proxy()
            await self.trading_client.account.update_leverage(
                market_name=market,
                leverage=Decimal(leverage)
            )
            self.logger.debug(f"{self.account_config.name} | Leverage {market}: {leverage}x")
            return True
        except Exception as e:
            self.logger.error(f"{self.account_config.name} | Ошибка leverage: {e}")
            return False
        finally:
            self._clear_proxy()

    async def _ensure_initialized(self):
        """Убедиться что клиент инициализирован"""
        if not self._initialized:
            await self.initialize()

    async def mass_cancel_all_orders(self, market_data_provider=None) -> bool:
        """
        Массовая отмена ВСЕХ ордеров аккаунта через REST API

        Использует endpoint POST /api/v1/user/order/massCancel с cancelAll=true
        Это более эффективно чем отменять ордера по одному.

        Args:
            market_data_provider: MarketDataProvider для HTTP запросов

        Returns:
            True если запрос прошел успешно, False при ошибке
        """
        try:
            if market_data_provider is None:
                self.logger.warning(
                    f"{self.account_config.name} | mass_cancel_all_orders: "
                    "market_data_provider не передан, невозможно выполнить запрос"
                )
                return False

            # Получаем сессию из market_data_provider
            session = await market_data_provider._get_session()
            base_url = market_data_provider.base_url

            url = f"{base_url}/user/order/massCancel"
            headers = {
                'X-Api-Key': self.account_config.api_key,
                'Content-Type': 'application/json',
                'User-Agent': 'Extended-Bot/0.1'
            }

            # Отменяем ВСЕ ордера аккаунта
            payload = {
                'cancelAll': True
            }

            async with session.post(url, json=payload, headers=headers) as response:
                data = await response.json()

                if response.status == 200 and data.get('status') == 'OK':
                    self.logger.debug(f"{self.account_config.name} | mass cancel OK")
                    return True
                else:
                    error_msg = data.get('error', {}).get('message', 'Unknown error')
                    self.logger.debug(f"{self.account_config.name} | mass cancel error: {error_msg}")
                    return False

        except Exception as e:
            self.logger.error(
                f"{self.account_config.name} | Ошибка mass_cancel_all_orders: {e}"
            )
            return False

    async def close(self):
        """Закрыть соединения и освободить ресурсы"""
        if self.trading_client:
            # Пытаемся закрыть HTTP сессии SDK если они есть
            try:
                # Проверяем наличие HTTP клиентов в SDK
                if hasattr(self.trading_client, '_client') and hasattr(self.trading_client._client, 'aclose'):
                    await self.trading_client._client.aclose()
                    self.logger.debug(f"{self.account_config.name}: SDK HTTP клиент закрыт")
                elif hasattr(self.trading_client, 'close'):
                    await self.trading_client.close()
                    self.logger.debug(f"{self.account_config.name}: SDK клиент закрыт через close()")

                # Небольшая задержка для корректного закрытия
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.debug(f"{self.account_config.name}: не удалось закрыть SDK клиент: {e}")

            # Явно очищаем ссылку на клиент
            self.trading_client = None

            # Помечаем как неинициализированный
            self._initialized = False
            self.logger.debug(f"Клиент {self.account_config.name} закрыт")
