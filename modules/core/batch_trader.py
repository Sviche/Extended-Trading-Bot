"""
Batch Trader - Торговля пачками аккаунтов по логике Lighter-bot

Ключевые принципы:
- Аккаунты торгуют группами (пачками) от 5 до 7 аккаунтов
- В каждой пачке есть лонг-аккаунты (1-3) и шорт-аккаунты (остальные)
- Количество лонгов ≠ количеству шортов (для хеджирования)

Логика хеджирования:
- batch_size_usd - это размер ВСЕЙ ПАЧКИ, а не одной позиции
- Лонги делят между собой половину этого размера
- Шорты делят между собой вторую половину
- Таким образом: сумма_лонгов = сумма_шортов = batch_size_usd / 2
"""

import asyncio
import random
import time
import traceback
from decimal import Decimal
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

from modules.core.extended_client import ExtendedClient, AccountConfig
from modules.helpers.market_data import MarketDataProvider
from modules.core.logger import setup_logger
from modules.helpers.orderbook_cache import orderbook_cache
from modules.helpers.websocket_manager import ExtendedWebSocketManager
from modules.helpers.market_rules import market_rules
from modules.core.constants import RETRY_SETTINGS, LIMIT_ORDER_CONFIG, WEBSOCKET_CONFIG
from settings import TRADING_SETTINGS, POSITION_MANAGEMENT, DELAYS


def round_to_min_size(amount: Decimal, market: str) -> Decimal:
    """
    Округлить размер позиции до минимального изменения размера для рынка
    Использует правила из market_rules_config.py

    Args:
        amount: Размер позиции
        market: Рынок (например "BTC-USD")

    Returns:
        Округленный размер
    """
    # Используем market_rules для получения корректного min_change_size
    # Убираем суффикс -USD если есть
    clean_market = market.replace('-USD', '')

    # Используем метод из market_rules для правильного округления
    rounded = market_rules.round_size_to_min_change(clean_market, amount)

    # Проверяем, что размер не меньше минимального
    min_size = market_rules.get_min_trade_size(clean_market)
    if min_size and rounded < min_size:
        rounded = min_size

    return rounded


def distribute_amount_randomly(total: Decimal, num_parts: int, variation_range: tuple) -> List[Decimal]:
    """
    Распределить сумму между частями с рандомизацией размеров.

    Размеры будут отличаться друг от друга, но сумма всегда = total.

    Args:
        total: Общая сумма для распределения
        num_parts: Количество частей
        variation_range: Диапазон вариации [min, max], например [0.1, 0.4]

    Returns:
        Список размеров, сумма которых = total

    Пример:
        distribute_amount_randomly(60, 3, (0.1, 0.4))
        → [15.2, 20.8, 24.0] (сумма = 60)
    """
    if num_parts == 1:
        return [total]

    # Генерируем случайные веса с вариацией
    # Базовый вес = 1.0, добавляем случайное отклонение
    min_var, max_var = variation_range
    weights = []

    for _ in range(num_parts):
        # Случайная вариация: от -max_var до +max_var
        variation = random.uniform(-max_var, max_var)
        weight = 1.0 + variation
        # Гарантируем положительный вес (минимум 0.3)
        weight = max(0.3, weight)
        weights.append(weight)

    # Нормализуем веса чтобы сумма = 1
    total_weight = sum(weights)
    normalized_weights = [w / total_weight for w in weights]

    # Распределяем сумму согласно весам
    amounts = [total * Decimal(str(w)) for w in normalized_weights]

    # Корректируем последний элемент для точной суммы (из-за округлений)
    amounts[-1] = total - sum(amounts[:-1])

    return amounts


@dataclass
class AccountBatch:
    """Пачка аккаунтов для торговли"""
    long_accounts: List[AccountConfig]  # Лонг-аккаунты
    short_accounts: List[AccountConfig]  # Шорт-аккаунты
    market: str  # Рынок для торговли
    created_at: datetime

    @property
    def total_accounts(self) -> int:
        return len(self.long_accounts) + len(self.short_accounts)

    @property
    def long_count(self) -> int:
        return len(self.long_accounts)

    @property
    def short_count(self) -> int:
        return len(self.short_accounts)


class BatchTrader:
    """
    Торговля пачками аккаунтов

    Логика работы:
    1. Разделяет аккаунты на пачки (5-7 аккаунтов)
    2. В каждой пачке назначает лонги (1-3) и шорты (остальные)
    3. Одновременно открывает позиции по пачке
    4. Мониторит позиции (TP/SL/время)
    5. Закрывает позиции по условиям
    """

    def __init__(
        self,
        accounts: List[AccountConfig],
        testnet: bool = False,
        logger=None
    ):
        """
        Инициализация трейдера

        Args:
            accounts: Список аккаунтов для торговли
            testnet: Использовать тестнет
            logger: Логгер
        """
        self.accounts = accounts
        self.testnet = testnet
        self.logger = logger or setup_logger()

        # Создаем клиентов для каждого аккаунта
        self.clients: Dict[str, ExtendedClient] = {}
        for account in accounts:
            self.clients[account.name] = ExtendedClient(
                account_config=account,
                testnet=testnet,
                logger=self.logger
            )

        # Провайдер маркет-данных (используем первый доступный прокси)
        proxy = next((acc.proxy for acc in accounts if acc.proxy), None)
        self.market_data = MarketDataProvider(testnet=testnet, proxy=proxy, logger=self.logger)

        # WebSocket Manager для лимитных ордеров (опционально)
        self.ws_manager: Optional[ExtendedWebSocketManager] = None
        if LIMIT_ORDER_CONFIG['websocket_enabled']:
            # Формируем список рынков для WebSocket
            markets = [f"{m}-USD" for m in TRADING_SETTINGS['markets']]
            # Извлекаем уникальные прокси из аккаунтов для WebSocket подключений
            proxies = list(set(acc.proxy for acc in accounts if acc.proxy))
            self.ws_manager = ExtendedWebSocketManager(
                markets=markets,
                testnet=testnet,
                proxies=proxies
            )

        # Активные пачки
        self.active_batches: List[AccountBatch] = []

        # Статистика
        self.stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'total_batches': 0
        }

        # BatchTrader инициализирован (техническая информация)

    async def initialize(self):
        """Инициализировать всех клиентов и WebSocket"""
        # Инициализируем клиентов
        tasks = []
        for client in self.clients.values():
            tasks.append(client.initialize())

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Ошибка инициализации клиентов: {e}")
            raise

        # Запускаем WebSocket Manager для лимитных ордеров (в фоне, без логирования)
        if self.ws_manager:
            asyncio.create_task(self.ws_manager.start())
            await asyncio.sleep(3)

    def create_batches(
        self,
        accounts: Optional[List[AccountConfig]] = None
    ) -> List[AccountBatch]:
        """
        Создать пачки аккаунтов

        Args:
            accounts: Список аккаунтов (если None - используются все)

        Returns:
            Список пачек
        """
        if accounts is None:
            accounts = self.accounts.copy()

        if not accounts:
            self.logger.warning("Нет аккаунтов для создания пачек")
            return []

        batches = []
        remaining = accounts.copy()

        # Случайный выбор рынка для каждой пачки
        markets = TRADING_SETTINGS['markets']

        while remaining:
            # Случайный размер пачки
            min_size, max_size = TRADING_SETTINGS['batch_size_range']
            batch_size = random.randint(
                min_size,
                min(max_size, len(remaining))
            )

            # Берем аккаунты для пачки
            batch_accounts = remaining[:batch_size]
            remaining = remaining[batch_size:]

            # Случайное количество лонгов
            min_longs, max_longs = TRADING_SETTINGS['long_accounts_range']
            long_count = random.randint(
                min_longs,
                min(max_longs, batch_size - 1)  # Должен быть хотя бы 1 шорт
            )

            # Перемешиваем и разделяем
            random.shuffle(batch_accounts)
            longs = batch_accounts[:long_count]
            shorts = batch_accounts[long_count:]

            # Случайный рынок
            market = random.choice(markets)

            batch = AccountBatch(
                long_accounts=longs,
                short_accounts=shorts,
                market=market,
                created_at=datetime.now()
            )

            batches.append(batch)

            self.logger.info(
                f"Создана пачка: {batch.total_accounts} аккаунтов "
                f"({batch.long_count} лонгов, {batch.short_count} шортов) "
                f"на {market}"
            )

        self.stats['total_batches'] += len(batches)
        return batches

    async def trade_batch(self, batch: AccountBatch):
        """
        Торговать одной пачкой аккаунтов

        Args:
            batch: Пачка для торговли
        """
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info(
            f"Начало торговли пачки: {batch.market}, "
            f"{batch.total_accounts} аккаунтов"
        )
        self.logger.info("=" * 60)

        max_batch_retries = TRADING_SETTINGS.get('max_batch_retries', 3)
        market_name = f"{batch.market}-USD"

        for attempt in range(1, max_batch_retries + 1):
            try:
                # Получаем leverage для рынка
                leverage = TRADING_SETTINGS['leverage'].get(
                    batch.market,
                    TRADING_SETTINGS['leverage'].get('BTC', 10)
                )

                # Устанавливаем leverage для всех аккаунтов
                await self._set_leverage_for_batch(batch, leverage)

                # Открываем позиции
                opened_accounts, failed_accounts = await self._open_positions(batch)

                # Проверяем результат открытия
                if failed_accounts:
                    # Не все позиции открылись - несбалансированный батч
                    self.logger.warning(
                        f"⚠️ Батч несбалансирован: открыто {len(opened_accounts)}/{batch.total_accounts} позиций"
                    )

                    if opened_accounts:
                        # Есть открытые позиции - нужно закрыть их
                        self.logger.info(
                            f"Закрытие {len(opened_accounts)} открытых позиций для балансировки..."
                        )

                        # Закрываем открытые позиции
                        close_tasks = []
                        for account in opened_accounts:
                            close_tasks.append(
                                self._close_position_by_account(
                                    account=account,
                                    market=market_name,
                                    reason="несбалансированный батч"
                                )
                            )

                        await asyncio.gather(*close_tasks, return_exceptions=True)

                        self.logger.info(
                            f"✓ Позиции закрыты для балансировки"
                        )

                    if attempt < max_batch_retries:
                        self.logger.info(
                            f"Повторная попытка открытия батча ({attempt}/{max_batch_retries})..."
                        )
                        # Задержка перед retry
                        await asyncio.sleep(DELAYS.get('on_error', 60) / 2)
                        continue
                    else:
                        self.logger.error(
                            f"✗ Не удалось открыть все позиции батча после {max_batch_retries} попыток. "
                            f"Батч пропущен."
                        )
                        return

                # Все позиции открыты успешно - переходим к мониторингу
                self.logger.info(
                    f"✓ Все {len(opened_accounts)} позиций успешно открыты, начинаем мониторинг"
                )

                # Мониторим позиции
                await self._monitor_positions(batch)
                return  # Успешное завершение

            except Exception as e:
                self.logger.error(f"Ошибка торговли пачки: {e}")
                if attempt < max_batch_retries:
                    self.logger.info(f"Повторная попытка ({attempt}/{max_batch_retries})...")
                    await asyncio.sleep(DELAYS.get('on_error', 60) / 2)
                else:
                    raise

    async def _set_leverage_for_batch(self, batch: AccountBatch, leverage: int):
        """Установить leverage для всех аккаунтов пачки"""
        self.logger.info(f"Установка leverage {leverage}x для {batch.market}")

        tasks = []
        all_accounts = batch.long_accounts + batch.short_accounts

        for account in all_accounts:
            client = self.clients[account.name]
            # Добавляем -USD суффикс для API
            market_name = f"{batch.market}-USD"
            tasks.append(client.update_leverage(market_name, leverage))

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            self.logger.warning(f"Ошибка установки leverage: {e}")

    async def _open_positions(self, batch: AccountBatch) -> tuple:
        """
        Открыть позиции для пачки

        Returns:
            tuple: (opened_accounts, failed_accounts) - списки успешно открытых и неуспешных аккаунтов
        """
        market_name = f"{batch.market}-USD"
        order_type = TRADING_SETTINGS.get('order_mode', 'LIMIT')  # LIMIT или MARKET из settings

        # Валидация order_mode
        if order_type not in ['LIMIT', 'MARKET']:
            self.logger.warning(f"Invalid order_mode '{order_type}', using LIMIT")
            order_type = 'LIMIT'

        self.logger.info(
            f"\n{'='*60}\n"
            f"ОТКРЫТИЕ ПОЗИЦИЙ: {batch.market}\n"
            f"{'='*60}"
        )
        self.logger.info(
            f"Аккаунтов в пачке: {batch.total_accounts} "
            f"({batch.long_count} LONG, {batch.short_count} SHORT)"
        )

        # Определяем ОБЩИЙ размер пачки (всех позиций вместе)
        min_size, max_size = TRADING_SETTINGS['batch_size_usd']
        total_batch_size_usd = Decimal(str(random.uniform(min_size, max_size)))

        # Половину делят лонги, половину - шорты (для хеджирования)
        long_total_usd = total_batch_size_usd / Decimal('2')
        short_total_usd = total_batch_size_usd / Decimal('2')

        # Рандомизация размеров (анти-сибил)
        variation_range = tuple(TRADING_SETTINGS.get('order_size_variation', [0.1, 0.4]))

        # Распределяем суммы с вариацией - каждый аккаунт получает разный размер
        long_sizes = distribute_amount_randomly(long_total_usd, len(batch.long_accounts), variation_range)
        short_sizes = distribute_amount_randomly(short_total_usd, len(batch.short_accounts), variation_range)

        self.logger.info("")
        box_width = 50
        self.logger.info("+" + "-" * box_width + "+")
        self.logger.info(f"|  РАЗМЕР ПОЗИЦИЙ{' ' * (box_width - 17)}|")
        self.logger.info("+" + "-" * box_width + "+")
        line1 = f"Общий размер пачки:   $ {total_batch_size_usd:>10.2f}"
        line2 = f"|- Лонги (всего):     $ {long_total_usd:>10.2f}"
        line3 = f"'- Шорты (всего):     $ {short_total_usd:>10.2f}"
        self.logger.info(f"| {line1:<{box_width - 2}}|")
        self.logger.info(f"| {line2:<{box_width - 2}}|")
        self.logger.info(f"| {line3:<{box_width - 2}}|")
        self.logger.info("+" + "-" * box_width + "+")
        # Показываем индивидуальные размеры для каждого аккаунта
        long_sizes_str = ", ".join([f"${s:.2f}" for s in long_sizes])
        short_sizes_str = ", ".join([f"${s:.2f}" for s in short_sizes])
        line4 = f"Лонги ({len(long_sizes)}): {long_sizes_str}"
        line5 = f"Шорты ({len(short_sizes)}): {short_sizes_str}"
        self.logger.info(f"| {line4:<{box_width - 2}}|")
        self.logger.info(f"| {line5:<{box_width - 2}}|")
        self.logger.info("+" + "-" * box_width + "+")
        self.logger.info("")
        self.logger.debug(
            f"Детали: long_accounts={len(batch.long_accounts)}, "
            f"short_accounts={len(batch.short_accounts)}, "
            f"long_sizes={[float(s) for s in long_sizes]}, short_sizes={[float(s) for s in short_sizes]}"
        )

        # Формируем список всех аккаунтов с параметрами
        accounts_to_open = []

        # Лонговые позиции - каждый аккаунт получает свой размер
        for i, account in enumerate(batch.long_accounts):
            accounts_to_open.append({
                'account': account,
                'market': market_name,
                'side': "BUY",
                'size_usd': long_sizes[i],
                'order_type': order_type
            })

        # Шортовые позиции - каждый аккаунт получает свой размер
        for i, account in enumerate(batch.short_accounts):
            accounts_to_open.append({
                'account': account,
                'market': market_name,
                'side': "SELL",
                'size_usd': short_sizes[i],
                'order_type': order_type
            })

        # Запускаем открытие позиций ПАРАЛЛЕЛЬНО с задержкой between_orders
        tasks = []

        for idx, params in enumerate(accounts_to_open):
            # Создаём task для открытия позиции
            task = asyncio.create_task(
                self._open_position(
                    account=params['account'],
                    market=params['market'],
                    side=params['side'],
                    size_usd=params['size_usd'],
                    order_type=params['order_type']
                )
            )
            tasks.append((params['account'], task))

            # Задержка между запуском ордеров (не ждём исполнения)
            if idx < len(accounts_to_open) - 1:
                delay = random.uniform(*DELAYS['between_orders'])
                self.logger.debug(f"Задержка перед следующим ордером: {delay:.1f}s")
                await asyncio.sleep(delay)

        # Теперь ждём завершения всех tasks
        self.logger.info(f"Все {len(tasks)} ордеров запущены, ожидание исполнения...")

        results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)

        # Собираем результаты с привязкой к аккаунтам
        opened_accounts = []
        failed_accounts = []

        for (account, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                error_msg = f"{type(result).__name__}: {str(result)}"
                self.logger.error(f"{account.name} | Ошибка открытия позиции: {error_msg}")
                failed_accounts.append(account)
            else:
                opened_accounts.append(account)

        # Итоговая статистика по открытию
        self.logger.info(
            f"\n{'─'*60}\n"
            f"ИТОГО: открыто {len(opened_accounts)}/{len(tasks)} позиций"
            + (f", неудачно: {len(failed_accounts)}" if failed_accounts else "") +
            f"\n{'─'*60}\n"
        )

        return opened_accounts, failed_accounts

    async def _open_position(
        self,
        account: AccountConfig,
        market: str,
        side: str,
        size_usd: Decimal,
        order_type: str
    ):
        """
        Открыть позицию для одного аккаунта

        Args:
            account: Аккаунт
            market: Рынок (с суффиксом -USD)
            side: Направление (BUY/SELL)
            size_usd: Размер в USD
            order_type: Тип ордера (MARKET/LIMIT)
        """
        client = self.clients[account.name]

        try:
            # Для лимитных ордеров используем retry логику
            if order_type == "LIMIT":
                success = await self._open_position_with_limit_retry(
                    account=account,
                    market=market,
                    side=side,
                    size_usd=size_usd
                )

                if success:
                    self.stats['successful_orders'] += 1
                    self.stats['total_orders'] += 1
                else:
                    self.stats['failed_orders'] += 1
                    self.stats['total_orders'] += 1
                    raise Exception("Failed to open position with limit orders")

                return

            # === MARKET режим: быстрое исполнение ===
            elif order_type == "MARKET":
                # 1. Получить агрессивную цену через MarketDataProvider
                price = await self.market_data.get_market_price_for_order(
                    market=market,
                    side=side,
                    aggressive=True
                )

                # 2. Конвертировать USD → amount
                amount = size_usd / price
                amount = round_to_min_size(amount, market)

                self.logger.info(
                    f"{account.name}: открытие MARKET {side} позиции "
                    f"{market} {amount} (~${size_usd:.2f})"
                )

                # 3. Разместить маркет-ордер (1 попытка, IOC, агрессивная цена)
                order = await client.place_market_order(
                    market=market,
                    side=side,
                    amount=amount,
                    market_data_provider=self.market_data,
                    reduce_only=False
                )

                # 4. КОРОТКАЯ проверка исполнения (~1 секунда, без retry)
                await asyncio.sleep(1.0)  # Даем время на исполнение

                # 5. Проверка позиции (1 попытка через REST API)
                positions = await self.market_data.get_positions_rest(
                    api_key=account.api_key,
                    market=market
                )

                if positions and len(positions) > 0:
                    pos = positions[0]
                    pos_size = pos.get('size', 0)
                    pos_side = pos.get('side', 'UNKNOWN')
                    pos_entry = pos.get('openPrice', 0)
                    pos_value = pos.get('notional', 0)

                    self.logger.info(
                        f"✓ {account.name}: MARKET позиция открыта - "
                        f"{pos_side} {pos_size} @ ${pos_entry} "
                        f"(notional: ${pos_value:.2f})"
                    )
                    self.stats['successful_orders'] += 1
                    self.stats['total_orders'] += 1
                else:
                    # Позиция не появилась - считаем ошибкой
                    self.logger.warning(
                        f"✗ {account.name}: MARKET ордер размещен, но позиция не найдена"
                    )
                    self.stats['failed_orders'] += 1
                    self.stats['total_orders'] += 1
                    raise Exception("Market order placed but position not found")

                return

        except Exception as e:
            self.logger.error(
                f"{account.name}: ошибка открытия позиции: {e}"
            )
            self.stats['failed_orders'] += 1
            self.stats['total_orders'] += 1
            raise

    async def _monitor_positions(self, batch: AccountBatch):
        """
        Мониторинг и закрытие позиций по таймеру

        Позиции закрываются автоматически по истечении времени удержания.
        TP/SL не используются.
        """
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info(f"Начало мониторинга позиций пачки {batch.market}")
        self.logger.info("=" * 60)

        market_name = f"{batch.market}-USD"
        all_accounts = batch.long_accounts + batch.short_accounts

        # Время начала и конца удержания
        start_time = datetime.now()
        min_hold, max_hold = POSITION_MANAGEMENT['holding_time_range']
        hold_duration = random.randint(min_hold, max_hold)
        end_time = start_time + timedelta(seconds=hold_duration)

        self.logger.info(
            f"Время удержания позиций: {hold_duration} сек "
            f"(до {end_time.strftime('%H:%M:%S')})"
        )

        monitor_interval = POSITION_MANAGEMENT['monitor_interval_sec']

        # Список аккаунтов с открытыми позициями
        open_positions = set(acc.name for acc in all_accounts)

        # Счетчик итераций для периодической сводки
        iteration = 0

        self.logger.info(
            f"Мониторинг начат: {len(open_positions)} позиций на {market_name}"
        )

        while open_positions and datetime.now() < end_time:
            try:
                await asyncio.sleep(monitor_interval)
                iteration += 1

                # Вычисляем оставшееся время
                time_left = (end_time - datetime.now()).total_seconds()
                minutes_left = int(time_left // 60)
                seconds_left = int(time_left % 60)

                # Периодическая сводка каждые N итераций
                if iteration % 3 == 0:  # Каждые 3 итерации (примерно каждые 15 сек при интервале 5 сек)
                    self.logger.info(
                        f"Мониторинг: {len(open_positions)} позиций активны, "
                        f"осталось {minutes_left}м {seconds_left}с"
                    )

                # Проверяем каждый аккаунт
                for account_name in list(open_positions):
                    try:
                        account = next(
                            (a for a in all_accounts if a.name == account_name),
                            None
                        )
                        if not account:
                            self.logger.warning(
                                f"Аккаунт {account_name} не найден в списке all_accounts"
                            )
                            continue

                        # Получаем позиции через REST API для более надежной проверки
                        self.logger.debug(
                            f"{account_name}: получение позиций для {market_name} через REST API"
                        )

                        try:
                            positions = await self.market_data.get_positions_rest(
                                api_key=account.api_key,
                                market=market_name
                            )
                        except Exception as e:
                            # Если REST API не работает, пробуем SDK
                            self.logger.debug(
                                f"{account_name}: ошибка REST API ({e}), пробуем SDK"
                            )
                            client = self.clients[account_name]
                            positions = await client.get_positions(market=market_name)

                        if not positions:
                            # Позиция уже закрыта или не была открыта
                            self.logger.info(
                                f"{account_name}: позиция не найдена (уже закрыта или не открывалась)"
                            )
                            open_positions.discard(account_name)
                            continue

                        position = positions[0]  # Должна быть только одна позиция для этого рынка

                        # Проверяем что позиция для правильного рынка
                        pos_market = position.get('market', '')
                        if pos_market != market_name:
                            self.logger.warning(
                                f"{account_name}: позиция для другого рынка ({pos_market} != {market_name})"
                            )
                            open_positions.discard(account_name)
                            continue

                        # Получаем данные позиции
                        side = position.get('side', 'UNKNOWN')
                        size = position.get('size', 0)
                        unrealized_pnl = position.get('unrealisedPnl', 0)
                        mark_price = position.get('markPrice', 0)
                        entry_price = position.get('openPrice', 0)
                        notional = position.get('notional', 0)

                        # Проверяем условия закрытия
                        pnl_pct = self._calculate_pnl_percent(position)

                        # Преобразуем PnL в число для безопасного форматирования
                        try:
                            pnl_value = float(unrealized_pnl) if unrealized_pnl else 0.0
                        except (ValueError, TypeError):
                            pnl_value = 0.0

                        # Логируем состояние позиции на каждой итерации для лучшей видимости
                        self.logger.info(
                            f"{account_name}: {side} {size} @ ${entry_price} "
                            f"(mark: ${mark_price}, notional: ${notional:.2f}) | "
                            f"PnL: {pnl_pct:+.2f}% (${pnl_value:+.2f})"
                        )

                        # TP/SL не используются, закрытие только по таймеру

                    except Exception as e:
                        self.logger.error(
                            f"{account_name}: ошибка мониторинга позиции: {e}\n"
                            f"Traceback:\n{traceback.format_exc()}"
                        )

            except Exception as e:
                self.logger.error(
                    f"Ошибка внешнего цикла мониторинга: {e}\n"
                    f"Traceback:\n{traceback.format_exc()}"
                )

        # Закрываем оставшиеся позиции по истечении времени
        if open_positions:
            self.logger.info(
                f"⏰ Истекло время удержания! Закрытие {len(open_positions)} оставшихся позиций..."
            )

            # Собираем информацию о всех позициях для параллельного закрытия
            positions_to_close = []

            for account_name in list(open_positions):
                account = next(
                    (a for a in all_accounts if a.name == account_name),
                    None
                )
                if not account:
                    continue

                try:
                    # Получаем позицию для отображения финального PnL
                    positions = await self.market_data.get_positions_rest(
                        api_key=account.api_key,
                        market=market_name
                    )

                    if positions:
                        pos = positions[0]
                        pnl_pct = self._calculate_pnl_percent(pos)
                        unrealized_pnl = pos.get('unrealisedPnl', 0)

                        # Безопасное преобразование PnL
                        try:
                            pnl_value = float(unrealized_pnl) if unrealized_pnl else 0.0
                        except (ValueError, TypeError):
                            pnl_value = 0.0

                        self.logger.info(
                            f"🕒 {account_name}: закрытие по времени | "
                            f"финальный PnL: {pnl_pct:+.2f}% (${pnl_value:+.2f})"
                        )

                        # Добавляем в список для закрытия
                        positions_to_close.append({
                            'account_name': account_name,
                            'account': account,
                            'client': self.clients[account_name],
                            'market': market_name,
                            'position': pos
                        })
                    else:
                        self.logger.debug(f"{account_name}: позиция не найдена (уже закрыта)")

                except Exception as e:
                    self.logger.debug(
                        f"{account_name}: не удалось получить позицию: {e}"
                    )
                    # Всё равно пробуем закрыть через SDK
                    try:
                        client = self.clients[account_name]
                        sdk_positions = await client.get_positions(market=market_name)
                        if sdk_positions:
                            positions_to_close.append({
                                'account_name': account_name,
                                'account': account,
                                'client': client,
                                'market': market_name,
                                'position': sdk_positions[0]
                            })
                    except Exception as sdk_e:
                        self.logger.error(f"{account_name}: ошибка получения позиции через SDK: {sdk_e}")

            # Параллельное закрытие всех позиций с задержкой между ордерами
            if positions_to_close:
                self.logger.info(f"Запуск параллельного закрытия {len(positions_to_close)} позиций...")
                await self._close_positions_batch(positions_to_close)
                self.logger.success(f"Закрытие позиций завершено")
            else:
                self.logger.info("Нет позиций для закрытия (все уже закрыты)")
        else:
            self.logger.success(
                f"Все позиции закрыты досрочно (по TP/SL)"
            )

        self.logger.info(
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Мониторинг пачки {batch.market} ЗАВЕРШЕН\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

    def _calculate_pnl_percent(self, position: Dict) -> Decimal:
        """Вычислить PnL в процентах"""
        unrealized_pnl = Decimal(str(position.get('unrealisedPnl', 0)))
        value = Decimal(str(position.get('value', 1)))

        if value == 0:
            return Decimal('0')

        return (unrealized_pnl / value) * Decimal('100')

    async def _close_position(
        self,
        account: AccountConfig,
        market: str,
        position: Dict
    ):
        """Закрыть позицию"""
        client = self.clients[account.name]

        try:
            side = position.get('side', '')
            size = position.get('size', 0)
            entry_price = position.get('openPrice', 0)
            unrealized_pnl = position.get('unrealisedPnl', 0)

            if not side:
                self.logger.error(
                    f"{account.name}: некорректная структура позиции - нет поля 'side'"
                )
                raise ValueError(f"Позиция не содержит поле 'side': {position}")

            if not size or size == 0:
                self.logger.error(
                    f"{account.name}: некорректная структура позиции - size={size}"
                )
                raise ValueError(f"Позиция имеет некорректный size: {size}")

            size_decimal = Decimal(str(size))

            # Противоположное направление для закрытия
            close_side = "SELL" if side == "LONG" else "BUY"

            # Преобразуем unrealized_pnl в число (может быть строкой)
            try:
                pnl_value = float(unrealized_pnl) if unrealized_pnl else 0.0
            except (ValueError, TypeError):
                pnl_value = 0.0

            self.logger.info(
                f"🔄 {account.name}: закрытие {side} {size_decimal} "
                f"(entry: {entry_price}, PnL: ${pnl_value:+.2f})"
            )

            # Выбираем метод закрытия в зависимости от order_mode
            order_type = TRADING_SETTINGS.get('order_mode', 'LIMIT')

            # Валидация order_mode
            if order_type not in ['LIMIT', 'MARKET']:
                self.logger.warning(f"Invalid order_mode '{order_type}', using LIMIT")
                order_type = 'LIMIT'

            # === LIMIT режим: retry + fallback ===
            if order_type == "LIMIT":
                success = await self._close_position_with_limit_retry(
                    account=account,
                    market=market,
                    position=position
                )

                if not success:
                    raise Exception("Failed to close position with limit orders")

            # === MARKET режим: 1 попытка с маркет-ордером ===
            elif order_type == "MARKET":
                success = await self._close_position_with_market(
                    account=account,
                    market=market,
                    position=position
                )

                if not success:
                    raise Exception("Failed to close position with market order")

            return

        except Exception as e:
            self.logger.error(
                f"❌ {account.name}: ошибка закрытия позиции: {e}\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            raise

    async def _close_position_by_account(
        self,
        account: AccountConfig,
        market: str,
        reason: str
    ):
        """Закрыть позицию аккаунта по имени"""
        try:
            self.logger.info(
                f"{account.name}: закрытие позиции по причине: {reason}"
            )

            # Пробуем сначала через REST API
            try:
                positions = await self.market_data.get_positions_rest(
                    api_key=account.api_key,
                    market=market
                )
            except Exception as e:
                # Fallback на SDK
                self.logger.debug(
                    f"{account.name}: ошибка REST API ({e}), используем SDK"
                )
                client = self.clients[account.name]
                positions = await client.get_positions(market=market)

            self.logger.debug(
                f"{account.name}: получено позиций для закрытия: {len(positions) if positions else 0}"
            )

            if not positions:
                self.logger.info(
                    f"{account.name}: позиция не найдена (возможно уже закрыта)"
                )
                return

            position = positions[0]

            # Проверяем что это правильный рынок
            pos_market = position.get('market', '')
            if pos_market != market:
                self.logger.warning(
                    f"{account.name}: позиция для другого рынка ({pos_market} != {market})"
                )
                return

            self.logger.debug(
                f"{account.name}: закрытие позиции - market={pos_market}, "
                f"side={position.get('side')}, size={position.get('size')}"
            )

            await self._close_position(account, market, position)

        except Exception as e:
            self.logger.error(
                f"{account.name}: ошибка закрытия позиции: {e}\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            raise

    async def run_continuous_trading(
        self,
        cycles: Optional[int] = None
    ):
        """
        Непрерывная торговля циклами

        Args:
            cycles: Количество циклов (None = бесконечно)
        """
        self.logger.info(
            f"Запуск непрерывной торговли "
            f"({'бесконечно' if cycles is None else f'{cycles} циклов'})"
        )

        cycle_num = 0

        while cycles is None or cycle_num < cycles:
            cycle_num += 1
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"ЦИКЛ {cycle_num}")
            self.logger.info(f"{'='*50}\n")

            try:
                # Создаем пачки
                batches = self.create_batches()

                if not batches:
                    self.logger.warning("Нет аккаунтов для торговли")
                    break

                # Торгуем каждой пачкой
                for idx, batch in enumerate(batches, 1):
                    self.logger.info(
                        f"\nПачка {idx}/{len(batches)}"
                    )

                    await self.trade_batch(batch)

                    # Задержка между пачками
                    if idx < len(batches):
                        delay = random.uniform(*DELAYS['between_orders'])
                        self.logger.info(
                            f"Задержка перед следующей пачкой: {delay:.1f} сек"
                        )
                        await asyncio.sleep(delay)

                # Статистика цикла
                self.logger.info(f"\nЦикл {cycle_num} завершен")
                self.logger.info(
                    f"Статистика: {self.stats['successful_orders']}/{self.stats['total_orders']} успешных ордеров"
                )

                # Задержка между циклами
                if cycles is None or cycle_num < cycles:
                    delay = random.uniform(*DELAYS['between_orders'])
                    self.logger.info(
                        f"\nОжидание {delay:.1f} сек перед следующим циклом..."
                    )
                    await asyncio.sleep(delay)

            except Exception as e:
                self.logger.error(f"Ошибка в цикле {cycle_num}: {e}")
                await asyncio.sleep(DELAYS['on_error'])

        self.logger.info("\nНепрерывная торговля завершена")
        self.logger.info(f"Финальная статистика: {self.stats}")

    # ============================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ЛИМИТНЫМИ ОРДЕРАМИ (RETRY ЛОГИКА)
    # ============================================================================

    async def _get_orderbook_price(self, market: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Получает лучшие bid и ask цены из orderbook

        Приоритет:
        1. WebSocket кеш (мгновенный доступ)
        2. REST API (fallback)

        Args:
            market: Рынок (BTC-USD)

        Returns:
            Tuple[bid_price, ask_price] или (None, None)
        """
        # ШАГ 1: Пытаемся получить из WebSocket кеша
        if LIMIT_ORDER_CONFIG['websocket_enabled']:
            cached_prices = orderbook_cache.get_prices(
                market,
                max_age_seconds=LIMIT_ORDER_CONFIG['websocket_cache_max_age']
            )

            if cached_prices is not None:
                bid, ask = cached_prices
                self.logger.debug(
                    f"🚀 {market} цены из WebSocket кеша: "
                    f"bid=${bid}, ask=${ask}"
                )
                return bid, ask

        # ШАГ 2: Fallback на REST API
        if LIMIT_ORDER_CONFIG['websocket_fallback_to_rest']:
            self.logger.debug(f"🔄 {market} WebSocket кеш недоступен, используем REST API...")

            try:
                stats = await self.market_data.get_market_stats(market)
                # Используем mark_price как приближение
                # В идеале нужен отдельный метод для получения orderbook через REST
                mid_price = stats.mark_price
                # Приблизительный spread 0.1%
                spread = mid_price * Decimal('0.001')
                bid = mid_price - spread / Decimal('2')
                ask = mid_price + spread / Decimal('2')

                self.logger.debug(
                    f"🔄 {market} цены из REST API: "
                    f"bid=${bid}, ask=${ask} (приблизительно)"
                )
                return bid, ask

            except Exception as e:
                self.logger.error(f"❌ {market}: ошибка получения цен через REST API: {e}")
                return None, None

        return None, None

    async def _open_position_with_limit_retry(
        self,
        account: AccountConfig,
        market: str,
        side: str,
        size_usd: Decimal
    ) -> bool:
        """
        Открывает позицию лимитным ордером с retry логикой

        Args:
            account: Аккаунт
            market: Рынок (BTC-USD)
            side: Направление (BUY/SELL)
            size_usd: Размер в USD

        Returns:
            True если позиция открылась, False если нет
        """
        client = self.clients[account.name]
        max_retries = TRADING_SETTINGS['max_open_retries']
        execution_timeout = TRADING_SETTINGS['order_execution_timeout']

        for attempt in range(max_retries):
            try:
                # Отменяем все предыдущие открытые ордера для этого рынка
                # ВАЖНО: Делаем это перед КАЖДОЙ попыткой, чтобы освободить баланс
                cancelled = await client.cancel_all_orders(
                    market=market,
                    market_data_provider=self.market_data
                )
                if cancelled > 0:
                    # Даём время на обработку отмены
                    await asyncio.sleep(1)

                # Получаем bid/ask из WebSocket кеша или REST API
                bid, ask = await self._get_orderbook_price(market)

                if bid is None or ask is None:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(random.uniform(2, 5))
                    continue

                # Вычисляем цену с адаптивным offset
                static_offset = Decimal(str(TRADING_SETTINGS['limit_order_offset_percent']))

                if TRADING_SETTINGS['use_adaptive_offset']:
                    spread_percent = orderbook_cache.get_spread_percent(market)
                    if spread_percent is not None and spread_percent > 0:
                        # Адаптивный offset = min(static_offset, spread/3)
                        adaptive_offset = min(
                            static_offset,
                            spread_percent / Decimal('100') / Decimal('3')
                        )
                    else:
                        adaptive_offset = static_offset
                else:
                    adaptive_offset = static_offset

                # Рассчитываем цену лимитного ордера
                if side == "BUY":
                    # Покупка НИЖЕ bid (Maker)
                    limit_price = bid * (Decimal('1') - adaptive_offset)
                else:
                    # Продажа ВЫШЕ ask (Maker)
                    limit_price = ask * (Decimal('1') + adaptive_offset)

                # Конвертируем USD в количество базового актива
                amount = size_usd / limit_price
                amount = round_to_min_size(amount, market)

                self.logger.debug(
                    f"{account.name} | Расчет: size_usd=${size_usd}, "
                    f"limit_price=${limit_price}, amount={amount}"
                )

                # Округляем цену для вывода до 2 знаков после запятой
                price_display = float(limit_price)
                market_short = market.replace('-USD', '')

                # Размещаем лимитный ордер
                order = await client.place_limit_order(
                    market=market,
                    side=side,
                    amount=amount,
                    price=limit_price,
                    post_only=False,
                    reduce_only=False
                )

                order_id = order.get('id') or order.get('order_id') or order.get('orderId', 'unknown')
                self.logger.debug(f"{account.name} | Ордер размещен, ID={order_id}")

                # Ждем исполнения ордера
                position_opened = await self._wait_for_order_execution(
                    account=account,
                    market=market,
                    side=side,
                    timeout=execution_timeout
                )

                if position_opened:
                    self.logger.success(
                        f"{account.name} | ✅ Позиция {market} {side} успешно открыта"
                    )
                    return True
                else:
                    self.logger.debug(
                        f"{account.name} | Ордер не исполнился за {execution_timeout}s, "
                        f"отменяем..."
                    )

                    # Отменяем ордер если ID известен
                    if order_id != 'unknown':
                        await client.cancel_order(order_id)

                    # Проверяем не открылась ли позиция во время отмены
                    await asyncio.sleep(2)
                    positions = await client.get_positions(market=market)

                    if positions:
                        # Позиция открылась!
                        self.logger.success(
                            f"{account.name} | ✅ Позиция {market} открылась "
                            f"во время отмены ордера"
                        )
                        return True

                    if attempt < max_retries - 1:
                        self.logger.debug(f"{account.name} | Повторная попытка через 3s...")
                        await asyncio.sleep(3)

            except Exception as e:
                import traceback
                error_msg = f"{type(e).__name__}: {str(e)}"
                self.logger.error(f"{account.name} | Ошибка открытия позиции: {error_msg}")
                self.logger.debug(f"{account.name} | Traceback: {traceback.format_exc()}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 5))

        self.logger.error(
            f"{account.name} | Не удалось открыть позицию {market} "
            f"после {max_retries} попыток"
        )
        return False

    async def _wait_for_order_execution(
        self,
        account: AccountConfig,
        market: str,
        side: str,
        timeout: float
    ) -> bool:
        """
        Ожидает исполнения ордера (проверяет появление позиции)

        Args:
            account: Аккаунт
            market: Рынок
            side: Ожидаемое направление (BUY/SELL)
            timeout: Таймаут в секундах

        Returns:
            True если позиция открылась, False если нет
        """
        client = self.clients[account.name]
        start_time = time.time()
        check_interval = LIMIT_ORDER_CONFIG['check_interval']

        self.logger.debug(
            f"{account.name} | Ожидание исполнения ордера {market} {side} ({timeout}s)"
        )

        while (time.time() - start_time) < timeout:
            try:
                positions = await client.get_positions(market=market)

                if positions:
                    position = positions[0]
                    pos_side = position.get('side', 'UNKNOWN')
                    pos_size = abs(Decimal(str(position.get('size', 0))))

                    if pos_size > Decimal('0.0001'):
                        # Проверяем что направление совпадает
                        if (side == "BUY" and pos_side == "LONG") or \
                           (side == "SELL" and pos_side == "SHORT"):
                            elapsed = time.time() - start_time
                            self.logger.success(
                                f"{account.name} | ✅ Ордер исполнен за {elapsed:.1f}s! "
                                f"Позиция {market} {pos_side} открыта"
                            )
                            return True

                elapsed = time.time() - start_time
                remaining = timeout - elapsed
                self.logger.debug(
                    f"{account.name} | Проверка позиции {market}: не найдена, "
                    f"осталось {remaining:.0f}s"
                )

                await asyncio.sleep(check_interval)

            except Exception as e:
                self.logger.warning(f"{account.name} | Ошибка проверки позиции: {e}")
                await asyncio.sleep(check_interval)

        self.logger.debug(f"{account.name} | Тайм-аут ожидания исполнения ордера {market}")
        return False

    async def _close_position_with_limit_retry(
        self,
        account: AccountConfig,
        market: str,
        position: Dict
    ) -> bool:
        """
        Закрывает позицию лимитным ордером с retry логикой

        Args:
            account: Аккаунт
            market: Рынок
            position: Позиция для закрытия

        Returns:
            True если позиция закрылась, False если нет
        """
        client = self.clients[account.name]
        max_retries = TRADING_SETTINGS['max_close_retries']
        close_timeout = TRADING_SETTINGS['position_close_timeout']

        for attempt in range(max_retries):
            try:
                self.logger.info(
                    f"{account.name} | Попытка {attempt + 1}/{max_retries} "
                    f"закрытия позиции {market}"
                )

                # Отменяем все предыдущие ордера перед каждой попыткой
                cancelled = await client.cancel_all_orders(
                    market=market,
                    market_data_provider=self.market_data
                )
                if cancelled > 0:
                    self.logger.info(
                        f"{account.name} | Отменено {cancelled} старых ордеров перед попыткой {attempt + 1}"
                    )
                    await asyncio.sleep(1)

                # Получаем текущую позицию
                positions = await client.get_positions(market=market)
                if not positions:
                    self.logger.info(f"{account.name} | Позиция {market} уже закрыта")
                    # Отменяем все оставшиеся ордера
                    await client.cancel_all_orders(market=market, market_data_provider=self.market_data)
                    return True

                position = positions[0]
                pos_side = position.get('side', 'UNKNOWN')
                pos_size = abs(Decimal(str(position.get('size', 0))))

                # Получаем bid/ask
                bid, ask = await self._get_orderbook_price(market)
                if bid is None or ask is None:
                    self.logger.warning(f"{account.name} | Не удалось получить цены")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(random.uniform(2, 5))
                    continue

                # Вычисляем цену закрывающего ордера
                static_offset = Decimal(str(TRADING_SETTINGS['limit_order_offset_percent']))

                if TRADING_SETTINGS['use_adaptive_offset']:
                    spread_percent = orderbook_cache.get_spread_percent(market)
                    if spread_percent and spread_percent > 0:
                        adaptive_offset = min(
                            static_offset,
                            spread_percent / Decimal('100') / Decimal('3')
                        )
                    else:
                        adaptive_offset = static_offset
                else:
                    adaptive_offset = static_offset

                # Противоположное направление для закрытия
                if pos_side == "LONG":
                    # Закрываем продажей ВЫШЕ ask
                    close_side = "SELL"
                    limit_price = ask * (Decimal('1') + adaptive_offset)
                else:
                    # Закрываем покупкой НИЖЕ bid
                    close_side = "BUY"
                    limit_price = bid * (Decimal('1') - adaptive_offset)

                self.logger.info(
                    f"{account.name} | Закрытие {pos_side} позиции: "
                    f"{close_side} {pos_size} @ ${limit_price}"
                )

                # Размещаем закрывающий лимитный ордер
                order = await client.place_limit_order(
                    market=market,
                    side=close_side,
                    amount=pos_size,
                    price=limit_price,
                    post_only=False,
                    reduce_only=True
                )

                order_id = order.get('id') or order.get('order_id') or order.get('orderId', 'unknown')
                self.logger.info(f"{account.name} | Ордер на закрытие размещен, ID={order_id}")

                # Ждем закрытия позиции
                position_closed = await self._wait_for_position_close(
                    account=account,
                    market=market,
                    timeout=close_timeout
                )

                if position_closed:
                    self.logger.success(f"{account.name} | ✅ Позиция {market} успешно закрыта")
                    # ВАЖНО: Отменяем все оставшиеся ордера после успешного закрытия
                    cancelled = await client.cancel_all_orders(
                        market=market,
                        market_data_provider=self.market_data
                    )
                    if cancelled > 0:
                        self.logger.info(
                            f"{account.name} | Отменено {cancelled} оставшихся ордеров после закрытия"
                        )
                    return True
                else:
                    self.logger.warning(
                        f"{account.name} | Позиция не закрылась за {close_timeout}s, "
                        f"отменяем ордер..."
                    )

                    # Отменяем ордер если ID известен
                    if order_id != 'unknown':
                        await client.cancel_order(order_id)

                    if attempt < max_retries - 1:
                        self.logger.debug(f"{account.name} | Повторная попытка через 3s...")
                        await asyncio.sleep(3)

            except Exception as e:
                self.logger.error(f"{account.name} | Ошибка закрытия позиции: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 5))

        self.logger.error(
            f"{account.name} | Не удалось закрыть позицию {market} "
            f"после {max_retries} попыток"
        )

        # Fallback: закрываем маркет-ордером
        if LIMIT_ORDER_CONFIG['use_market_fallback']:
            self.logger.warning(f"{account.name} | 🔄 Fallback: закрываем маркет-ордером...")

            try:
                positions = await client.get_positions(market=market)
                if not positions:
                    self.logger.info(f"{account.name} | Позиция уже закрыта")
                    # Отменяем все оставшиеся ордера
                    await client.cancel_all_orders(market=market, market_data_provider=self.market_data)
                    return True

                position = positions[0]
                pos_side = position.get('side', 'UNKNOWN')
                pos_size = abs(Decimal(str(position.get('size', 0))))

                close_side = "SELL" if pos_side == "LONG" else "BUY"

                order = await client.place_market_order(
                    market=market,
                    side=close_side,
                    amount=pos_size,
                    market_data_provider=self.market_data,
                    reduce_only=True
                )

                self.logger.success(f"{account.name} | ✅ Позиция {market} закрыта маркет-ордером")
                # Отменяем все оставшиеся ордера
                await client.cancel_all_orders(market=market, market_data_provider=self.market_data)
                return True

            except Exception as e:
                self.logger.error(f"{account.name} | Ошибка fallback закрытия: {e}")
                return False

        return False

    async def _wait_for_position_close(
        self,
        account: AccountConfig,
        market: str,
        timeout: float
    ) -> bool:
        """
        Ожидает закрытия позиции

        Args:
            account: Аккаунт
            market: Рынок
            timeout: Таймаут в секундах

        Returns:
            True если позиция закрылась, False если нет
        """
        client = self.clients[account.name]
        start_time = time.time()
        check_interval = LIMIT_ORDER_CONFIG['check_interval']

        while (time.time() - start_time) < timeout:
            try:
                positions = await client.get_positions(market=market)

                if not positions:
                    return True

                # Проверяем размер позиции
                position = positions[0]
                pos_size = abs(Decimal(str(position.get('size', 0))))

                if pos_size < Decimal('0.0001'):
                    return True

                await asyncio.sleep(check_interval)

            except Exception as e:
                await asyncio.sleep(check_interval)

        return False

    async def _close_position_with_market(
        self,
        account: AccountConfig,
        market: str,
        position: Dict[str, Any]
    ) -> bool:
        """
        Закрыть позицию маркет-ордером (для MARKET режима)
        Одна попытка, без retry, быстрое исполнение

        Args:
            account: Аккаунт
            market: Рынок (например "BTC-USD")
            position: Информация о позиции

        Returns:
            True если закрыто успешно, False если нет
        """
        client = self.clients[account.name]

        try:
            pos_side = position['side']  # LONG или SHORT
            pos_size = Decimal(str(position['size']))

            # Определяем направление закрытия (противоположное позиции)
            close_side = "SELL" if pos_side == "LONG" else "BUY"

            self.logger.info(
                f"{account.name}: Закрытие {pos_side} позиции маркет-ордером "
                f"{market} {pos_size}"
            )

            # 1. Отменить все старые ордера (чтобы не блокировали баланс)
            try:
                await client.mass_cancel_all_orders()
                await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.debug(f"{account.name}: Ошибка отмены ордеров (игнорируем): {e}")

            # 2. Разместить маркет-ордер на закрытие
            order = await client.place_market_order(
                market=market,
                side=close_side,
                amount=pos_size,
                market_data_provider=self.market_data,
                reduce_only=True  # Только закрытие позиции
            )

            # 3. Короткое ожидание исполнения
            await asyncio.sleep(1.0)

            # 4. Проверка закрытия (1 попытка)
            positions = await self.market_data.get_positions_rest(
                api_key=account.api_key,
                market=market
            )

            # Позиция закрыта если список пуст или size = 0
            if not positions or len(positions) == 0:
                self.logger.info(f"✓ {account.name}: Позиция закрыта (MARKET)")
                # Отменить все оставшиеся ордера
                try:
                    await client.mass_cancel_all_orders()
                except Exception as e:
                    self.logger.debug(f"{account.name}: Ошибка отмены ордеров после закрытия: {e}")
                return True
            else:
                pos_size_after = Decimal(str(positions[0].get('size', 0)))
                if pos_size_after < Decimal('0.0001'):  # По сути 0
                    self.logger.info(f"✓ {account.name}: Позиция закрыта (MARKET)")
                    try:
                        await client.mass_cancel_all_orders()
                    except Exception as e:
                        self.logger.debug(f"{account.name}: Ошибка отмены ордеров после закрытия: {e}")
                    return True
                else:
                    self.logger.warning(
                        f"{account.name}: Маркет-ордер не закрыл позицию полностью "
                        f"(осталось {pos_size_after})"
                    )
                    return False

        except Exception as e:
            self.logger.error(f"{account.name}: Ошибка закрытия MARKET: {e}")
            return False

    # ============================================================================

    async def close_all_positions(self, timeout: float = 60.0):
        """
        Закрыть все открытые позиции по всем аккаунтам с retry-логикой

        Логика:
        1. Получаем все открытые позиции
        2. Отправляем лимитные ордера на закрытие с задержкой between_orders
        3. После max_close_retries попыток - закрываем оставшиеся маркет-ордерами
        4. ПОВТОРЯЕМ поиск позиций и закрытие до тех пор, пока не перестанем находить позиции (макс 5 раундов)
        5. Массовая отмена всех ордеров

        Args:
            timeout: Не используется (оставлен для совместимости)
        """
        self.logger.info("")
        self.logger.info("Закрытие открытых позиций...")

        # Retry-логика для обнаружения всех позиций
        max_detection_rounds = 5  # Максимум 5 раундов поиска позиций
        detection_round = 0

        while detection_round < max_detection_rounds:
            detection_round += 1

            if detection_round > 1:
                self.logger.info("")
                self.logger.info(f"{'='*60}")
                self.logger.info(f"РАУНД {detection_round}: Повторный поиск незакрытых позиций...")
                self.logger.info(f"{'='*60}")
                await asyncio.sleep(3)  # Даём время API обновиться

            # Собираем все аккаунты с открытыми позициями
            all_positions = await self._fetch_all_positions()

            # Если позиций нет - выходим
            if not all_positions:
                if detection_round == 1:
                    self.logger.info("Нет открытых позиций")
                else:
                    self.logger.success(f"✅ Все позиции закрыты после {detection_round-1} раундов!")
                break

            # Закрываем найденные позиции
            await self._close_positions_batch(all_positions)

            # Если это был первый раунд и мы закрыли все успешно - продолжаем искать
            # Если это был не первый раунд - значит нашли упущенные позиции

        # После всех раундов - массовая отмена ордеров
        await self._mass_cancel_all_accounts()

    async def _fetch_all_positions(self) -> list:
        """
        Получить все открытые позиции со всех аккаунтов

        Returns:
            Список словарей с информацией о позициях
        """
        all_positions = []

        async def fetch_account_positions(account: AccountConfig):
            """Получить позиции одного аккаунта"""
            account_name = account.name
            client = self.clients.get(account_name)
            if not client:
                self.logger.debug(f"{account_name}: клиент не найден, пропускаем")
                return []

            positions_list = []
            try:
                # Получаем ВСЕ позиции аккаунта через SDK (надежнее чем REST API)
                self.logger.debug(f"{account_name}: запрос позиций через SDK...")
                positions = await client.get_positions()

                self.logger.debug(
                    f"{account_name}: SDK вернул {len(positions) if positions else 0} позиций, "
                    f"тип: {type(positions)}"
                )

                if positions:
                    for pos in positions:
                        # Проверяем что позиция имеет размер > 0
                        pos_size = pos.get('size', 0)
                        try:
                            pos_size = abs(float(pos_size)) if pos_size else 0
                        except (ValueError, TypeError):
                            pos_size = 0

                        market = pos.get('market', 'UNKNOWN')
                        side = pos.get('side', 'UNKNOWN')

                        if pos_size > 0.0001:
                            self.logger.debug(
                                f"{account_name}: найдена позиция {market} {side} size={pos_size}"
                            )
                            positions_list.append({
                                'account_name': account_name,
                                'account': account,
                                'client': client,
                                'market': market,
                                'position': pos
                            })
                        else:
                            self.logger.debug(
                                f"{account_name}: позиция {market} {side} пропущена (size={pos_size} <= 0.0001)"
                            )

            except Exception as e:
                self.logger.warning(f"{account_name}: REST API ошибка: {e}, пробуем SDK...")
                # Пробуем fallback через SDK для всех рынков из настроек
                try:
                    for market in [f"{m}-USD" for m in TRADING_SETTINGS['markets']]:
                        try:
                            positions = await client.get_positions(market=market)
                            self.logger.debug(
                                f"{account_name}: SDK для {market} вернул {len(positions) if positions else 0} позиций"
                            )
                            if positions:
                                for pos in positions:
                                    pos_size = pos.get('size', 0)
                                    try:
                                        pos_size = abs(float(pos_size)) if pos_size else 0
                                    except (ValueError, TypeError):
                                        pos_size = 0

                                    if pos_size > 0.0001:
                                        self.logger.debug(
                                            f"{account_name}: найдена позиция через SDK {market} size={pos_size}"
                                        )
                                        positions_list.append({
                                            'account_name': account_name,
                                            'account': account,
                                            'client': client,
                                            'market': market,
                                            'position': pos
                                        })
                        except Exception as inner_e:
                            self.logger.debug(f"{account_name}: SDK ошибка для {market}: {inner_e}")
                except Exception:
                    pass

            self.logger.debug(f"{account_name}: итого найдено {len(positions_list)} позиций")
            return positions_list

        # Параллельно запрашиваем позиции со всех аккаунтов
        self.logger.info(f"Проверка позиций на {len(self.accounts)} аккаунтах...")
        self.logger.debug(f"Список аккаунтов: {[acc.name for acc in self.accounts]}")
        fetch_tasks = [fetch_account_positions(acc) for acc in self.accounts]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        # Собираем все позиции
        self.logger.debug(f"Получено {len(results)} результатов от аккаунтов")
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.debug(f"Результат {i}: исключение {type(result).__name__}: {result}")
            elif result:
                self.logger.debug(f"Результат {i}: {len(result)} позиций")
                all_positions.extend(result)
            else:
                self.logger.debug(f"Результат {i}: пустой список")

        # Логируем найденные позиции
        if all_positions:
            self.logger.info(f"Найдено открытых позиций: {len(all_positions)}")
            # Подробный список позиций
            for pos_info in all_positions:
                self.logger.debug(
                    f"  - {pos_info['account_name']}: {pos_info['market']} "
                    f"size={pos_info['position'].get('size', 0)}"
                )

        return all_positions

    async def _close_positions_batch(self, all_positions: list):
        """
        Закрыть пачку позиций с retry-логикой

        Args:
            all_positions: Список словарей с информацией о позициях
        """
        if not all_positions:
            return

        # Подготавливаем данные для закрытия
        positions_to_close = []
        for pos_info in all_positions:
            account_name = pos_info['account_name']
            account = pos_info['account']
            client = pos_info['client']
            market = pos_info['market']
            position = pos_info['position']

            # Определяем направление закрытия (противоположное открытой позиции)
            pos_side = position.get('side', 'UNKNOWN').upper()
            current_size = abs(Decimal(str(position.get('size', '0'))))

            if pos_side == 'LONG':
                close_side = 'SELL'
            elif pos_side == 'SHORT':
                close_side = 'BUY'
            else:
                raw_size = Decimal(str(position.get('size', '0')))
                close_side = 'SELL' if raw_size > 0 else 'BUY'

            positions_to_close.append({
                'account_name': account_name,
                'account': account,
                'client': client,
                'market': market,
                'position': position,
                'close_side': close_side,
                'size': current_size,
                'pos_side': pos_side
            })

        # Получаем настройки
        max_retries = TRADING_SETTINGS['max_close_retries']
        close_timeout = TRADING_SETTINGS['position_close_timeout']
        delay_range = DELAYS.get('between_accounts', [3, 5])  # Задержка между аккаунтами при закрытии
        use_market_fallback = LIMIT_ORDER_CONFIG.get('use_market_fallback', True)
        order_type = TRADING_SETTINGS.get('order_mode', 'LIMIT')  # ИСПРАВЛЕНО: order_mode вместо order_type

        # Словарь для отслеживания статуса закрытия
        close_status = {f"{p['account_name']}:{p['market']}": False for p in positions_to_close}

        # === ЭТАП 1: Закрытие лимитными/маркет ордерами с retry ===
        for attempt in range(max_retries):
            # Фильтруем только незакрытые позиции
            remaining = [p for p in positions_to_close 
                        if not close_status[f"{p['account_name']}:{p['market']}"]]
            
            if not remaining:
                break
                
            if attempt > 0:
                self.logger.info(f"Попытка закрытия {attempt + 1}/{max_retries} (осталось: {len(remaining)})...")

                # Отменяем старые ордера перед каждой попыткой
                cancel_tasks = []
                for pos_info in remaining:
                    cancel_tasks.append(
                        pos_info['client'].cancel_all_orders(
                            market=pos_info['market'],
                            market_data_provider=self.market_data
                        )
                    )
                results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
                cancelled_total = sum(r for r in results if isinstance(r, int))
                if cancelled_total > 0:
                    self.logger.debug(f"Отменено ордеров: {cancelled_total}")
                await asyncio.sleep(1)

            # ЭТАП 1.1: Размещаем ордера последовательно с задержкой
            placed_orders = []  # Список успешно размещенных ордеров
            
            for i, pos_info in enumerate(remaining):

                # Размещаем ордер
                order_info = await self._place_close_order(
                    account_name=pos_info['account_name'],
                    account=pos_info['account'],
                    client=pos_info['client'],
                    market=pos_info['market'],
                    side=pos_info['close_side'],
                    size=pos_info['size'],
                    order_type=order_type
                )
                
                key = f"{pos_info['account_name']}:{pos_info['market']}"
                
                if order_info:
                    # Если позиция уже закрыта - сразу отмечаем успех
                    if order_info.get('already_closed'):
                        close_status[key] = True
                    else:
                        placed_orders.append({
                            'key': key,
                            'account': pos_info['account'],
                            'client': pos_info['client'],
                            'market': pos_info['market'],
                            **order_info
                        })

                # Задержка между размещением ордеров
                if i < len(remaining) - 1:
                    delay = random.uniform(delay_range[0], delay_range[1])
                    await asyncio.sleep(delay)

            # ЭТАП 1.2: Ждем исполнения всех ордеров параллельно
            if placed_orders:
                # Создаем задачи ожидания для всех размещенных ордеров
                wait_tasks = []
                for order_info in placed_orders:
                    wait_task = asyncio.create_task(
                        self._wait_for_position_close(
                            account=order_info['account'],
                            market=order_info['market'],
                            timeout=close_timeout
                        )
                    )
                    wait_tasks.append((order_info['key'], wait_task))
                
                # Собираем результаты параллельно
                for key, task in wait_tasks:
                    try:
                        result = await asyncio.wait_for(task, timeout=close_timeout + 5)
                        if result:
                            close_status[key] = True
                    except asyncio.TimeoutError:
                        task.cancel()
                    except Exception as e:
                        self.logger.debug(f"Ошибка ожидания закрытия {key}: {e}")

            # Проверяем фактическое состояние позиций
            await asyncio.sleep(2)
            for pos_info in remaining:
                key = f"{pos_info['account_name']}:{pos_info['market']}"
                if not close_status[key]:
                    try:
                        positions = await pos_info['client'].get_positions(market=pos_info['market'])
                        if not positions:
                            close_status[key] = True
                            self.logger.debug(f"Позиция {key} закрылась")
                    except Exception:
                        pass

        # === ЭТАП 2: Маркет-ордера для оставшихся позиций ===
        remaining_after_limit = [p for p in positions_to_close 
                                if not close_status[f"{p['account_name']}:{p['market']}"]]
        
        if remaining_after_limit and use_market_fallback:
            self.logger.warning(f"Fallback: закрытие {len(remaining_after_limit)} позиций МАРКЕТ-ордерами...")
            
            # Отменяем все ордера перед маркет-закрытием
            cancel_tasks = []
            for pos_info in remaining_after_limit:
                cancel_tasks.append(
                    pos_info['client'].cancel_all_orders(
                        market=pos_info['market'],
                        market_data_provider=self.market_data
                    )
                )
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            await asyncio.sleep(1)

            # Закрываем маркет-ордерами
            for pos_info in remaining_after_limit:
                key = f"{pos_info['account_name']}:{pos_info['market']}"
                try:
                    # Проверяем актуальную позицию
                    positions = await pos_info['client'].get_positions(market=pos_info['market'])
                    if not positions:
                        close_status[key] = True
                        self.logger.debug(f"{pos_info['account_name']}: {pos_info['market']} уже закрыта")
                        continue
                    
                    position = positions[0]
                    pos_size = abs(Decimal(str(position.get('size', 0))))
                    pos_side_actual = position.get('side', 'UNKNOWN').upper()
                    close_side = "SELL" if pos_side_actual == "LONG" else "BUY"
                    
                    self.logger.debug(
                        f"{pos_info['account_name']}: МАРКЕТ {pos_info['market']} {close_side} {pos_size}"
                    )
                    
                    await pos_info['client'].place_market_order(
                        market=pos_info['market'],
                        side=close_side,
                        amount=pos_size,
                        market_data_provider=self.market_data,
                        reduce_only=True
                    )
                    
                    await asyncio.sleep(2)
                    
                    # Проверяем что закрылась
                    positions = await pos_info['client'].get_positions(market=pos_info['market'])
                    if not positions:
                        close_status[key] = True
                        self.logger.debug(f"{pos_info['account_name']}: {pos_info['market']} закрыта маркетом")
                    else:
                        self.logger.warning(f"{pos_info['account_name']}: {pos_info['market']} НЕ закрылась")
                        
                except Exception as e:
                    self.logger.error(f"{pos_info['account_name']}: ошибка маркет-закрытия {pos_info['market']}: {e}")

        # === ЭТАП 3: Итоги ===
        success_count = sum(1 for v in close_status.values() if v)
        failed_count = len(close_status) - success_count

        self.logger.info("=" * 60)
        self.logger.info(
            f"Закрытие позиций завершено: {success_count} успешно, "
            f"{failed_count} с ошибками"
        )
        self.logger.info("=" * 60)

    async def _place_close_order(
        self,
        account_name: str,
        account: AccountConfig,
        client: ExtendedClient,
        market: str,
        side: str,
        size: Decimal,
        order_type: str = "LIMIT"
    ) -> Optional[Dict]:
        """
        Размещает ордер на закрытие позиции (без ожидания исполнения)
        
        Args:
            account_name: Имя аккаунта
            account: Конфиг аккаунта  
            client: Клиент
            market: Рынок
            side: Сторона закрытия (BUY/SELL)
            size: Размер
            order_type: Тип ордера (LIMIT/MARKET)
            
        Returns:
            Dict с информацией о размещенном ордере или None при ошибке
        """
        try:
            size = round_to_min_size(size, market)
            
            if order_type == "LIMIT":
                # Получаем текущую позицию
                positions = await client.get_positions(market=market)
                if not positions:
                    return {'already_closed': True}
                    
                position = positions[0]
                pos_side = position.get('side', 'UNKNOWN')
                pos_size = abs(Decimal(str(position.get('size', 0))))
                
                # Получаем bid/ask
                bid, ask = await self._get_orderbook_price(market)
                if bid is None or ask is None:
                    return None
                
                # Вычисляем цену
                static_offset = Decimal(str(TRADING_SETTINGS['limit_order_offset_percent']))
                
                if TRADING_SETTINGS['use_adaptive_offset']:
                    spread_percent = orderbook_cache.get_spread_percent(market)
                    if spread_percent and spread_percent > 0:
                        adaptive_offset = min(
                            static_offset,
                            spread_percent / Decimal('100') / Decimal('3')
                        )
                    else:
                        adaptive_offset = static_offset
                else:
                    adaptive_offset = static_offset
                
                # Противоположное направление для закрытия
                if pos_side == "LONG":
                    close_side = "SELL"
                    limit_price = ask * (Decimal('1') + adaptive_offset)
                else:
                    close_side = "BUY"
                    limit_price = bid * (Decimal('1') - adaptive_offset)
                
                # Размещаем закрывающий лимитный ордер
                order = await client.place_limit_order(
                    market=market,
                    side=close_side,
                    amount=pos_size,
                    price=limit_price,
                    post_only=False,
                    reduce_only=True
                )
                
                order_id = order.get('id') or order.get('order_id') or order.get('orderId', 'unknown')
                
                return {
                    'order_id': order_id,
                    'order_type': 'LIMIT',
                    'close_side': close_side,
                    'size': pos_size,
                    'price': limit_price
                }
                    
            else:
                # MARKET ордер
                order = await client.place_market_order(
                    market=market,
                    side=side,
                    amount=size,
                    market_data_provider=self.market_data,
                    reduce_only=True
                )
                
                order_id = order.get('id') or order.get('order_id') or order.get('orderId', 'unknown') if order else 'unknown'
                
                return {
                    'order_id': order_id,
                    'order_type': 'MARKET',
                    'close_side': side,
                    'size': size
                }
                    
        except Exception as e:
            self.logger.error(f"{account_name} | Ошибка размещения ордера закрытия {market}: {e}")
            return None

    async def _close_single_position_one_attempt(
        self,
        account_name: str,
        account: AccountConfig,
        client: ExtendedClient,
        market: str,
        side: str,
        size: Decimal,
        order_type: str = "LIMIT"
    ) -> bool:
        """
        Одна попытка закрытия позиции (без retry внутри)
        
        Args:
            account_name: Имя аккаунта
            account: Конфиг аккаунта
            client: Клиент
            market: Рынок
            side: Сторона закрытия (BUY/SELL)
            size: Размер
            order_type: Тип ордера (LIMIT/MARKET)
            
        Returns:
            True если позиция закрылась, False если нет
        """
        try:
            size = round_to_min_size(size, market)
            close_timeout = TRADING_SETTINGS['position_close_timeout']
            
            if order_type == "LIMIT":
                # Получаем текущую позицию
                positions = await client.get_positions(market=market)
                if not positions:
                    return True  # Уже закрыта
                    
                position = positions[0]
                pos_side = position.get('side', 'UNKNOWN')
                pos_size = abs(Decimal(str(position.get('size', 0))))
                
                # Получаем bid/ask
                bid, ask = await self._get_orderbook_price(market)
                if bid is None or ask is None:
                    self.logger.warning(f"{account_name} | Не удалось получить цены для {market}")
                    return False
                
                # Вычисляем цену
                static_offset = Decimal(str(TRADING_SETTINGS['limit_order_offset_percent']))
                
                if TRADING_SETTINGS['use_adaptive_offset']:
                    spread_percent = orderbook_cache.get_spread_percent(market)
                    if spread_percent and spread_percent > 0:
                        adaptive_offset = min(
                            static_offset,
                            spread_percent / Decimal('100') / Decimal('3')
                        )
                    else:
                        adaptive_offset = static_offset
                else:
                    adaptive_offset = static_offset
                
                # Противоположное направление для закрытия
                if pos_side == "LONG":
                    close_side = "SELL"
                    limit_price = ask * (Decimal('1') + adaptive_offset)
                else:
                    close_side = "BUY"
                    limit_price = bid * (Decimal('1') - adaptive_offset)
                
                self.logger.info(
                    f"{account_name} | Закрытие {pos_side} позиции: "
                    f"{close_side} {pos_size} @ ${limit_price}"
                )
                
                # Размещаем закрывающий лимитный ордер
                order = await client.place_limit_order(
                    market=market,
                    side=close_side,
                    amount=pos_size,
                    price=limit_price,
                    post_only=False,
                    reduce_only=True
                )
                
                order_id = order.get('id') or order.get('order_id') or order.get('orderId', 'unknown')
                self.logger.info(f"{account_name} | Ордер на закрытие размещен, ID={order_id}")
                
                # Ждем закрытия позиции
                self.logger.info(f"{account_name} | Ожидание закрытия позиции {market} ({close_timeout}s)")
                position_closed = await self._wait_for_position_close(
                    account=account,
                    market=market,
                    timeout=close_timeout
                )
                
                if position_closed:
                    return True
                else:
                    self.logger.warning(
                        f"{account_name} | Тайм-аут ожидания закрытия позиции {market}"
                    )
                    return False
                    
            else:
                # MARKET ордер
                self.logger.info(
                    f"{account_name} | Маркет-закрытие {market} {side} {size}"
                )
                
                await client.place_market_order(
                    market=market,
                    side=side,
                    amount=size,
                    market_data_provider=self.market_data,
                    reduce_only=True
                )
                
                await asyncio.sleep(2)
                
                positions = await client.get_positions(market=market)
                if not positions:
                    return True
                else:
                    self.logger.warning(f"{account_name} | Позиция {market} не закрылась маркетом")
                    return False
                    
        except Exception as e:
            self.logger.error(f"{account_name} | Ошибка закрытия {market}: {e}")
            return False

    async def _close_single_position(
        self,
        account_name: str,
        client: ExtendedClient,
        market: str,
        side: str,
        size: Decimal
    ) -> bool:
        """
        Закрыть одну позицию используя тот же тип ордера что и при открытии
        С retry-логикой для надежного закрытия

        Returns:
            True если успешно, False если ошибка
        """
        try:
            # Округляем размер
            size = round_to_min_size(size, market)

            # Закрываем лимитным ордером с retry логикой
            self.logger.info(
                f"{account_name}: закрытие лимитным ордером {market} {side} {size}"
            )

            # Находим аккаунт
            account = None
            for acc in self.accounts:
                if acc.name == account_name:
                    account = acc
                    break

            if not account:
                self.logger.error(f"{account_name}: аккаунт не найден")
                return False

            # Формируем структуру позиции для метода
            position = {
                'side': 'LONG' if side == 'SELL' else 'SHORT',  # Обратная сторона
                'size': float(size),
                'market': market
            }

            # Используем существующий метод с retry логикой (3 попытки)
            success = await self._close_position_with_limit_retry(
                account=account,
                market=market,
                position=position
            )

            if success:
                self.logger.success(
                    f"{account_name}: позиция закрыта на {market}"
                )
                return True
            else:
                self.logger.error(
                    f"{account_name}: не удалось закрыть позицию на {market} после всех попыток"
                )
                return False

        except Exception as e:
            self.logger.error(
                f"{account_name}: ошибка закрытия позиции на {market}: {e}"
            )
            return False

    async def _mass_cancel_all_accounts(self):
        """
        Массовая отмена ВСЕХ ордеров на ВСЕХ аккаунтах

        Использует mass cancel API endpoint для эффективной отмены
        всех ордеров одним запросом на аккаунт.
        """
        self.logger.info("Отмена всех ордеров...")

        try:
            # Запускаем mass cancel параллельно для всех аккаунтов
            cancel_tasks = []
            for account_name, client in self.clients.items():
                cancel_tasks.append(
                    client.mass_cancel_all_orders(market_data_provider=self.market_data)
                )

            # Ждем завершения всех запросов
            results = await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # Подсчитываем результаты
            success_count = sum(1 for r in results if r and not isinstance(r, Exception))
            failed_count = len(results) - success_count

            if failed_count > 0:
                self.logger.warning(f"Mass cancel: {success_count}/{len(self.clients)} OK, {failed_count} ошибок")
            else:
                self.logger.info(f"Mass cancel: {success_count}/{len(self.clients)} аккаунтов OK")

        except Exception as e:
            self.logger.error(f"Ошибка массовой отмены: {e}")

    async def close(self):
        """Закрыть все соединения"""
        self.logger.debug("Закрытие соединений...")

        try:
            # Останавливаем WebSocket Manager
            if self.ws_manager:
                try:
                    await self.ws_manager.stop()
                except Exception as e:
                    self.logger.debug(f"Ошибка закрытия WebSocket Manager: {e}")

            # Закрываем все клиенты параллельно
            tasks = []
            for client in self.clients.values():
                tasks.append(client.close())

            # Закрываем market_data провайдер
            tasks.append(self.market_data.close())

            # Ждем закрытия всех соединений
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Проверяем ошибки
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.debug(f"Ошибка закрытия соединения #{i}: {result}")

            # Даем время на корректное закрытие всех соединений
            await asyncio.sleep(0.5)

            self.logger.debug("Все соединения закрыты")

        except Exception as e:
            self.logger.error(f"Ошибка при закрытии соединений: {e}")
