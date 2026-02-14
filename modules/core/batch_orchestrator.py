"""
Batch Orchestrator - центральная система управления торговлей

Архитектура:
┌─────────────────────────────────────────────────────────┐
│ Account Pool (100 аккаунтов)                            │
│ [Available: 60] [In Trade: 25] [Cooldown: 15]          │
└─────────────────────────────────────────────────────────┘
                     ↓
         ┌───────────────────────┐
         │  Batch Generator      │ ← Создает задачи каждые N сек
         └───────────────────────┘
                     ↓
         ┌───────────────────────┐
         │   Task Queue          │ ← Буфер задач
         │  [Task1, Task2, ...]  │
         └───────────────────────┘
                     ↓
         ┌─────────────────────────────┐
         │  Worker Pool (3-5 workers)  │ ← Параллельная обработка
         │  W1   W2   W3   W4   W5     │
         └─────────────────────────────┘
                     ↓
         [Open → Monitor → Close → Release to Pool]

Преимущества:
- Динамические батчи (каждый раз новые комбинации)
- Контролируемая нагрузка (N воркеров)
- Автоматическая балансировка
- Graceful shutdown
"""

import asyncio
import time
import random
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from modules.core.logger import setup_logger
from modules.core.account_pool import AccountPool, BalancedAccountPool

# Инициализация логгера
logger = setup_logger("BatchOrchestrator")


@dataclass
class TradingTask:
    """Задача на открытие позиций"""
    task_id: str
    account_ids: List[str]
    market: str
    created_at: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)

    def __repr__(self):
        return f"Task({self.task_id}, {len(self.account_ids)} accounts, {self.market})"


@dataclass
class TaskResult:
    """Результат выполнения задачи"""
    task: TradingTask
    success: bool
    positions_opened: int = 0
    positions_closed: int = 0
    pnl: float = 0.0
    execution_time: float = 0.0
    error: Optional[str] = None


class BatchGenerator:
    """
    Генератор батчей из пула аккаунтов

    Постоянно мониторит пул и создает новые задачи когда:
    - Достаточно свободных аккаунтов
    - Очередь не переполнена
    """

    def __init__(self, account_pool: AccountPool, task_queue: asyncio.Queue,
                 markets: List[str], batch_size_range: tuple = (5, 7),
                 generation_interval: float = 5.0, max_queue_size: int = 10):
        """
        Args:
            account_pool: Пул аккаунтов
            task_queue: Очередь для задач
            markets: Список доступных рынков
            batch_size_range: Диапазон размера батча (min, max)
            generation_interval: Интервал проверки (секунды)
            max_queue_size: Максимальный размер очереди
        """
        self.pool = account_pool
        self.queue = task_queue
        self.markets = markets
        self.batch_size_range = batch_size_range
        self.generation_interval = generation_interval
        self.max_queue_size = max_queue_size

        self.running = False
        self.tasks_generated = 0

    async def run(self):
        """Основной цикл генерации"""
        self.running = True
        # BatchGenerator запущен (техническая информация)

        while self.running:
            try:
                await self._generate_batch()
            except Exception as e:
                logger.error(f"Error in batch generation: {e}", exc_info=True)

            await asyncio.sleep(self.generation_interval)

    async def _generate_batch(self):
        """Попытка создать новый батч"""

        # Проверить размер очереди
        if self.queue.qsize() >= self.max_queue_size:
            logger.debug(f"Queue is full ({self.queue.qsize()}/{self.max_queue_size}), skipping generation")
            return

        # Случайный размер батча
        batch_size = random.randint(*self.batch_size_range)

        # Попытка получить аккаунты
        account_ids = self.pool.get_random_batch(
            size=batch_size,
            min_size=self.batch_size_range[0]  # Минимум 5 аккаунтов
        )

        if not account_ids:
            stats = self.pool.get_pool_stats()
            logger.debug(
                f"Not enough accounts for batch (need: {batch_size}, "
                f"available: {stats['available']}, in_trade: {stats['in_trade']}, "
                f"cooldown: {stats['cooldown']})"
            )
            return

        # Случайный рынок
        market = random.choice(self.markets)

        # Создать задачу
        task = TradingTask(
            task_id=f"task_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            account_ids=account_ids,
            market=market
        )

        # Добавить в очередь
        await self.queue.put(task)
        self.tasks_generated += 1
        # Генерация задач без лишнего логирования

    def stop(self):
        """Остановить генератор"""
        self.running = False
        # BatchGenerator остановлен


# Глобальный счетчик батчей (thread-safe через asyncio)
_batch_counter = 0
_batch_counter_lock = asyncio.Lock()


async def get_next_batch_number() -> int:
    """Получить следующий номер батча (потокобезопасно)"""
    global _batch_counter
    async with _batch_counter_lock:
        _batch_counter += 1
        return _batch_counter


class TradingWorker:
    """
    Воркер для обработки торговых задач

    Берет задачи из очереди и выполняет:
    1. Открытие позиций (BatchTrader)
    2. Мониторинг позиций
    3. Закрытие позиций
    4. Возврат аккаунтов в пул
    """

    def __init__(self, worker_id: int, task_queue: asyncio.Queue,
                 account_pool: AccountPool, batch_trader,
                 cooldown_after_trade: int = 60):
        """
        Args:
            worker_id: ID воркера
            task_queue: Очередь задач
            account_pool: Пул аккаунтов
            batch_trader: Instance BatchTrader для торговли
            cooldown_after_trade: Cooldown после сделки (секунды)
        """
        self.worker_id = worker_id
        self.queue = task_queue
        self.pool = account_pool
        self.trader = batch_trader
        self.cooldown = cooldown_after_trade

        self.running = False
        self.tasks_processed = 0
        self.tasks_successful = 0
        self.tasks_failed = 0

    async def run(self):
        """Основной цикл обработки задач"""
        self.running = True
        # Worker запущен (техническая информация)

        while self.running:
            try:
                # Взять задачу из очереди (ждет если пусто)
                task = await asyncio.wait_for(self.queue.get(), timeout=1.0)

                # Обработать задачу
                result = await self._process_task(task)

                # Обновить статистику
                self.tasks_processed += 1
                if result.success:
                    self.tasks_successful += 1
                else:
                    self.tasks_failed += 1

                # Отметить задачу как выполненную
                self.queue.task_done()

            except asyncio.TimeoutError:
                # Нет задач, продолжить ожидание
                continue
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}", exc_info=True)

    async def _process_task(self, task: TradingTask) -> TaskResult:
        """Обработать задачу"""
        start_time = time.time()
        result = None  # Инициализируем result заранее

        try:
            # Выполнить торговлю через BatchTrader
            result = await self._execute_trading(task)
            execution_time = time.time() - start_time
            return result

        except asyncio.CancelledError:
            # Обработка Ctrl+C - вернуть аккаунты немедленно
            self.pool.release_immediately(task.account_ids)
            raise  # Пробросить дальше для graceful shutdown

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Worker {self.worker_id} failed {task} after {execution_time:.1f}s: {e}",
                exc_info=True
            )

            # Вернуть аккаунты немедленно при ошибке
            self.pool.release_immediately(task.account_ids)

            result = TaskResult(
                task=task,
                success=False,
                execution_time=execution_time,
                error=str(e)
            )
            return result

        finally:
            # Вернуть аккаунты в пул с cooldown (только если result был создан)
            if result and isinstance(self.pool, BalancedAccountPool):
                # Сообщить о результате для балансировки
                for acc_id in task.account_ids:
                    if result.success:
                        self.pool.report_success(acc_id)
                    else:
                        self.pool.report_error(acc_id, Exception(result.error or "Unknown error"))

    async def _execute_trading(self, task: TradingTask) -> TaskResult:
        """
        Выполнить полный цикл торговли

        Это интерфейс между оркестратором и BatchTrader

        Использует существующие методы BatchTrader:
        - _set_leverage_for_batch
        - _open_positions
        - _monitor_positions
        """
        from modules.core.batch_trader import AccountBatch
        from datetime import datetime

        positions_opened = 0
        positions_closed = 0
        pnl = 0.0

        # Получить объекты AccountConfig из account_ids
        accounts = []
        for acc_id in task.account_ids:
            # Найти аккаунт в списке всех аккаунтов трейдера
            found = None
            for acc in self.trader.accounts:
                # Проверяем разные поля: account_id, name, id
                acc_identifier = str(getattr(acc, 'account_id', None) or getattr(acc, 'name', None) or getattr(acc, 'id', None))
                if acc_identifier == acc_id or acc.name == acc_id:
                    found = acc
                    break

            if found:
                accounts.append(found)
            else:
                logger.warning(f"Account not found for ID: {acc_id}")

        if not accounts:
            raise Exception(f"No accounts found for IDs: {task.account_ids}")

        # Разделить аккаунты на лонги и шорты
        # Случайное количество лонгов (1-3, но не больше половины)
        from settings import TRADING_SETTINGS
        min_longs, max_longs = TRADING_SETTINGS['long_accounts_range']
        long_count = random.randint(
            min_longs,
            min(max_longs, len(accounts) - 1)  # Минимум 1 шорт
        )

        # Перемешиваем и разделяем
        random.shuffle(accounts)
        longs = accounts[:long_count]
        shorts = accounts[long_count:]

        # Получить номер батча
        batch_number = await get_next_batch_number()

        # Создать AccountBatch
        batch = AccountBatch(
            long_accounts=longs,
            short_accounts=shorts,
            market=task.market.replace('-USD', ''),  # Убираем суффикс для batch
            created_at=datetime.now(),
            batch_number=batch_number
        )

        # Выполнить торговлю через BatchTrader
        try:
            # Получаем leverage для рынка (поддержка [min, max, step] и фиксированного)
            from settings import TRADING_SETTINGS
            leverage_config = TRADING_SETTINGS['leverage'].get(
                batch.market,
                TRADING_SETTINGS['leverage'].get('BTC', 10)
            )

            # 1. Установить leverage (рандомизация происходит внутри)
            await self.trader._set_leverage_for_batch(batch, leverage_config)
            await self.trader._open_positions(batch)
            positions_opened = batch.total_accounts

            # 2. Установить нативные стоплоссы (TPSL POSITION) — ВРЕМЕННО ОТКЛЮЧЕНО
            # from settings import POSITION_MANAGEMENT
            # sl_enabled = POSITION_MANAGEMENT.get('stop_loss_enabled', False)
            # logger.info(f"🛡️ SL enabled: {sl_enabled}")
            # if sl_enabled:
            #     try:
            #         await self.trader._place_native_stop_losses(batch)
            #     except Exception as e:
            #         logger.error(f"❌ Ошибка установки нативных SL: {type(e).__name__}: {e}")
            #         import traceback
            #         logger.error(f"Traceback: {traceback.format_exc()}")

            # 3. Мониторить позиции (включает закрытие)
            await self.trader._monitor_positions(batch)
            positions_closed = batch.total_accounts

            # TODO: Собрать реальный PnL из результатов мониторинга
            # Пока используем случайное значение
            pnl = random.uniform(-50, 100)

        except Exception as e:
            logger.error(f"Error during trading execution: {e}", exc_info=True)
            raise

        finally:
            # Вернуть аккаунты в пул с cooldown
            self.pool.release_batch(task.account_ids, cooldown_override=self.cooldown)

        return TaskResult(
            task=task,
            success=True,
            positions_opened=positions_opened,
            positions_closed=positions_closed,
            pnl=pnl,
            execution_time=0  # Будет вычислено воркером
        )

    def stop(self):
        """Остановить воркер"""
        self.running = False
        # Worker остановлен

    def get_stats(self) -> Dict:
        """Получить статистику воркера"""
        return {
            'worker_id': self.worker_id,
            'tasks_processed': self.tasks_processed,
            'tasks_successful': self.tasks_successful,
            'tasks_failed': self.tasks_failed,
            'success_rate': round(
                self.tasks_successful / self.tasks_processed * 100, 1
            ) if self.tasks_processed > 0 else 0
        }


class BatchOrchestrator:
    """
    Центральный оркестратор торговли

    Управляет:
    - Пулом аккаунтов
    - Генератором батчей
    - Воркерами
    - Очередью задач
    """

    def __init__(self, accounts: List[Dict], markets: List[str],
                 batch_trader, config: Dict):
        """
        Args:
            accounts: Список аккаунтов из БД
            markets: Доступные рынки
            batch_trader: Instance BatchTrader
            config: Конфигурация из settings.py
        """
        self.config = config

        # Создать пул аккаунтов
        use_balanced = config.get('use_balanced_pool', True)
        if use_balanced:
            self.account_pool = BalancedAccountPool(
                accounts=accounts,
                cooldown_seconds=config.get('account_cooldown_seconds', 60),
                max_consecutive_errors=config.get('max_consecutive_errors', 3)
            )
        else:
            self.account_pool = AccountPool(
                accounts=accounts,
                cooldown_seconds=config.get('account_cooldown_seconds', 60)
            )

        # Создать очередь задач
        self.task_queue = asyncio.Queue(maxsize=config.get('max_queue_size', 50))

        # Создать генератор
        self.generator = BatchGenerator(
            account_pool=self.account_pool,
            task_queue=self.task_queue,
            markets=markets,
            batch_size_range=config.get('batch_size_range', (5, 7)),
            generation_interval=config.get('generation_interval', 5.0),
            max_queue_size=config.get('max_queue_size', 10)
        )

        # Создать воркеры
        num_workers = config.get('num_workers', 3)
        self.workers = [
            TradingWorker(
                worker_id=i,
                task_queue=self.task_queue,
                account_pool=self.account_pool,
                batch_trader=batch_trader,
                cooldown_after_trade=config.get('account_cooldown_seconds', 60)
            )
            for i in range(num_workers)
        ]

        self.running = False
        self.stats_task = None

        # Orchestrator инициализирован (техническая информация)

    async def run(self):
        """Запустить оркестратор"""
        self.running = True

        # Запустить все компоненты
        tasks = []

        # Генератор
        tasks.append(asyncio.create_task(self.generator.run()))

        # Воркеры
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.run()))

        # Статистика (каждые 30 секунд)
        self.stats_task = asyncio.create_task(self._stats_loop())
        tasks.append(self.stats_task)

        try:
            # Ждать завершения всех задач
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass  # Обработка отмены без лишних логов
        finally:
            await self.shutdown()

    async def shutdown(self, close_positions: bool = True):
        """
        Graceful shutdown с закрытием позиций

        Args:
            close_positions: Закрывать ли все открытые позиции перед выходом
        """
        if not self.running:
            return

        logger.info("=" * 60)
        logger.info("НАЧАЛО GRACEFUL SHUTDOWN")
        logger.info("=" * 60)
        self.running = False

        # Остановить генератор (не создавать новые задачи)
        self.generator.stop()
        logger.info("Генератор остановлен, новые задачи не создаются")

        # Дождаться завершения текущих задач в очереди (timeout 30s)
        logger.info(f"Ожидание завершения текущих задач (очередь: {self.task_queue.qsize()})...")
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=30.0)
            logger.info("Все задачи из очереди завершены")
        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout ожидания очереди, принудительная остановка "
                f"(осталось задач: {self.task_queue.qsize()})"
            )

        # Остановить воркеров
        for worker in self.workers:
            worker.stop()
        logger.info("Воркеры остановлены")

        # Отменить статистику
        if self.stats_task:
            self.stats_task.cancel()
            try:
                await self.stats_task
            except asyncio.CancelledError:
                pass

        # Закрыть все открытые позиции перед выходом
        if close_positions:
            logger.info("")
            logger.info("Закрытие всех открытых позиций...")
            try:
                # Получаем batch_trader из воркеров
                if self.workers and hasattr(self.workers[0], 'trader'):
                    trader = self.workers[0].trader
                    # close_all_positions уже включает массовую отмену ордеров
                    await trader.close_all_positions(timeout=60.0)
                else:
                    logger.warning("Не найден BatchTrader для закрытия позиций")
            except Exception as e:
                logger.error(f"Ошибка при закрытии позиций: {e}", exc_info=True)
        else:
            # Даже если не закрываем позиции, отменяем все висящие ордера
            logger.info("")
            logger.info("Отмена всех висящих ордеров...")
            try:
                if self.workers and hasattr(self.workers[0], 'trader'):
                    trader = self.workers[0].trader
                    await trader._mass_cancel_all_accounts()
            except Exception as e:
                logger.error(f"Ошибка при отмене ордеров: {e}", exc_info=True)

        # Вывести финальную статистику
        logger.info("")
        self._print_stats()

        logger.info("")
        logger.info("=" * 60)
        logger.info("AUTO TRADING SHUTDOWN COMPLETE")
        logger.info("=" * 60)

    async def _stats_loop(self):
        """Периодический вывод статистики"""
        from settings import LOG_SETTINGS
        stats_interval = LOG_SETTINGS.get('stats_interval_sec', 300)
        while self.running:
            await asyncio.sleep(stats_interval)
            self._print_stats()

    def _print_stats(self):
        """Вывести статистику"""
        # Статистика пула
        pool_stats = self.account_pool.get_pool_stats()

        # Статистика воркеров
        worker_stats = [w.get_stats() for w in self.workers]
        total_processed = sum(w['tasks_processed'] for w in worker_stats)
        total_successful = sum(w['tasks_successful'] for w in worker_stats)
        total_failed = sum(w['tasks_failed'] for w in worker_stats)

        logger.info("=" * 60)
        logger.info("AUTO TRADING STATS")
        logger.info("=" * 60)
        logger.info(f"Pool: {pool_stats['available']} available, {pool_stats['in_trade']} trading, "
                   f"{pool_stats['cooldown']} cooldown (utilization: {pool_stats['utilization']}%)")
        logger.info(f"Queue: {self.task_queue.qsize()} tasks pending")
        logger.info(f"Generator: {self.generator.tasks_generated} tasks generated")
        logger.info(f"Workers: {total_processed} processed ({total_successful} success, {total_failed} failed)")

        for stats in worker_stats:
            logger.info(
                f"  Worker {stats['worker_id']}: {stats['tasks_processed']} tasks "
                f"({stats['success_rate']}% success)"
            )
        logger.info("=" * 60)
