"""
Account Pool - управление пулом аккаунтов для динамического формирования батчей

Этот модуль отвечает за:
- Отслеживание статусов аккаунтов (available/in_trade/cooldown)
- Случайный выбор аккаунтов для батчей
- Автоматический возврат аккаунтов в пул после cooldown
- Предотвращение использования одних и тех же аккаунтов одновременно
"""

import time
import random
import asyncio
from typing import List, Set, Dict, Optional
from dataclasses import dataclass
from modules.core.logger import setup_logger

# Инициализация логгера
logger = setup_logger("AccountPool")


@dataclass
class AccountStatus:
    """Статус аккаунта в пуле"""
    account_id: str
    status: str  # 'available', 'in_trade', 'cooldown', 'disabled'
    release_time: Optional[float] = None  # Время когда аккаунт станет доступен
    trades_count: int = 0  # Количество сделок
    last_trade_time: Optional[float] = None


class AccountPool:
    """
    Пул аккаунтов для динамического формирования батчей

    Принцип работы:
    1. Все аккаунты изначально available
    2. При создании батча аккаунты переходят в in_trade
    3. После закрытия позиций аккаунты переходят в cooldown
    4. После cooldown аккаунты возвращаются в available

    Это гарантирует:
    - Каждый батч имеет уникальную комбинацию аккаунтов
    - Аккаунты отдыхают между сделками
    - Равномерное распределение нагрузки
    """

    def __init__(self, accounts: List[Dict], cooldown_seconds: int = 60):
        """
        Args:
            accounts: Список аккаунтов из БД (Dict с полями id, name и т.д.)
            cooldown_seconds: Время отдыха после сделки (секунды)
        """
        self.cooldown_seconds = cooldown_seconds

        # Создать статусы для всех аккаунтов
        self.statuses: Dict[str, AccountStatus] = {}
        for acc in accounts:
            # Поддерживаем разные форматы: Dict из БД или AccountConfig
            if isinstance(acc, dict):
                # Dict из БД
                account_id = str(acc.get('id') or acc.get('account_id') or acc.get('name'))
            else:
                # AccountConfig объект
                account_id = str(getattr(acc, 'account_id', None) or getattr(acc, 'name', None))

            self.statuses[account_id] = AccountStatus(
                account_id=account_id,
                status='available'
            )

        # AccountPool инициализирован (техническая информация)

    def get_available_count(self) -> int:
        """Получить количество доступных аккаунтов"""
        self._update_cooldowns()
        return sum(1 for s in self.statuses.values() if s.status == 'available')

    def get_in_trade_count(self) -> int:
        """Получить количество аккаунтов в позициях"""
        return sum(1 for s in self.statuses.values() if s.status == 'in_trade')

    def get_cooldown_count(self) -> int:
        """Получить количество аккаунтов на cooldown"""
        self._update_cooldowns()
        return sum(1 for s in self.statuses.values() if s.status == 'cooldown')

    def get_pool_stats(self) -> Dict:
        """Получить статистику пула"""
        self._update_cooldowns()

        total = len(self.statuses)
        available = self.get_available_count()
        in_trade = self.get_in_trade_count()
        cooldown = self.get_cooldown_count()
        disabled = sum(1 for s in self.statuses.values() if s.status == 'disabled')

        return {
            'total': total,
            'available': available,
            'in_trade': in_trade,
            'cooldown': cooldown,
            'disabled': disabled,
            'utilization': round((in_trade + cooldown) / total * 100, 1) if total > 0 else 0
        }

    def get_random_batch(self, size: int, min_size: Optional[int] = None) -> Optional[List[str]]:
        """
        Получить случайный батч из доступных аккаунтов

        Args:
            size: Желаемый размер батча
            min_size: Минимальный размер (если None, то = size)

        Returns:
            Список account_id или None если недостаточно аккаунтов
        """
        self._update_cooldowns()

        # Получить доступные аккаунты
        available = [
            acc_id for acc_id, status in self.statuses.items()
            if status.status == 'available'
        ]

        # Проверить достаточно ли аккаунтов
        min_size = min_size or size
        if len(available) < min_size:
            logger.debug(
                f"Not enough available accounts: {len(available)} < {min_size} "
                f"(available: {self.get_available_count()}, in_trade: {self.get_in_trade_count()}, "
                f"cooldown: {self.get_cooldown_count()})"
            )
            return None

        # Выбрать случайные аккаунты
        batch_size = min(size, len(available))
        batch = random.sample(available, batch_size)

        # Пометить как in_trade
        for acc_id in batch:
            self.statuses[acc_id].status = 'in_trade'
            self.statuses[acc_id].last_trade_time = time.time()

        logger.debug(f"Created batch with {len(batch)} accounts: {batch}")

        return batch

    def release_batch(self, account_ids: List[str], cooldown_override: Optional[int] = None):
        """
        Вернуть аккаунты в пул после завершения торговли

        Args:
            account_ids: Список account_id для возврата
            cooldown_override: Переопределить время cooldown (секунды)
        """
        cooldown = cooldown_override if cooldown_override is not None else self.cooldown_seconds
        release_time = time.time() + cooldown

        for acc_id in account_ids:
            if acc_id not in self.statuses:
                logger.warning(f"Unknown account ID: {acc_id}")
                continue

            status = self.statuses[acc_id]
            status.status = 'cooldown'
            status.release_time = release_time
            status.trades_count += 1

        logger.debug(
            f"Released {len(account_ids)} accounts to cooldown ({cooldown}s): {account_ids}"
        )

    def release_immediately(self, account_ids: List[str]):
        """Вернуть аккаунты немедленно (без cooldown)"""
        for acc_id in account_ids:
            if acc_id not in self.statuses:
                continue

            self.statuses[acc_id].status = 'available'
            self.statuses[acc_id].release_time = None

        logger.debug(f"Released {len(account_ids)} accounts immediately: {account_ids}")

    def disable_account(self, account_id: str, reason: str = ""):
        """Отключить аккаунт (например, при ошибке)"""
        if account_id not in self.statuses:
            return

        self.statuses[account_id].status = 'disabled'
        logger.warning(f"Disabled account {account_id}: {reason}")

    def enable_account(self, account_id: str):
        """Включить отключенный аккаунт"""
        if account_id not in self.statuses:
            return

        self.statuses[account_id].status = 'available'
        self.statuses[account_id].release_time = None
        logger.info(f"Enabled account {account_id}")

    def _update_cooldowns(self):
        """Обновить статусы аккаунтов на cooldown"""
        now = time.time()
        released = []

        for acc_id, status in self.statuses.items():
            if status.status == 'cooldown' and status.release_time:
                if now >= status.release_time:
                    status.status = 'available'
                    status.release_time = None
                    released.append(acc_id)

        if released:
            logger.debug(f"Released {len(released)} accounts from cooldown: {released}")

    def get_account_status(self, account_id: str) -> Optional[AccountStatus]:
        """Получить статус конкретного аккаунта"""
        return self.statuses.get(account_id)

    def get_least_used_accounts(self, count: int) -> List[str]:
        """Получить наименее используемые аккаунты (для балансировки)"""
        self._update_cooldowns()

        available = [
            (acc_id, status) for acc_id, status in self.statuses.items()
            if status.status == 'available'
        ]

        # Сортировать по количеству сделок
        sorted_accounts = sorted(available, key=lambda x: x[1].trades_count)

        return [acc_id for acc_id, _ in sorted_accounts[:count]]

    async def wait_for_available_accounts(self, min_count: int, timeout: float = 60.0) -> bool:
        """
        Ждать пока появится минимум аккаунтов

        Args:
            min_count: Минимальное количество доступных аккаунтов
            timeout: Максимальное время ожидания (секунды)

        Returns:
            True если аккаунты появились, False если timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.get_available_count() >= min_count:
                return True

            await asyncio.sleep(1.0)

        return False


class BalancedAccountPool(AccountPool):
    """
    Улучшенный пул с балансировкой нагрузки

    Особенности:
    - Приоритет наименее используемым аккаунтам
    - Автоматическое отключение проблемных аккаунтов
    - Статистика использования
    """

    def __init__(self, accounts: List[Dict], cooldown_seconds: int = 60,
                 max_consecutive_errors: int = 3):
        super().__init__(accounts, cooldown_seconds)
        self.max_consecutive_errors = max_consecutive_errors
        self.consecutive_errors: Dict[str, int] = {acc_id: 0 for acc_id in self.statuses}

    def get_random_batch(self, size: int, min_size: Optional[int] = None,
                        balanced: bool = True) -> Optional[List[str]]:
        """
        Получить батч с опциональной балансировкой

        Args:
            size: Желаемый размер
            min_size: Минимальный размер
            balanced: Использовать балансировку (приоритет наименее используемым)
        """
        if not balanced:
            return super().get_random_batch(size, min_size)

        self._update_cooldowns()

        # Получить доступные аккаунты, отсортированные по использованию
        available = [
            (acc_id, status) for acc_id, status in self.statuses.items()
            if status.status == 'available'
        ]

        min_size = min_size or size
        if len(available) < min_size:
            return None

        # Сортировать по количеству сделок (наименьшие первыми)
        sorted_accounts = sorted(available, key=lambda x: x[1].trades_count)

        # Взять первые N наименее используемых + немного случайности
        batch_size = min(size, len(sorted_accounts))
        candidates = sorted_accounts[:batch_size * 2] if len(sorted_accounts) > batch_size else sorted_accounts
        batch = random.sample([acc_id for acc_id, _ in candidates], min(batch_size, len(candidates)))

        # Пометить как in_trade
        for acc_id in batch:
            self.statuses[acc_id].status = 'in_trade'
            self.statuses[acc_id].last_trade_time = time.time()

        logger.debug(f"Created balanced batch with {len(batch)} accounts: {batch}")

        return batch

    def report_error(self, account_id: str, error: Exception):
        """Сообщить об ошибке аккаунта"""
        if account_id not in self.consecutive_errors:
            return

        self.consecutive_errors[account_id] += 1

        if self.consecutive_errors[account_id] >= self.max_consecutive_errors:
            self.disable_account(account_id, f"Too many errors: {error}")

    def report_success(self, account_id: str):
        """Сообщить об успешной сделке"""
        if account_id in self.consecutive_errors:
            self.consecutive_errors[account_id] = 0
