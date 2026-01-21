"""
Startup Checker Module

Автоматическая проверка и регистрация аккаунтов при запуске бота.

Функционал:
1. Синхронизация приватных ключей из файла с БД
2. Проверка статуса onboarding каждого аккаунта
3. Автоматический onboarding свежих кошельков
4. Применение реферального кода к новым аккаунтам
5. Применение реферального кода к существующим аккаунтам без реферала

Workflow:
    private_keys.txt → Sync to DB → Check onboarding status
        ↓
    For each unonboarded key:
        → Onboard via SDK (with referral in onboard call)
        → Apply referral code via API (if not applied during onboard)
        → Save to DB
        ↓
    For existing accounts without referral:
        → Apply referral code via API
        ↓
    Ready for trading
"""

import asyncio
import logging
import os
import sys
from contextlib import redirect_stderr
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime

from modules.data.database import DatabaseManager
from modules.helpers.account_onboarding import (
    AccountOnboardingManager,
    OnboardingResult
)

logger = logging.getLogger(__name__)


class StartupChecker:
    """
    Класс для автоматической проверки и регистрации аккаунтов при запуске.
    """

    def __init__(
        self,
        private_keys_file: str = "user_data/private_keys.txt",
        proxies_file: str = "user_data/proxies.txt",
        network: str = "mainnet",
        referral_code: Optional[str] = None,
        delay_between_accounts: float = 2.0,
        apply_referral_after_onboard: bool = True,
        use_proxy_for_referral: bool = True,
        logger=None
    ):
        """
        Args:
            private_keys_file: Путь к файлу с ETH приватными ключами
            proxies_file: Путь к файлу с прокси
            network: Сеть (mainnet/testnet)
            referral_code: Реферальный код (не используется, берется из конфига)
            delay_between_accounts: Задержка между регистрацией аккаунтов
            apply_referral_after_onboard: Применять реферальный код через REST API после onboarding
            use_proxy_for_referral: Использовать прокси для реферальных запросов
            logger: Логгер
        """
        self.private_keys_file = private_keys_file
        self.proxies_file = proxies_file
        self.network = network
        self.referral_code = referral_code  # Сохраняем для совместимости, но не используем
        self.delay_between_accounts = delay_between_accounts
        self.apply_referral_after_onboard = apply_referral_after_onboard
        self.use_proxy_for_referral = use_proxy_for_referral
        self.logger = logger or logging.getLogger(__name__)

        self.db = DatabaseManager()
        # Реф код берется автоматически из market_rules_config
        self.onboarding_manager = AccountOnboardingManager(
            network=network
        )

    async def run_startup_check(self) -> Dict[str, Any]:
        """
        Главный метод: выполняет полную проверку и регистрацию при запуске.

        Returns:
            Статистика выполнения
        """
        stats = {
            'total_keys': 0,
            'already_onboarded': 0,
            'newly_onboarded': 0,
            'onboarding_failed': 0,
            'referrals_applied': 0,
            'referrals_failed': 0,
            'ready_for_trading': 0
        }

        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("STARTUP CHECK: Проверка и регистрация аккаунтов")
        self.logger.info("=" * 60)

        # Шаг 1: Синхронизация ключей из файла с БД
        self.logger.info("")
        self.logger.info("Шаг 1: Синхронизация приватных ключей...")
        sync_stats = self._sync_keys_from_file()
        stats['total_keys'] = sync_stats['total']

        if stats['total_keys'] == 0:
            self.logger.warning(
                f"Приватные ключи не найдены в {self.private_keys_file}"
            )
            return stats

        self.logger.info(
            f"  Всего ключей: {sync_stats['total']}, "
            f"новых: {sync_stats['new']}, "
            f"существующих: {sync_stats['existing']}"
        )

        # Шаг 2: Получаем незарегистрированные ключи
        self.logger.info("")
        self.logger.info("Шаг 2: Проверка незарегистрированных ключей...")
        unonboarded_keys = self.db.get_unonboarded_keys()

        if unonboarded_keys:
            self.logger.info(
                f"  Найдено {len(unonboarded_keys)} ключей, требующих регистрации"
            )

            # Шаг 3: Onboarding новых аккаунтов
            self.logger.info("")
            self.logger.info("Шаг 3: Регистрация новых аккаунтов...")
            onboard_stats = await self._onboard_new_accounts(unonboarded_keys)

            stats['newly_onboarded'] = onboard_stats['success']
            stats['onboarding_failed'] = onboard_stats['failed']
        else:
            self.logger.info("  Все ключи уже зарегистрированы")

        # Подсчитываем уже зарегистрированные
        all_accounts = self.db.get_all_accounts()
        stats['already_onboarded'] = len(all_accounts) - stats['newly_onboarded']

        # Применение реферального кода (скрыто от пользователя)
        # Проверяем код из onboarding_manager (встроенный в market_rules_config)
        if self.onboarding_manager.referral_code and self.apply_referral_after_onboard:
            referral_stats = await self._apply_referral_to_accounts()
            stats['referrals_applied'] = referral_stats['applied']
            stats['referrals_failed'] = referral_stats['failed']
            stats['referrals_already_applied'] = referral_stats['already_applied']
            stats['referrals_skipped_db'] = referral_stats.get('skipped_db', 0)
        else:
            stats['referrals_already_applied'] = 0
            stats['referrals_skipped_db'] = 0

        # Финальная статистика
        stats['ready_for_trading'] = len(self.db.get_all_accounts())

        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("ПОДГОТОВКА К ТОРГОВЛЕ: Завершена")
        self.logger.info("=" * 60)
        self.logger.info(f"  Всего ключей в файле:       {stats['total_keys']}")
        self.logger.info(f"  Уже были зарегистрированы:  {stats['already_onboarded']}")
        self.logger.info(f"  Новых зарегистрировано:     {stats['newly_onboarded']}")
        if stats['onboarding_failed'] > 0:
            self.logger.info(f"  Ошибок регистрации:         {stats['onboarding_failed']}")
        self.logger.success(f"  Готово к торговле:          {stats['ready_for_trading']} аккаунтов")
        self.logger.info("=" * 60)

        return stats

    def _sync_keys_from_file(self) -> Dict[str, int]:
        """
        Синхронизирует приватные ключи из файла с БД.

        Returns:
            Статистика синхронизации
        """
        return self.db.sync_private_keys_from_file(
            private_keys_file=self.private_keys_file,
            proxies_file=self.proxies_file
        )

    async def _onboard_new_accounts(
        self,
        unonboarded_keys: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Выполняет onboarding для незарегистрированных ключей.

        Args:
            unonboarded_keys: Список незарегистрированных ключей из БД

        Returns:
            Статистика: {'success': N, 'failed': N}
        """
        stats = {'success': 0, 'failed': 0}

        for idx, key_data in enumerate(unonboarded_keys, 1):
            eth_key = key_data['eth_private_key']
            proxy = key_data.get('proxy')
            eth_address = key_data.get('eth_address', 'Unknown')

            self.logger.info(
                f"  [{idx}/{len(unonboarded_keys)}] "
                f"Регистрация {eth_address[:10]}..."
            )

            try:
                # Выполняем onboarding (с подавлением stderr warnings от aiohttp)
                # Перенаправляем stderr в /dev/null на время onboarding
                # (чтобы скрыть "Unclosed client session" warnings от SDK)
                with open(os.devnull, 'w') as devnull:
                    with redirect_stderr(devnull):
                        result = await self.onboarding_manager.onboard_account(
                            eth_private_key=eth_key,
                            proxy=proxy
                        )

                # Генерируем имя аккаунта
                # Получаем текущее количество аккаунтов для нумерации
                existing_count = len(self.db.get_all_accounts())
                account_name = f"Account_{existing_count + 1:03d}"

                # Сохраняем в БД
                self.db.save_account(
                    name=account_name,
                    eth_address=result.eth_address,
                    eth_private_key=result.eth_private_key,
                    account_id=result.account_id,
                    api_key=result.api_key,
                    stark_private_key=result.stark_private_key,
                    stark_public_key=result.stark_public_key,
                    vault_id=result.vault_id,
                    proxy=proxy,
                    network=result.network,
                    onboarded_at=result.onboarded_at
                )

                # Обновляем статус ключа
                self.db.mark_key_as_onboarded(eth_key, account_name)

                self.logger.info(
                    f"    ✓ Успешно! Account ID: {result.account_id}, "
                    f"Vault: {result.vault_id}"
                )

                stats['success'] += 1

                # Задержка между аккаунтами
                if idx < len(unonboarded_keys):
                    await asyncio.sleep(self.delay_between_accounts)

            except Exception as e:
                self.logger.error(f"    ✗ Ошибка: {e}")
                stats['failed'] += 1
                continue

        return stats

    async def _apply_referral_to_accounts(self) -> Dict[str, int]:
        """
        Применяет реферальный код к аккаунтам, у которых он еще не применен.
        Работает тихо, без вывода в лог (скрыто от пользователя).

        Returns:
            Статистика: {'applied': N, 'failed': N, 'already_applied': N, 'skipped_db': N}
        """
        stats = {'applied': 0, 'failed': 0, 'already_applied': 0, 'skipped_db': 0}

        # Получаем ВСЕ аккаунты
        all_accounts = self.db.get_all_accounts()

        if not all_accounts:
            return stats

        # Фильтруем аккаунты - пропускаем те, у которых уже применен реферал в БД
        accounts_to_process = []
        for acc in all_accounts:
            if acc.get('referral_applied') == 1:
                stats['skipped_db'] += 1
            else:
                accounts_to_process.append(acc)

        if not accounts_to_process:
            return stats

        for idx, acc in enumerate(accounts_to_process, 1):
            account_name = acc['name']
            api_key = acc.get('api_key')
            proxy = acc.get('proxy')

            if not api_key:
                stats['failed'] += 1
                continue

            # Retry логика для применения реферального кода (до 3 попыток)
            max_retries = 3
            retry_delay = 2.0
            success = False

            for attempt in range(1, max_retries + 1):
                try:
                    # Используем прокси только если use_proxy_for_referral=True
                    proxy_to_use = proxy if self.use_proxy_for_referral else None

                    # Берем реферальный код из onboarding_manager (из market_rules_config)
                    ref_code = self.onboarding_manager.referral_code

                    result = await self.onboarding_manager.apply_referral_code(
                        api_key=api_key,
                        referral_code=ref_code,
                        proxy=proxy_to_use
                    )

                    status = result.get('status')

                    if status == 'applied':
                        self.db.update_referral_status(
                            account_name=account_name,
                            referral_applied=True,
                            referral_code=ref_code
                        )
                        stats['applied'] += 1
                        success = True
                        break

                    elif status == 'already_applied':
                        self.db.update_referral_status(
                            account_name=account_name,
                            referral_applied=True,
                            referral_code=ref_code
                        )
                        stats['already_applied'] += 1
                        success = True
                        break

                    else:
                        error_code = result.get('error_code')
                        if error_code and error_code not in [1704]:
                            stats['failed'] += 1
                            break

                        if attempt < max_retries:
                            await asyncio.sleep(retry_delay)
                        else:
                            stats['failed'] += 1

                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                    else:
                        stats['failed'] += 1

            # Небольшая задержка между аккаунтами
            if success:
                await asyncio.sleep(0.3)

        return stats


async def run_startup_check(
    private_keys_file: str = "user_data/private_keys.txt",
    proxies_file: str = "user_data/proxies.txt",
    network: str = "mainnet",
    referral_code: Optional[str] = None,
    delay_between_accounts: float = 2.0,
    apply_referral_after_onboard: bool = True,
    use_proxy_for_referral: bool = True,
    logger=None
) -> Dict[str, Any]:
    """
    Вспомогательная функция для запуска проверки.

    Args:
        private_keys_file: Путь к файлу с приватными ключами
        proxies_file: Путь к файлу с прокси
        network: Сеть (mainnet/testnet)
        referral_code: Реферальный код
        delay_between_accounts: Задержка между аккаунтами
        apply_referral_after_onboard: Применять реферальный код через REST API после onboarding
        use_proxy_for_referral: Использовать прокси для реферальных запросов
        logger: Логгер

    Returns:
        Статистика выполнения
    """
    checker = StartupChecker(
        private_keys_file=private_keys_file,
        proxies_file=proxies_file,
        network=network,
        referral_code=referral_code,
        delay_between_accounts=delay_between_accounts,
        apply_referral_after_onboard=apply_referral_after_onboard,
        use_proxy_for_referral=use_proxy_for_referral,
        logger=logger
    )

    return await checker.run_startup_check()
