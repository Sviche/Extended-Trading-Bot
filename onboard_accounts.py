"""
Утилита для массового onboarding аккаунтов Extended

Читает Ethereum приватные ключи из private_keys.txt,
делает автоматический onboarding и сохраняет в БД.

Usage:
    python onboard_accounts.py [--network mainnet|testnet] [--referral CODE]
"""

import asyncio
import argparse
import sys
import warnings
from pathlib import Path

# Подавляем все ResourceWarning'и (unclosed sessions от SDK)
warnings.simplefilter("ignore", ResourceWarning)

# Добавляем модули в путь
sys.path.insert(0, str(Path(__file__).parent))

from modules.helpers.account_onboarding import (
    AccountOnboardingManager,
    OnboardingResult
)
from modules.data.database import DatabaseManager
from modules.core.logger import setup_logger
import logging

# Подавляем asyncio warnings о незакрытых ресурсах
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

logger = setup_logger()


async def onboard_and_save_to_db(
    private_keys_file: str = "user_data/private_keys.txt",
    proxies_file: str = "user_data/proxies.txt",
    network: str = "mainnet",
    referral_code: str = None
):
    """
    Полный цикл onboarding с сохранением в БД.

    Args:
        private_keys_file: Файл с ETH приватными ключами
        proxies_file: Файл с прокси
        network: Сеть (mainnet/testnet)
        referral_code: Реферальный код (не используется, берется из конфига)
    """

    # 1. Читаем приватные ключи
    logger.info(f"Читаем приватные ключи из {private_keys_file}...")

    if not Path(private_keys_file).exists():
        logger.error(f"Файл {private_keys_file} не найден!")
        return

    with open(private_keys_file, 'r', encoding='utf-8') as f:
        eth_keys = [line.strip() for line in f if line.strip()]

    if not eth_keys:
        logger.error("Не найдено приватных ключей в файле!")
        return

    logger.success(f"Найдено {len(eth_keys)} приватных ключей")

    # 2. Читаем прокси
    proxies = None
    if Path(proxies_file).exists():
        logger.info(f"Читаем прокси из {proxies_file}...")
        with open(proxies_file, 'r', encoding='utf-8') as f:
            proxies = [line.strip() for line in f if line.strip()]
        logger.success(f"Найдено {len(proxies)} прокси")

        if len(proxies) < len(eth_keys):
            logger.warning(
                f"Прокси ({len(proxies)}) меньше чем ключей ({len(eth_keys)}). "
                f"Добавьте больше прокси или часть аккаунтов будет без прокси."
            )
        elif len(proxies) > len(eth_keys):
            logger.info(
                f"Прокси ({len(proxies)}) больше чем ключей ({len(eth_keys)}). "
                f"Будут использованы первые {len(eth_keys)} прокси."
            )
            proxies = proxies[:len(eth_keys)]  # Обрезаем до нужного количества
    else:
        logger.warning(f"Файл {proxies_file} не найден, прокси не используются")

    # 3. Создаем менеджер onboarding
    logger.info(f"Инициализация onboarding для сети: {network}")
    # Реф код берется автоматически из market_rules_config
    manager = AccountOnboardingManager(
        network=network
    )

    # 4. Делаем onboarding
    logger.info("")
    logger.info("=" * 70)
    logger.info("Начинаем массовый onboarding аккаунтов...")
    logger.info("=" * 70)

    results = await manager.onboard_multiple_accounts(
        eth_private_keys=eth_keys,
        proxies=proxies,
        delay_between_accounts=2.0  # 2 секунды между аккаунтами
    )

    if not results:
        logger.error("Ни один аккаунт не был успешно зарегистрирован!")
        return

    logger.success(f"\nУспешно зарегистрировано {len(results)} аккаунтов")

    # 5. Сохраняем в БД
    logger.info("\nСохранение аккаунтов в БД...")

    db = DatabaseManager()

    saved_count = 0
    for idx, result in enumerate(results, 1):
        try:
            # Генерируем имя аккаунта
            account_name = f"Account_{idx:03d}"

            # Получаем прокси для этого аккаунта
            proxy = proxies[idx - 1] if proxies else None

            # Сохраняем в БД
            db.save_account(
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

            saved_count += 1
            logger.info(
                f"  [{idx}/{len(results)}] {account_name} → "
                f"Account ID: {result.account_id}, Vault: {result.vault_id}"
            )

        except Exception as e:
            # Более детальная информация об ошибке
            error_msg = str(e)
            if "UNIQUE constraint" in error_msg:
                logger.warning(
                    f"Аккаунт {account_name} уже существует в БД "
                    f"(Account ID: {result.account_id}). Пропускаем."
                )
            else:
                logger.error(f"Ошибка сохранения аккаунта {account_name}: {e}")
            continue

    logger.info(f"\n✓ Сохранено {saved_count} аккаунтов в БД")

    # 6. Показываем статистику
    logger.info("\n" + "=" * 70)
    logger.info("СТАТИСТИКА ONBOARDING")
    logger.info("=" * 70)
    logger.info(f"Всего ключей:              {len(eth_keys)}")
    logger.info(f"Успешно зарегистрировано:  {len(results)}")
    logger.info(f"Сохранено в БД:            {saved_count}")
    logger.info(f"Неудачных:                 {len(eth_keys) - len(results)}")
    logger.info("=" * 70)

    # 7. Показываем список аккаунтов
    logger.info("\nЗарегистрированные аккаунты:")
    for idx, result in enumerate(results, 1):
        logger.info(
            f"  [{idx}] {result.eth_address[:10]}... → "
            f"Extended Account #{result.account_id} (Vault: {result.vault_id})"
        )

    logger.info(
        f"\n✓ Onboarding завершен! Все данные сохранены в database/extended_bot.db"
    )


async def check_accounts_balance():
    """
    Проверка балансов зарегистрированных аккаунтов.
    """
    from modules.helpers.account_onboarding import AccountOnboardingManager

    logger.info("Проверка балансов аккаунтов...")

    db = DatabaseManager()
    accounts = db.get_all_accounts()

    if not accounts:
        logger.warning("В БД нет аккаунтов для проверки")
        return

    logger.info(f"Найдено {len(accounts)} аккаунтов в БД\n")

    # Для каждого аккаунта создаем trading client и проверяем баланс
    from modules.helpers.account_onboarding import OnboardingResult

    for acc in accounts:
        try:
            # Создаем OnboardingResult из данных БД
            result = OnboardingResult(
                eth_address=acc['eth_address'],
                eth_private_key=acc['eth_private_key'],
                stark_public_key=acc['stark_public_key'],
                stark_private_key=acc['stark_private_key'],
                vault_id=acc['vault_id'],
                account_id=acc['account_id'],
                api_key=acc['api_key'],
                onboarded_at=acc['onboarded_at'],
                network=acc['network']
            )

            # Создаем менеджер для нужной сети
            manager = AccountOnboardingManager(network=acc['network'])

            # Создаем trading client
            client = manager.create_trading_client(result)

            # Получаем баланс
            balance_info = await client.get_collateral_balance()

            logger.info(
                f"[{acc['name']}] Account #{acc['account_id']}: "
                f"Balance = ${balance_info.get('total_balance', 0):.2f} "
                f"(Available: ${balance_info.get('available_balance', 0):.2f})"
            )

        except Exception as e:
            logger.error(f"Ошибка проверки баланса для {acc['name']}: {e}")
            continue


def main():
    """Entry point"""

    parser = argparse.ArgumentParser(
        description="Массовый onboarding аккаунтов Extended"
    )

    parser.add_argument(
        '--network',
        choices=['mainnet', 'testnet'],
        default='mainnet',
        help='Сеть для onboarding (по умолчанию: mainnet)'
    )

    parser.add_argument(
        '--referral',
        type=str,
        default=None,
        help='Реферальный код (опционально)'
    )

    parser.add_argument(
        '--private-keys',
        type=str,
        default='user_data/private_keys.txt',
        help='Файл с приватными ключами (по умолчанию: user_data/private_keys.txt)'
    )

    parser.add_argument(
        '--proxies',
        type=str,
        default='user_data/proxies.txt',
        help='Файл с прокси (по умолчанию: user_data/proxies.txt)'
    )

    parser.add_argument(
        '--check-balance',
        action='store_true',
        help='Проверить балансы уже зарегистрированных аккаунтов'
    )

    args = parser.parse_args()

    try:
        if args.check_balance:
            # Проверяем балансы
            asyncio.run(check_accounts_balance())
        else:
            # Делаем onboarding
            asyncio.run(onboard_and_save_to_db(
                private_keys_file=args.private_keys,
                proxies_file=args.proxies,
                network=args.network,
                referral_code=args.referral
            ))

    except KeyboardInterrupt:
        logger.warning("\n\nПрервано пользователем (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nКритическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
