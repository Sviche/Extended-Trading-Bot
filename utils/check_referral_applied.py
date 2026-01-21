"""
Проверка - применен ли реферальный код к аккаунту
"""

import asyncio
import aiohttp
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from modules.core.logger import setup_logger
from modules.data.database import DatabaseManager

logger = setup_logger()


async def check_referral_for_account():
    """Проверяем различные эндпоинты для информации о реферале"""

    db = DatabaseManager()
    accounts = db.get_all_accounts()

    if not accounts:
        logger.error("Нет аккаунтов в БД!")
        return

    # Берем последний аккаунт (свежий)
    acc = accounts[-1]

    logger.info("=" * 80)
    logger.info("ПРОВЕРКА РЕФЕРАЛЬНОГО СТАТУСА")
    logger.info("=" * 80)
    logger.info(f"Account: {acc['name']}")
    logger.info(f"ETH Address: {acc['eth_address']}")
    logger.info(f"Account ID: {acc['account_id']}")
    logger.info("=" * 80)

    api_key = acc['api_key']
    base_url = "https://api.starknet.extended.exchange"

    headers = {
        "X-Api-Key": api_key,
        "User-Agent": "Extended-Bot-Test/1.0",
        "Accept": "application/json"
    }

    # Список эндпоинтов для проверки
    endpoints = [
        "/api/v1/user/account/info",
        "/api/v1/user/accounts",
        "/api/v1/user/referrals/status",
        "/api/v1/user/referrals",
        "/api/v1/user/referral",  # возможно это правильный
        "/api/v1/user/account/referral",  # или этот
        "/api/v1/user/referrer",  # или этот
        "/api/v1/user/info",
        "/api/v1/user/profile",
        "/api/v1/user/settings",
        "/api/v1/user/details",
    ]

    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            logger.info(f"\n[Testing] {endpoint}")

            try:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    status = resp.status
                    text = await resp.text()

                    logger.info(f"  Status: {status}")

                    if status == 200:
                        logger.info(f"  ✓ Response: {text[:500]}")

                        # Проверяем есть ли в ответе упоминание реферала
                        if 'referral' in text.lower() or 'referred' in text.lower() or 'sviche' in text.lower():
                            logger.info(f"  🎯 FOUND REFERRAL INFO!")
                    else:
                        logger.info(f"  Response: {text[:200]}")

            except Exception as e:
                logger.error(f"  Error: {e}")

            await asyncio.sleep(0.5)

    logger.info("\n" + "=" * 80)
    logger.info("ПРОВЕРКА ЗАВЕРШЕНА")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(check_referral_for_account())
