"""
Extended Bot - Main Entry Point
"""

import sys
import asyncio
import warnings
import signal
from utils import logo

# Установка UTF-8 для Windows консоли (ДО импорта логгера!)
if sys.platform == 'win32':
    try:
        import io
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding='utf-8',
            errors='replace',
            line_buffering=True
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer,
            encoding='utf-8',
            errors='replace',
            line_buffering=True
        )
    except Exception:
        pass  # Если не удалось, продолжаем с обычным выводом

# Подавляем предупреждения aiohttp о незакрытых сессиях
# ВАЖНО: используем regex с .* для надежного перехвата
warnings.filterwarnings('ignore', message='.*Unclosed client session.*')
warnings.filterwarnings('ignore', message='.*Unclosed connector.*')
warnings.filterwarnings('ignore', message='.*unclosed.*')
warnings.filterwarnings('ignore', category=ResourceWarning, message='.*unclosed.*')
warnings.filterwarnings('ignore', category=ResourceWarning)

# Подавляем все ResourceWarning связанные с aiohttp
import logging
logging.getLogger('aiohttp').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.ERROR)

# ============================================================
# КРИТИЧНО: Установка SDK proxy patch ДО всех импортов SDK
# ============================================================
from modules.helpers.sdk_proxy_patch import install_sdk_proxy_patch

# Устанавливаем патч один раз при старте приложения
_SDK_PATCH_SUCCESS = install_sdk_proxy_patch()

# ============================================================
# Импорты модулей бота
# ============================================================
from modules.core.logger import setup_logger
from modules.helpers.account_manager import AccountManager
from modules.helpers.startup_checker import StartupChecker
from modules.core.batch_trader import BatchTrader
from modules.core.constants import *
from settings import *


async def main():
    """Main function to run the Extended Bot"""
    # Показываем красивый логотип
    logo.print_logo()

    # Даём время пользователю увидеть логотип, TG канал и QR код
    await asyncio.sleep(2.5)

    logger = setup_logger()

    logger.info("=" * 60)
    logger.info("Extended Bot v0.1 - Starting")
    logger.info("=" * 60)

    # Проверяем установку SDK proxy patch
    if not _SDK_PATCH_SUCCESS:
        logger.error(
            "Не удалось установить SDK proxy patch!\n"
            "Прокси НЕ будут работать. Установите aiohttp-socks:\n"
            "  pip install aiohttp-socks\n"
            "Или используйте VPN для доступа к Extended API."
        )
        # Не останавливаем бота - можно работать через VPN
    else:
        logger.debug("SDK proxy patch установлен успешно")

    # Переменная для хранения оркестратора (нужна для shutdown в signal handler)
    orchestrator = None
    trader = None

    # Обработчик сигнала Ctrl+C
    def signal_handler(sig, frame):
        """Обработчик SIGINT (Ctrl+C)"""
        logger.info("\n\n")
        logger.info("=" * 60)
        logger.info("Получен сигнал остановки (Ctrl+C)")
        logger.info("=" * 60)
        # Отменяем все задачи для graceful shutdown
        for task in asyncio.all_tasks():
            task.cancel()

    # Регистрируем обработчик сигнала
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # === Автоматическая проверка и регистрация аккаунтов ===
        if ONBOARDING_CONFIG.get('auto_onboard_enabled', True):
            await asyncio.sleep(0.5)
            logger.info("")
            logger.info("Шаг 0: Автоматическая проверка и регистрация аккаунтов...")

            startup_checker = StartupChecker(
                private_keys_file=str(PRIVATE_KEYS_FILE),
                proxies_file=str(PROXIES_FILE),
                network=ONBOARDING_CONFIG.get('network', 'mainnet'),
                referral_code=ONBOARDING_CONFIG.get('referral_code'),
                delay_between_accounts=ONBOARDING_CONFIG.get('delay_between_accounts', 2.0),
                apply_referral_after_onboard=ONBOARDING_CONFIG.get('apply_referral_after_onboard', True),
                use_proxy_for_referral=ONBOARDING_CONFIG.get('use_proxy_for_referral', True),
                logger=logger
            )

            startup_stats = await startup_checker.run_startup_check()

            # Проверяем, есть ли аккаунты для торговли
            if startup_stats['ready_for_trading'] == 0:
                logger.error(
                    "Нет зарегистрированных аккаунтов для торговли.\n"
                    "Проверьте:\n"
                    "  1. Файл user_data/private_keys.txt содержит ETH приватные ключи\n"
                    "  2. Файл user_data/proxies.txt содержит прокси (опционально)\n"
                    "  3. Кошельки имеют достаточный баланс для регистрации"
                )
                return
        else:
            logger.info("Автоматический onboarding отключен в настройках")

        # Загрузка аккаунтов
        await asyncio.sleep(0.8)
        logger.info("")
        logger.info("Шаг 1: Загрузка аккаунтов...")
        account_manager = AccountManager(
            accounts_file="user_data/accounts.json",
            logger=logger
        )

        if account_manager.get_accounts_count() == 0:
            logger.error(
                "Нет загруженных аккаунтов. "
                "Проверьте что onboarding прошёл успешно или заполните БД вручную."
            )
            return

        # Валидация аккаунтов
        await asyncio.sleep(0.5)
        logger.info("")
        logger.info("Шаг 2: Валидация аккаунтов...")
        validation = account_manager.validate_accounts()
        logger.success(f"Валидных аккаунтов: {len(validation['valid'])}")

        if validation['invalid']:
            logger.warning(
                f"Невалидных аккаунтов: {len(validation['invalid'])}"
            )
            for invalid in validation['invalid']:
                logger.warning(f"  - {invalid}")

        if not validation['valid']:
            logger.error("Нет валидных аккаунтов для торговли")
            return

        # Создание трейдера
        await asyncio.sleep(0.5)
        logger.info("")
        logger.info("Шаг 3: Подготовка торговой системы...")
        accounts = account_manager.get_all_accounts()

        # Проверяем наличие прокси в аккаунтах
        # Теперь каждый аккаунт использует свой прокси (per-account proxy через SDK patch)
        proxy_count = sum(1 for acc in accounts if acc.proxy)
        if proxy_count > 0:
            logger.info(f"🌐 Прокси настроены для {proxy_count}/{len(accounts)} аккаунтов (per-account)")
        else:
            logger.warning("⚠️ Прокси не найдены в аккаунтах, SDK будет подключаться напрямую")

        # ВАЖНО: Установите testnet=True для тестирования
        # testnet=False для работы на mainnet
        testnet = False  # Измените на False для mainnet

        if testnet:
            logger.warning("⚠️ ТЕСТОВЫЙ РЕЖИМ (TESTNET)")

        # Создаем трейдера (сохраняем в переменную для доступа в signal_handler)
        trader = BatchTrader(
            accounts=accounts,
            testnet=testnet,
            logger=logger
        )

        await trader.initialize()

        # Запуск торговли
        await asyncio.sleep(0.5)
        logger.info("")
        logger.info("Шаг 4: Запуск торговли...")
        logger.info("")
        box_width = 46
        logger.info("┌" + "─" * box_width + "┐")
        logger.info(f"│ ⚙️  НАСТРОЙКИ ТОРГОВЛИ{' ' * (box_width - 23)}│")
        logger.info("├" + "─" * box_width + "┤")
        markets_str = ", ".join(TRADING_SETTINGS['markets'])
        line1 = f"Рынки:            {markets_str}"
        line2 = f"Аккаунтов в пачке: {TRADING_SETTINGS['batch_size_range'][0]}-{TRADING_SETTINGS['batch_size_range'][1]}"
        line3 = f"Размер пачки:     ${TRADING_SETTINGS['batch_size_usd'][0]}-${TRADING_SETTINGS['batch_size_usd'][1]}"
        line4 = f"Время холда:      {POSITION_MANAGEMENT['holding_time_range'][0]}-{POSITION_MANAGEMENT['holding_time_range'][1]}s"
        logger.info(f"│ {line1:<{box_width - 3}}│")
        logger.info(f"│ {line2:<{box_width - 3}}│")
        logger.info(f"│ {line3:<{box_width - 3}}│")
        logger.info("├" + "─" * box_width + "┤")
        logger.info(f"│ {line4:<{box_width - 3}}│")
        logger.info("└" + "─" * box_width + "┘")
        logger.info("")

        # Запуск торговли
        try:
            # === AUTO TRADING MODE ===
            await asyncio.sleep(1.0)
            logger.info("")
            logger.info("=" * 60)
            logger.success("🚀 MODE: AUTO TRADING")
            logger.info("=" * 60)
            logger.info("")

            from modules.core.batch_orchestrator import BatchOrchestrator

            # Подготовить список аккаунтов для пула
            # Конвертируем AccountConfig в Dict для совместимости
            accounts_for_pool = []
            for acc in accounts:
                accounts_for_pool.append({
                    'id': acc.account_id if hasattr(acc, 'account_id') else acc.name,
                    'name': acc.name,
                    'account_id': acc.account_id if hasattr(acc, 'account_id') else acc.name
                })

            # Создаем конфиг оркестратора из TRADING_SETTINGS
            orchestrator_config = {
                'use_balanced_pool': True,
                'account_cooldown_seconds': sum(TRADING_SETTINGS['account_cooldown_range']) // 2,  # среднее
                'max_consecutive_errors': TRADING_SETTINGS['max_consecutive_errors'],
                'num_workers': TRADING_SETTINGS['num_workers'],
                'batch_size_range': TRADING_SETTINGS['batch_size_range'],
                'generation_interval': TRADING_SETTINGS['generation_interval'],
                'max_queue_size': TRADING_SETTINGS['max_queue_size'],
            }

            # Создать оркестратор (сохраняем в переменную для доступа в signal_handler)
            orchestrator = BatchOrchestrator(
                accounts=accounts_for_pool,
                markets=[f"{m}-USD" for m in TRADING_SETTINGS['markets']],
                batch_trader=trader,
                config=orchestrator_config
            )

            # Запустить оркестратор
            try:
                await orchestrator.run()
            except asyncio.CancelledError:
                logger.info("Задачи отменены, начинается graceful shutdown...")
                # Shutdown будет вызван в finally блоке

        finally:
            # Закрытие (выполняется всегда)
            # Если оркестратор был создан, вызываем его shutdown
            if orchestrator is not None:
                try:
                    await orchestrator.shutdown(close_positions=True)
                except Exception as e:
                    logger.error(f"Ошибка при shutdown оркестратора: {e}")

            # Закрываем все соединения трейдера
            if trader is not None:
                try:
                    await trader.close()
                except Exception as e:
                    logger.error(f"Ошибка при закрытии трейдера: {e}")

    except asyncio.CancelledError:
        pass  # Уже обработано в finally выше

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise

    finally:
        logger.info("\n" + "=" * 60)
        logger.info("Extended Bot stopped")
        logger.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nBot stopped by user")
    except Exception as e:
        print(f"\n\nCritical error: {e}")
        sys.exit(1)
    finally:
        # Финальная очистка всех pending tasks
        try:
            # Получаем текущий event loop если он есть
            loop = asyncio.get_event_loop()
            if loop and not loop.is_closed():
                # Отменяем все pending задачи
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()

                # Даем время на отмену
                loop.run_until_complete(asyncio.sleep(0.1))

                # Закрываем loop
                loop.close()
        except Exception:
            pass  # Игнорируем ошибки при очистке

        sys.exit(0)
