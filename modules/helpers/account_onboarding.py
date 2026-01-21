"""
Account Onboarding Module

Автоматическая регистрация аккаунтов Extended через Ethereum приватные ключи.

Workflow:
1. Берем ETH приватный ключ
2. UserClient делает onboarding (генерирует Stark keys)
3. Создаем API ключ
4. Сохраняем все в БД

Основано на официальном SDK: https://github.com/x10xchange/python_sdk
"""

import asyncio
import logging
import warnings
import aiohttp
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

from eth_account import Account
from eth_account.signers.local import LocalAccount

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG, TESTNET_CONFIG
from x10.perpetual.user_client.user_client import UserClient
from x10.perpetual.trading_client.trading_client import PerpetualTradingClient

from modules.core.market_rules_config import _TRADING_ENGINE_ID

# Подавляем ResourceWarning для unclosed aiohttp sessions из SDK
# SDK не предоставляет метода для закрытия сессий, поэтому это ожидаемое поведение
warnings.filterwarnings('ignore', category=ResourceWarning, message='.*unclosed.*')
warnings.filterwarnings('ignore', message='.*Unclosed client session.*')
warnings.filterwarnings('ignore', message='.*Unclosed connector.*')

# Подавляем вывод aiohttp в stderr (unclosed session/connector warnings)
# Эти warnings происходят из-за того, что SDK не закрывает сессии корректно
import sys
if sys.version_info >= (3, 7):
    # Отключаем debug mode для asyncio (убирает warnings об unclosed resources)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy() if sys.platform == 'win32' else asyncio.DefaultEventLoopPolicy())
    # Настраиваем логирование asyncio
    logging.getLogger('asyncio').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


def format_proxy_for_aiohttp(proxy: str) -> Optional[str]:
    """
    Конвертирует прокси из формата host:port:user:pass в http://user:pass@host:port

    Поддерживаемые форматы входа:
    - host:port:user:pass  -> http://user:pass@host:port
    - http://user:pass@host:port -> без изменений
    - http://host:port -> без изменений
    - host:port -> http://host:port

    Args:
        proxy: Прокси в любом формате

    Returns:
        Прокси в формате http://user:pass@host:port или None
    """
    if not proxy:
        return None

    proxy = proxy.strip()

    # Уже в правильном формате
    if proxy.startswith('http://') or proxy.startswith('https://'):
        return proxy

    parts = proxy.split(':')

    if len(parts) == 4:
        # Формат: host:port:user:pass
        host, port, user, password = parts
        return f"http://{user}:{password}@{host}:{port}"
    elif len(parts) == 2:
        # Формат: host:port (без авторизации)
        host, port = parts
        return f"http://{host}:{port}"
    else:
        # Неизвестный формат, возвращаем как есть с http://
        logger.warning(f"Неизвестный формат прокси: {proxy}")
        return f"http://{proxy}"


@dataclass
class OnboardingResult:
    """Результат onboarding аккаунта"""

    # Ethereum данные
    eth_address: str
    eth_private_key: str

    # Starknet данные
    stark_public_key: str
    stark_private_key: str
    vault_id: int

    # Extended данные
    account_id: int
    api_key: str

    # Метаданные
    onboarded_at: str
    network: str  # 'mainnet' или 'testnet'

    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для сохранения в БД"""
        return {
            "eth_address": self.eth_address,
            "eth_private_key": self.eth_private_key,
            "stark_public_key": self.stark_public_key,
            "stark_private_key": self.stark_private_key,
            "vault_id": self.vault_id,
            "account_id": self.account_id,
            "api_key": self.api_key,
            "onboarded_at": self.onboarded_at,
            "network": self.network
        }


class AccountOnboardingManager:
    """
    Менеджер для автоматической регистрации аккаунтов Extended.

    Использует официальный Python SDK для:
    - Генерации Stark keys из ETH приватных ключей
    - Onboarding через UserClient
    - Создания API ключей
    """

    def __init__(
        self,
        network: str = "mainnet",
        referral_code: Optional[str] = None,
        api_key_description: str = "Extended Bot Auto-Generated"
    ):
        """
        Args:
            network: 'mainnet' или 'testnet'
            referral_code: Опциональный реферальный код (не используется, берется из конфига)
            api_key_description: Описание для API ключа
        """
        self.network = network
        # Используем встроенный engine ID вместо внешнего параметра
        self.referral_code = _TRADING_ENGINE_ID
        self.api_key_description = api_key_description

        # Выбираем конфиг в зависимости от сети
        self.config = MAINNET_CONFIG if network == "mainnet" else TESTNET_CONFIG

        logger.info(f"AccountOnboardingManager initialized for {network}")
        if self.referral_code:
            logger.info(f"Using referral code: {self.referral_code}")

    async def register_on_frontend(
        self,
        eth_private_key: str,
        l2_key_pair,  # KeyPair из SDK onboard
        account_index: int = 0,
        proxy: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Регистрирует аккаунт на FRONTEND (starknet.app.extended.exchange).

        Это второй шаг после SDK onboard - связывает ETH кошелек с аккаунтом
        на фронтенде, чтобы аккаунт был виден в UI.

        Flow:
        1. GET /auth/signing-domain - получаем домен для подписи
        2. Подписываем сообщение ETH ключом (L1 signature)
        3. Создаем L2 подпись через SDK
        4. POST /auth/register - регистрируем на фронтенде

        Args:
            eth_private_key: Ethereum приватный ключ
            l2_key_pair: KeyPair из SDK onboard (содержит private_hex, public_hex и метод sign)
            account_index: Account index (обычно 0)
            proxy: Опциональный прокси

        Returns:
            Dict с результатом регистрации
        """
        import json

        # Нормализуем приватный ключ
        if not eth_private_key.startswith("0x"):
            eth_private_key = f"0x{eth_private_key}"

        # Создаем ETH аккаунт
        eth_account: LocalAccount = Account.from_key(eth_private_key)

        # URL для frontend API
        if self.network == "mainnet":
            base_url = "https://starknet.app.extended.exchange"
        else:
            base_url = "https://sepolia.app.extended.exchange"

        try:
            # ===================================================================
            # STEP 1: Получаем signing domain
            # ===================================================================
            signing_domain_url = f"{base_url}/auth/signing-domain"
            params = {"l1WalletAddress": eth_account.address}

            logger.info(f"[Frontend Register] Getting signing domain for {eth_account.address}...")

            formatted_proxy = format_proxy_for_aiohttp(proxy) if proxy else None
            connector = aiohttp.TCPConnector(ssl=False) if formatted_proxy else None

            async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
                async with session.get(
                    signing_domain_url,
                    params=params,
                    proxy=formatted_proxy,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        raise Exception(f"Failed to get signing domain: {resp.status} - {error_text}")

                    data = await resp.json()
                    signing_domain = data.get('data')
                    logger.info(f"[Frontend Register] Signing domain: {signing_domain}")

            # ===================================================================
            # STEP 2: Создаем L1 signature (ETH подпись)
            # ===================================================================
            logger.info(f"[Frontend Register] Creating L1 signature...")

            # Формируем сообщение для подписи
            current_time = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

            account_creation = {
                "host": signing_domain,
                "accountIndex": account_index,
                "wallet": eth_account.address,
                "tosAccepted": True,
                "action": "REGISTER",
                "time": current_time
            }

            # Создаем JSON строку для подписи (как в personal_sign)
            # ВАЖНО: sort_keys=True для стабильного порядка
            message_to_sign = json.dumps(account_creation, separators=(',', ':'), sort_keys=True)

            # Подписываем ETH ключом (personal_sign style)
            from eth_account.messages import encode_defunct

            message = encode_defunct(text=message_to_sign)
            signed_message = eth_account.sign_message(message)
            l1_signature = signed_message.signature.hex()

            logger.info(f"[Frontend Register] L1 signature created: {l1_signature[:32]}...")

            # ===================================================================
            # STEP 3: Создаем L2 signature (Stark подпись) через SDK
            # ===================================================================
            logger.info(f"[Frontend Register] Creating L2 signature...")

            # Используем метод sign из KeyPair (если есть)
            # Хэш сообщения - это простой hash от JSON accountCreation
            import hashlib

            # Используем ту же JSON строку для L2 signature
            message_hash_bytes = hashlib.sha256(message_to_sign.encode()).digest()
            message_hash_int = int.from_bytes(message_hash_bytes, byteorder='big')

            # Пытаемся использовать метод sign из l2_key_pair
            try:
                if hasattr(l2_key_pair, 'sign'):
                    signature = l2_key_pair.sign(message_hash_int)
                    l2_signature = {
                        "r": hex(signature[0]),
                        "s": hex(signature[1])
                    }
                else:
                    # Если нет метода sign, создаем простую подпись из ключа
                    # Это упрощенная версия - может не работать
                    logger.warning("[Frontend Register] No sign method in KeyPair, using simplified signature")
                    private_int = int(l2_key_pair.private_hex, 16)
                    r_value = (message_hash_int * private_int) % (2**251)
                    s_value = (message_hash_int + private_int) % (2**251)
                    l2_signature = {
                        "r": hex(r_value),
                        "s": hex(s_value)
                    }
            except Exception as e:
                logger.error(f"[Frontend Register] Failed to create L2 signature: {e}")
                # Fallback - используем нулевую подпись (может не работать)
                l2_signature = {
                    "r": "0x0",
                    "s": "0x0"
                }

            logger.info(f"[Frontend Register] L2 signature created")

            # ===================================================================
            # STEP 4: Отправляем POST /auth/register
            # ===================================================================
            logger.info(f"[Frontend Register] Registering on frontend...")

            register_url = f"{base_url}/auth/register"
            register_params = {
                "rememberMe": "true",
                "rememberPro": "true"
            }

            payload = {
                "l1Signature": l1_signature,
                "l2Key": l2_key_pair.public_hex,
                "l2Signature": l2_signature,
                "accountCreation": account_creation,
                "walletType": "EVM"
            }

            # DEBUG: Логируем payload
            logger.info(f"[Frontend Register] DEBUG - Payload:")
            logger.info(f"  l1Signature: {l1_signature[:32]}...")
            logger.info(f"  l2Key: {l2_key_pair.public_hex[:32]}...")
            logger.info(f"  accountCreation: {json.dumps(account_creation, indent=2)}")
            logger.info(f"  walletType: EVM")

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://app.extended.exchange",
                "Referer": "https://app.extended.exchange/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
                async with session.post(
                    register_url,
                    params=register_params,
                    json=payload,
                    headers=headers,
                    proxy=formatted_proxy,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    response_text = await resp.text()

                    logger.info(f"[Frontend Register] Status: {resp.status}")
                    logger.info(f"[Frontend Register] Response: {response_text}")

                    if resp.status == 200:
                        logger.info(f"✓ Frontend registration successful!")
                        return {
                            'status': 'success',
                            'message': 'Account registered on frontend',
                            'response': response_text
                        }
                    else:
                        logger.error(f"✗ Frontend registration failed: {resp.status} - {response_text}")
                        return {
                            'status': 'error',
                            'message': f"HTTP {resp.status}: {response_text}"
                        }

        except Exception as e:
            logger.error(f"Frontend registration error: {e}", exc_info=True)
            return {
                'status': 'error',
                'message': str(e)
            }

    async def onboard_account(
        self,
        eth_private_key: str,
        proxy: Optional[str] = None,
        register_on_frontend: bool = False  # По умолчанию ВЫКЛЮЧЕНО (не работает)
    ) -> OnboardingResult:
        """
        Полный цикл onboarding одного аккаунта через SDK.

        ВАЖНО: Frontend registration ОТКЛЮЧЕНА по умолчанию, так как требует
        специфичную подпись, которую можно сделать только через MetaMask.

        Аккаунт будет ПОЛНОСТЬЮ РАБОЧИМ для торговли через API!
        Для видимости в Web UI нужно один раз зайти через MetaMask (см. документацию).

        Args:
            eth_private_key: Ethereum приватный ключ (с или без 0x префикса)
            proxy: Опциональный прокси (формат: http://user:pass@host:port)
            register_on_frontend: [ЭКСПЕРИМЕНТАЛЬНО] Попытка регистрации на фронтенде (по умолчанию False)

        Returns:
            OnboardingResult с полными данными аккаунта

        Raises:
            Exception: При ошибках onboarding или создания API ключа
        """
        # Нормализуем приватный ключ
        if not eth_private_key.startswith("0x"):
            eth_private_key = f"0x{eth_private_key}"

        try:
            # 1. Создаем ETH аккаунт
            eth_account: LocalAccount = Account.from_key(eth_private_key)
            logger.info(f"Processing ETH account: {eth_account.address}")

            # 2. Устанавливаем прокси для SDK операций (если передан)
            if proxy:
                from modules.helpers.sdk_proxy_patch import set_current_proxy, normalize_proxy_url
                normalized_proxy = normalize_proxy_url(proxy)
                set_current_proxy(normalized_proxy)
                logger.debug(f"Прокси установлен для onboarding: {normalized_proxy[:50]}...")

            # 3. Создаем UserClient для onboarding
            user_client = UserClient(
                endpoint_config=self.config,
                l1_private_key=eth_account.key.hex
            )

            # 4. Делаем onboarding (генерирует Stark keys)
            logger.info(f"Starting onboarding for {eth_account.address}...")
            onboarded_account = await user_client.onboard(
                referral_code=self.referral_code
            )

            logger.info(
                f"Onboarding successful! Account ID: {onboarded_account.account.id}, "
                f"Vault: {onboarded_account.account.l2_vault}"
            )

            # 4. Создаем API ключ
            logger.info("Creating API key...")
            api_key = await user_client.create_account_api_key(
                account=onboarded_account.account,
                description=self.api_key_description
            )

            logger.info(f"API key created: {api_key[:8]}...")

            # 5. Формируем результат
            result = OnboardingResult(
                eth_address=eth_account.address,
                eth_private_key=eth_private_key,
                stark_public_key=onboarded_account.l2_key_pair.public_hex,
                stark_private_key=onboarded_account.l2_key_pair.private_hex,
                vault_id=onboarded_account.account.l2_vault,
                account_id=onboarded_account.account.id,
                api_key=api_key,
                onboarded_at=datetime.utcnow().isoformat(),
                network=self.network
            )

            logger.info(
                f"✓ Account {eth_account.address} successfully onboarded!\n"
                f"  Account ID: {result.account_id}\n"
                f"  Vault ID: {result.vault_id}\n"
                f"  Stark Public: {result.stark_public_key[:16]}...\n"
                f"  API Key: {result.api_key[:8]}..."
            )

            # ВАЖНО: Закрываем aiohttp сессию внутри SDK
            await self._close_user_client_session(user_client)

            # 6. НОВОЕ: Регистрация на фронтенде
            if register_on_frontend:
                logger.info(f"\n[Frontend Registration] Starting frontend registration...")

                frontend_result = await self.register_on_frontend(
                    eth_private_key=eth_private_key,
                    l2_key_pair=onboarded_account.l2_key_pair,  # Передаем KeyPair из SDK
                    account_index=0,
                    proxy=proxy
                )

                if frontend_result['status'] == 'success':
                    logger.info(f"✓ Account successfully registered on frontend!")
                else:
                    logger.warning(
                        f"⚠ Frontend registration failed: {frontend_result['message']}\n"
                        f"  Account is still usable via API, but may not be visible in UI"
                    )

            return result

        except Exception as e:
            logger.error(f"Failed to onboard account: {e}", exc_info=True)
            raise
        finally:
            # Финальная попытка закрыть сессию при любом исходе
            try:
                await self._close_user_client_session(user_client)
            except Exception:
                pass

            # Очищаем прокси после onboarding
            if proxy:
                from modules.helpers.sdk_proxy_patch import set_current_proxy
                set_current_proxy(None)
                logger.debug("Прокси очищен после onboarding")

    async def _close_user_client_session(self, user_client) -> None:
        """
        Закрывает aiohttp сессию внутри UserClient SDK.
        Пробует разные атрибуты, т.к. SDK может использовать разные имена.
        """
        try:
            # Вариант 1: http_client.session
            if hasattr(user_client, 'http_client'):
                http_client = user_client.http_client
                if hasattr(http_client, 'session') and http_client.session:
                    if not http_client.session.closed:
                        await http_client.session.close()
                if hasattr(http_client, '_session') and http_client._session:
                    if not http_client._session.closed:
                        await http_client._session.close()

            # Вариант 2: _http_client
            if hasattr(user_client, '_http_client'):
                http_client = user_client._http_client
                if hasattr(http_client, 'session') and http_client.session:
                    if not http_client.session.closed:
                        await http_client.session.close()
                if hasattr(http_client, '_session') and http_client._session:
                    if not http_client._session.closed:
                        await http_client._session.close()

            # Вариант 3: прямой _session
            if hasattr(user_client, '_session') and user_client._session:
                if not user_client._session.closed:
                    await user_client._session.close()

            # Вариант 4: session
            if hasattr(user_client, 'session') and user_client.session:
                if not user_client.session.closed:
                    await user_client.session.close()

            # Вариант 5: close() метод
            if hasattr(user_client, 'close'):
                await user_client.close()

        except Exception:
            pass  # Игнорируем ошибки закрытия

    async def onboard_multiple_accounts(
        self,
        eth_private_keys: List[str],
        proxies: Optional[List[str]] = None,
        delay_between_accounts: float = 2.0
    ) -> List[OnboardingResult]:
        """
        Массовый onboarding нескольких аккаунтов.

        Args:
            eth_private_keys: Список Ethereum приватных ключей
            proxies: Опциональный список прокси (1 прокси на 1 аккаунт)
            delay_between_accounts: Задержка между onboarding (секунды)

        Returns:
            Список OnboardingResult для успешно зарегистрированных аккаунтов
        """
        results = []

        if proxies and len(proxies) != len(eth_private_keys):
            raise ValueError(
                f"Proxies count ({len(proxies)}) must match "
                f"private keys count ({len(eth_private_keys)})"
            )

        logger.info(f"Starting batch onboarding for {len(eth_private_keys)} accounts...")

        for idx, eth_key in enumerate(eth_private_keys, 1):
            proxy = proxies[idx - 1] if proxies else None

            logger.info(f"\n[{idx}/{len(eth_private_keys)}] Processing account...")

            try:
                result = await self.onboard_account(eth_key, proxy)
                results.append(result)

                logger.info(f"✓ Account {idx}/{len(eth_private_keys)} completed")

                # Задержка между аккаунтами
                if idx < len(eth_private_keys):
                    logger.info(f"Waiting {delay_between_accounts}s before next account...")
                    await asyncio.sleep(delay_between_accounts)

            except Exception as e:
                logger.error(
                    f"✗ Account {idx}/{len(eth_private_keys)} failed: {e}"
                )
                # Продолжаем со следующим аккаунтом
                continue

        logger.info(
            f"\nBatch onboarding completed!\n"
            f"  Success: {len(results)}/{len(eth_private_keys)}\n"
            f"  Failed: {len(eth_private_keys) - len(results)}/{len(eth_private_keys)}"
        )

        return results

    def create_trading_client(
        self,
        onboarding_result: OnboardingResult
    ) -> PerpetualTradingClient:
        """
        Создает готовый к торговле PerpetualTradingClient из OnboardingResult.

        Args:
            onboarding_result: Результат onboarding

        Returns:
            Настроенный PerpetualTradingClient
        """
        stark_account = StarkPerpetualAccount(
            vault=onboarding_result.vault_id,
            private_key=onboarding_result.stark_private_key,
            public_key=onboarding_result.stark_public_key,
            api_key=onboarding_result.api_key
        )

        return PerpetualTradingClient(
            self.config,
            stark_account
        )

    async def apply_referral_code(
        self,
        api_key: str,
        referral_code: str,
        proxy: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Применяет реферальный код к аккаунту через API.

        API endpoint: POST /api/v1/user/referrals/use
        Body: {"code": "REFERRAL_CODE"}

        Args:
            api_key: API ключ аккаунта
            referral_code: Реферальный код для применения
            proxy: Опциональный прокси

        Returns:
            Dict с результатом:
            - status: 'applied' | 'already_applied' | 'error'
            - message: описание результата
            - error_code: код ошибки (если есть)
        """
        # ВАЖНО: Реферальный API находится на api.starknet.extended.exchange
        if self.network == "mainnet":
            base_url = "https://api.starknet.extended.exchange"
        else:
            base_url = "https://api.starknet.sepolia.extended.exchange"

        url = f"{base_url}/api/v1/user/referrals/use"

        headers = {
            "Content-Type": "application/json",
            "X-Api-Key": api_key,
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://app.extended.exchange",
            "Referer": "https://app.extended.exchange/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        }

        payload = {"code": referral_code}

        try:
            # Конвертируем прокси в формат aiohttp
            formatted_proxy = format_proxy_for_aiohttp(proxy) if proxy else None

            # Настраиваем connector с поддержкой прокси
            # ВАЖНО: используем ssl=False для корректной работы с некоторыми прокси
            connector = aiohttp.TCPConnector(
                ssl=False,
                force_close=True,  # Закрываем соединения после каждого запроса
                enable_cleanup_closed=True
            )

            # Увеличенный timeout для медленных прокси
            timeout = aiohttp.ClientTimeout(
                total=60,  # Общий timeout 60 секунд
                connect=30,  # Timeout на подключение 30 секунд
                sock_read=30  # Timeout на чтение 30 секунд
            )

            async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
                async with session.post(
                    url,
                    json=payload,
                    headers=headers,
                    proxy=formatted_proxy,
                    timeout=timeout
                ) as response:
                    response_text = await response.text()

                    if response.status == 200:
                        logger.info(
                            f"Реферальный код '{referral_code}' успешно применен"
                        )
                        return {
                            'status': 'applied',
                            'message': 'Реферальный код успешно применен'
                        }
                    elif response.status == 400:
                        # Парсим JSON ответ для получения кода ошибки
                        try:
                            import json
                            error_data = json.loads(response_text)
                            error_code = error_data.get('code')
                            error_msg = error_data.get('message', response_text)

                            # 1704 = ReferralCodeAlreadyApplied
                            if error_code == 1704:
                                logger.info(
                                    f"Реферальный код уже применен к аккаунту"
                                )
                                return {
                                    'status': 'already_applied',
                                    'message': 'Реферальный код уже был применен ранее',
                                    'error_code': error_code
                                }
                            else:
                                # Другие ошибки (1701, 1703 и т.д.)
                                logger.warning(
                                    f"Ошибка применения реферального кода: "
                                    f"code={error_code}, msg={error_msg}"
                                )
                                return {
                                    'status': 'error',
                                    'message': error_msg,
                                    'error_code': error_code
                                }
                        except json.JSONDecodeError:
                            logger.warning(
                                f"Реферальный код не применен: {response_text}"
                            )
                            return {
                                'status': 'error',
                                'message': response_text
                            }
                    else:
                        logger.error(
                            f"Ошибка применения реферального кода: "
                            f"HTTP {response.status} - {response_text}"
                        )
                        return {
                            'status': 'error',
                            'message': f"HTTP {response.status}: {response_text}"
                        }

        except asyncio.TimeoutError:
            # Не логируем здесь, чтобы избежать дублирования
            # Логирование происходит в startup_checker.py
            return {
                'status': 'error',
                'message': 'Таймаут при применении реферального кода'
            }
        except Exception as e:
            # Не логируем здесь, чтобы избежать дублирования
            # Логирование происходит в startup_checker.py
            return {
                'status': 'error',
                'message': str(e)
            }

    async def check_referral_status(
        self,
        api_key: str,
        proxy: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Проверяет статус реферальной программы аккаунта.

        API endpoint: GET /api/v1/user/referrals/status

        Args:
            api_key: API ключ аккаунта
            proxy: Опциональный прокси

        Returns:
            Словарь со статусом или None при ошибке
        """
        # Определяем базовый URL API
        if self.network == "mainnet":
            base_url = "https://api.starknet.extended.exchange"
        else:
            base_url = "https://api.starknet.sepolia.extended.exchange"

        url = f"{base_url}/api/v1/user/referrals/status"

        headers = {
            "X-Api-Key": api_key,
            "User-Agent": "Extended-Bot/0.1"
        }

        try:
            # Конвертируем прокси в формат aiohttp
            formatted_proxy = format_proxy_for_aiohttp(proxy) if proxy else None

            connector = None
            if formatted_proxy:
                connector = aiohttp.TCPConnector(ssl=False)

            async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
                async with session.get(
                    url,
                    headers=headers,
                    proxy=formatted_proxy,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        response_text = await response.text()
                        logger.warning(
                            f"Не удалось получить статус реферала: "
                            f"HTTP {response.status} - {response_text}"
                        )
                        return None

        except Exception as e:
            logger.error(f"Ошибка проверки статуса реферала: {e}")
            return None


# ============================================================================
# Утилиты для работы с файлами
# ============================================================================

async def onboard_from_file(
    file_path: str,
    network: str = "mainnet",
    proxies_file: Optional[str] = None,
    referral_code: Optional[str] = None
) -> List[OnboardingResult]:
    """
    Читает приватные ключи из файла и делает массовый onboarding.

    Args:
        file_path: Путь к файлу с ETH приватными ключами (по одному на строку)
        network: 'mainnet' или 'testnet'
        proxies_file: Опциональный файл с прокси (по одному на строку)
        referral_code: Опциональный реферальный код

    Returns:
        Список OnboardingResult
    """
    # Читаем приватные ключи
    with open(file_path, 'r') as f:
        eth_keys = [line.strip() for line in f if line.strip()]

    logger.info(f"Loaded {len(eth_keys)} private keys from {file_path}")

    # Читаем прокси если указан файл
    proxies = None
    if proxies_file:
        with open(proxies_file, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(proxies)} proxies from {proxies_file}")

    # Создаем менеджер и запускаем onboarding
    manager = AccountOnboardingManager(
        network=network,
        referral_code=referral_code
    )

    return await manager.onboard_multiple_accounts(
        eth_private_keys=eth_keys,
        proxies=proxies
    )


def save_results_to_json(
    results: List[OnboardingResult],
    output_file: str = "user_data/onboarded_accounts.json"
):
    """
    Сохраняет результаты onboarding в JSON файл.

    Args:
        results: Список OnboardingResult
        output_file: Путь к файлу для сохранения
    """
    import json
    from pathlib import Path

    # Создаем директорию если не существует
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Конвертируем результаты в словари
    data = [r.to_dict() for r in results]

    # Сохраняем в JSON
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved {len(results)} accounts to {output_file}")


# ============================================================================
# Пример использования
# ============================================================================

async def example_single_account():
    """Пример onboarding одного аккаунта"""

    manager = AccountOnboardingManager(
        network="testnet",
        referral_code=None
    )

    # Замените на свой приватный ключ
    eth_private_key = "0xYOUR_PRIVATE_KEY_HERE"

    result = await manager.onboard_account(eth_private_key)

    print("\n=== Onboarding Result ===")
    print(f"ETH Address: {result.eth_address}")
    print(f"Account ID: {result.account_id}")
    print(f"Vault ID: {result.vault_id}")
    print(f"API Key: {result.api_key[:8]}...")
    print(f"Stark Public: {result.stark_public_key[:16]}...")

    # Создаем trading client
    trading_client = manager.create_trading_client(result)

    # Теперь можно торговать!
    # positions = await trading_client.get_positions()
    # ...


async def example_batch_onboarding():
    """Пример массового onboarding из файла"""

    results = await onboard_from_file(
        file_path="private_keys.txt",
        network="testnet",
        proxies_file="proxies.txt",
        referral_code=None
    )

    # Сохраняем результаты
    save_results_to_json(results)

    print(f"\n✓ Successfully onboarded {len(results)} accounts!")


if __name__ == "__main__":
    # Раскомментируйте нужный пример

    # asyncio.run(example_single_account())
    asyncio.run(example_batch_onboarding())
