"""
Account Manager - Управление аккаунтами Extended
"""

import json
import os
from typing import List, Dict, Optional
from pathlib import Path

from modules.core.extended_client import AccountConfig
from modules.core.logger import setup_logger
from modules.data.database import DatabaseManager


class AccountManager:
    """
    Менеджер для работы с аккаунтами

    Поддерживает загрузку аккаунтов из:
    1. База данных (приоритет) - после onboarding
    2. user_data/accounts.json - легаси формат

    Формат JSON файла:
    [
        {
            "name": "Account1",
            "private_key": "0x...",
            "public_key": "0x...",
            "api_key": "...",
            "vault_id": 123456,
            "proxy": "http://user:pass@host:port"
        },
        ...
    ]
    """

    def __init__(
        self,
        accounts_file: str = "user_data/accounts.json",
        use_database: bool = True,
        logger=None
    ):
        """
        Инициализация менеджера

        Args:
            accounts_file: Путь к файлу с аккаунтами (легаси)
            use_database: Использовать БД для загрузки (по умолчанию True)
            logger: Логгер
        """
        self.accounts_file = accounts_file
        self.use_database = use_database
        self.logger = logger or setup_logger()
        self.accounts: List[AccountConfig] = []

        self._load_accounts()

    def _load_accounts(self):
        """Загрузить аккаунты (приоритет БД, затем JSON файл)"""

        # Пытаемся загрузить из БД
        if self.use_database:
            try:
                db = DatabaseManager()
                db_accounts = db.get_all_accounts()

                if db_accounts:
                    self.logger.info(f"Загрузка аккаунтов из БД...")
                    for acc in db_accounts:
                        try:
                            # Используем Stark ключи из БД
                            account = AccountConfig(
                                name=acc['name'],
                                private_key=acc['stark_private_key'],
                                public_key=acc['stark_public_key'],
                                api_key=acc['api_key'],
                                vault_id=acc['vault_id'],
                                proxy=acc.get('proxy', '')
                            )
                            self.accounts.append(account)
                        except Exception as e:
                            self.logger.error(
                                f"Ошибка загрузки аккаунта {acc['name']} из БД: {e}"
                            )

                    self.logger.info(
                        f"Загружено {len(self.accounts)} аккаунтов из БД"
                    )
                    return
                else:
                    self.logger.info(
                        "БД пуста, пытаемся загрузить из JSON файла..."
                    )
            except Exception as e:
                self.logger.warning(
                    f"Не удалось загрузить из БД: {e}. "
                    f"Пытаемся загрузить из JSON файла..."
                )

        # Загружаем из JSON файла (легаси)
        if not os.path.exists(self.accounts_file):
            self.logger.warning(
                f"Файл аккаунтов не найден: {self.accounts_file}"
            )
            self._create_example_file()
            return

        try:
            with open(self.accounts_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                raise ValueError("Файл должен содержать массив аккаунтов")

            for idx, acc_data in enumerate(data):
                try:
                    account = AccountConfig(
                        name=acc_data.get('name', f'Account{idx+1}'),
                        private_key=acc_data['private_key'],
                        public_key=acc_data['public_key'],
                        api_key=acc_data['api_key'],
                        vault_id=int(acc_data['vault_id']),
                        proxy=acc_data.get('proxy', '')
                    )
                    self.accounts.append(account)
                except KeyError as e:
                    self.logger.error(
                        f"Аккаунт {idx+1}: отсутствует обязательное поле {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Ошибка загрузки аккаунта {idx+1}: {e}"
                    )

            self.logger.info(f"Загружено аккаунтов: {len(self.accounts)}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка парсинга JSON: {e}")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки аккаунтов: {e}")

    def _create_example_file(self):
        """Создать пример файла с аккаунтами"""
        example_data = [
            {
                "name": "Account1",
                "private_key": "0x...",
                "public_key": "0x...",
                "api_key": "your_api_key_here",
                "vault_id": 123456,
                "proxy": "http://user:pass@host:port"
            }
        ]

        # Создаем директорию если не существует
        Path(self.accounts_file).parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(self.accounts_file, 'w', encoding='utf-8') as f:
                json.dump(example_data, f, indent=2, ensure_ascii=False)

            self.logger.info(
                f"Создан пример файла аккаунтов: {self.accounts_file}"
            )
            self.logger.info(
                "Заполните файл реальными данными аккаунтов"
            )
        except Exception as e:
            self.logger.error(f"Ошибка создания примера файла: {e}")

    def get_all_accounts(self) -> List[AccountConfig]:
        """
        Получить все аккаунты

        Returns:
            Список всех аккаунтов
        """
        return self.accounts.copy()

    def get_account_by_name(self, name: str) -> Optional[AccountConfig]:
        """
        Получить аккаунт по имени

        Args:
            name: Имя аккаунта

        Returns:
            AccountConfig или None если не найден
        """
        for account in self.accounts:
            if account.name == name:
                return account
        return None

    def get_accounts_batch(self, count: int, offset: int = 0) -> List[AccountConfig]:
        """
        Получить пачку аккаунтов

        Args:
            count: Количество аккаунтов
            offset: Смещение от начала списка

        Returns:
            Список аккаунтов
        """
        return self.accounts[offset:offset + count]

    def split_into_batches(
        self,
        batch_size_range: List[int]
    ) -> List[List[AccountConfig]]:
        """
        Разделить все аккаунты на пачки случайного размера

        Args:
            batch_size_range: [min, max] размер пачки

        Returns:
            Список пачек аккаунтов
        """
        import random

        batches = []
        remaining = self.accounts.copy()

        while remaining:
            # Случайный размер пачки
            batch_size = random.randint(
                batch_size_range[0],
                min(batch_size_range[1], len(remaining))
            )

            # Берем нужное количество аккаунтов
            batch = remaining[:batch_size]
            remaining = remaining[batch_size:]

            batches.append(batch)

        self.logger.info(
            f"Создано пачек: {len(batches)}, "
            f"размеры: {[len(b) for b in batches]}"
        )

        return batches

    def get_accounts_count(self) -> int:
        """
        Получить количество загруженных аккаунтов

        Returns:
            Количество аккаунтов
        """
        return len(self.accounts)

    def validate_accounts(self) -> Dict[str, List[str]]:
        """
        Валидация всех аккаунтов

        Returns:
            Dict с результатами валидации:
            {
                'valid': [список имен валидных аккаунтов],
                'invalid': [список имен невалидных аккаунтов]
            }
        """
        valid = []
        invalid = []

        for account in self.accounts:
            is_valid = True
            errors = []

            # Проверка private_key
            if not account.private_key or not account.private_key.startswith('0x'):
                errors.append("invalid private_key format")
                is_valid = False

            # Проверка public_key
            if not account.public_key or not account.public_key.startswith('0x'):
                errors.append("invalid public_key format")
                is_valid = False

            # Проверка api_key
            if not account.api_key or len(account.api_key) < 10:
                errors.append("invalid api_key")
                is_valid = False

            # Проверка vault_id
            if account.vault_id <= 0:
                errors.append("invalid vault_id")
                is_valid = False

            if is_valid:
                valid.append(account.name)
            else:
                invalid.append(f"{account.name} ({', '.join(errors)})")
                self.logger.warning(
                    f"Аккаунт {account.name} невалиден: {', '.join(errors)}"
                )

        return {
            'valid': valid,
            'invalid': invalid
        }

    def save_accounts(self):
        """
        Сохранить текущий список аккаунтов обратно в файл

        Используется если нужно обновить данные аккаунтов
        """
        try:
            data = []
            for account in self.accounts:
                data.append({
                    'name': account.name,
                    'private_key': account.private_key,
                    'public_key': account.public_key,
                    'api_key': account.api_key,
                    'vault_id': account.vault_id,
                    'proxy': account.proxy
                })

            with open(self.accounts_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Аккаунты сохранены в {self.accounts_file}")

        except Exception as e:
            self.logger.error(f"Ошибка сохранения аккаунтов: {e}")
