"""
Database Manager - Работа с SQLite базой данных
"""

import sqlite3
import json
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime

from modules.core.logger import setup_logger


class DatabaseManager:
    """
    Менеджер базы данных для хранения:
    - Credentials аккаунтов (API keys, Stark keys, Vault IDs)
    - Статистика торговли
    - История ордеров
    """

    def __init__(
        self,
        db_path: str = "database/extended_bot.db",
        logger=None,
        timeout: float = 30.0  # Увеличенный timeout для избежания database locked
    ):
        """
        Инициализация менеджера БД

        Args:
            db_path: Путь к файлу базы данных
            logger: Логгер
            timeout: Timeout для операций с БД (секунды)
        """
        self.db_path = db_path
        self.logger = logger or setup_logger()
        self.timeout = timeout

        # Создаем директорию если не существует
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Инициализируем БД
        self._init_database()

    def _init_database(self):
        """Инициализация структуры БД"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            # Таблица аккаунтов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    eth_address TEXT,
                    eth_private_key TEXT,
                    account_id INTEGER UNIQUE,
                    api_key TEXT,
                    stark_private_key TEXT,
                    stark_public_key TEXT,
                    vault_id INTEGER,
                    proxy TEXT,
                    network TEXT DEFAULT 'mainnet',
                    onboarded_at TIMESTAMP,
                    referral_applied INTEGER DEFAULT 0,
                    referral_code TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица приватных ключей для отслеживания незарегистрированных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS private_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    eth_private_key TEXT UNIQUE NOT NULL,
                    eth_address TEXT,
                    proxy TEXT,
                    is_onboarded INTEGER DEFAULT 0,
                    account_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_name) REFERENCES accounts(name)
                )
            ''')

            # === Миграции для существующих БД ===
            # Добавляем новые колонки если их нет (для обратной совместимости)
            self._run_migrations(cursor)

            # Таблица ордеров
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL,
                    order_id TEXT,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_name) REFERENCES accounts(name)
                )
            ''')

            # Таблица позиций
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size REAL NOT NULL,
                    entry_price REAL,
                    exit_price REAL,
                    pnl REAL,
                    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    close_reason TEXT,
                    FOREIGN KEY (account_name) REFERENCES accounts(name)
                )
            ''')

            # Таблица статистики
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL,
                    total_orders INTEGER DEFAULT 0,
                    successful_orders INTEGER DEFAULT 0,
                    failed_orders INTEGER DEFAULT 0,
                    total_volume REAL DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Индексы для быстрого поиска
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_orders_account '
                'ON orders(account_name)'
            )
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_positions_account '
                'ON positions(account_name)'
            )

            conn.commit()
            conn.close()

            self.logger.info(f"База данных инициализирована: {self.db_path}")

        except Exception as e:
            self.logger.error(f"Ошибка инициализации БД: {e}")
            raise

    def _run_migrations(self, cursor):
        """
        Выполняет миграции для обновления схемы существующей БД.
        Добавляет новые колонки если их нет.
        """
        # Получаем список существующих колонок в таблице accounts
        cursor.execute("PRAGMA table_info(accounts)")
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Миграция 1: Добавить referral_applied если нет
        if 'referral_applied' not in existing_columns:
            try:
                cursor.execute(
                    'ALTER TABLE accounts ADD COLUMN referral_applied INTEGER DEFAULT 0'
                )
                self.logger.info("Миграция: добавлена колонка referral_applied")
            except Exception as e:
                self.logger.debug(f"Колонка referral_applied уже существует: {e}")

        # Миграция 2: Добавить referral_code если нет
        if 'referral_code' not in existing_columns:
            try:
                cursor.execute(
                    'ALTER TABLE accounts ADD COLUMN referral_code TEXT'
                )
                self.logger.info("Миграция: добавлена колонка referral_code")
            except Exception as e:
                self.logger.debug(f"Колонка referral_code уже существует: {e}")

    def save_account(
        self,
        name: str,
        eth_address: Optional[str] = None,
        eth_private_key: Optional[str] = None,
        account_id: Optional[int] = None,
        api_key: Optional[str] = None,
        stark_private_key: Optional[str] = None,
        stark_public_key: Optional[str] = None,
        vault_id: Optional[int] = None,
        proxy: Optional[str] = None,
        network: str = "mainnet",
        onboarded_at: Optional[str] = None
    ) -> int:
        """
        Сохранить или обновить аккаунт

        Args:
            name: Имя аккаунта
            eth_address: Ethereum адрес
            eth_private_key: Ethereum приватный ключ
            account_id: ID аккаунта Extended
            api_key: API ключ
            stark_private_key: Stark private key
            stark_public_key: Stark public key
            vault_id: Vault ID
            proxy: Прокси
            network: Сеть (mainnet/testnet)
            onboarded_at: Время onboarding (ISO format)

        Returns:
            ID записи в БД
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            # Проверяем существование
            cursor.execute('SELECT id FROM accounts WHERE name = ?', (name,))
            existing = cursor.fetchone()

            if existing:
                # Обновляем существующий
                cursor.execute('''
                    UPDATE accounts SET
                        eth_address = COALESCE(?, eth_address),
                        eth_private_key = COALESCE(?, eth_private_key),
                        account_id = COALESCE(?, account_id),
                        api_key = COALESCE(?, api_key),
                        stark_private_key = COALESCE(?, stark_private_key),
                        stark_public_key = COALESCE(?, stark_public_key),
                        vault_id = COALESCE(?, vault_id),
                        proxy = COALESCE(?, proxy),
                        network = COALESCE(?, network),
                        onboarded_at = COALESCE(?, onboarded_at),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE name = ?
                ''', (
                    eth_address, eth_private_key, account_id, api_key,
                    stark_private_key, stark_public_key, vault_id,
                    proxy, network, onboarded_at, name
                ))
                record_id = existing[0]
                self.logger.debug(f"Аккаунт {name} обновлен")
            else:
                # Создаем новый
                cursor.execute('''
                    INSERT INTO accounts (
                        name, eth_address, eth_private_key, account_id,
                        api_key, stark_private_key, stark_public_key,
                        vault_id, proxy, network, onboarded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    name, eth_address, eth_private_key, account_id,
                    api_key, stark_private_key, stark_public_key,
                    vault_id, proxy, network, onboarded_at
                ))
                record_id = cursor.lastrowid
                self.logger.debug(f"Аккаунт {name} создан")

            conn.commit()
            conn.close()

            return record_id

        except Exception as e:
            self.logger.error(f"Ошибка сохранения аккаунта {name}: {e}")
            raise

    def get_account(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Получить аккаунт по имени

        Args:
            name: Имя аккаунта

        Returns:
            Данные аккаунта или None
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM accounts WHERE name = ?', (name,))
            row = cursor.fetchone()

            conn.close()

            if row:
                return dict(row)
            return None

        except Exception as e:
            self.logger.error(f"Ошибка получения аккаунта {name}: {e}")
            raise

    def get_all_accounts(self) -> List[Dict[str, Any]]:
        """
        Получить все аккаунты

        Returns:
            Список всех аккаунтов
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM accounts ORDER BY name')
            rows = cursor.fetchall()

            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            self.logger.error(f"Ошибка получения всех аккаунтов: {e}")
            raise

    def save_order(
        self,
        account_name: str,
        market: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float] = None,
        order_id: Optional[str] = None,
        status: str = "pending"
    ) -> int:
        """
        Сохранить ордер

        Args:
            account_name: Имя аккаунта
            market: Рынок
            side: Направление (BUY/SELL)
            order_type: Тип ордера (MARKET/LIMIT)
            amount: Размер
            price: Цена
            order_id: ID ордера из API
            status: Статус

        Returns:
            ID записи
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO orders (
                    account_name, market, side, order_type,
                    amount, price, order_id, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                account_name, market, side, order_type,
                amount, price, order_id, status
            ))

            record_id = cursor.lastrowid

            conn.commit()
            conn.close()

            return record_id

        except Exception as e:
            self.logger.error(f"Ошибка сохранения ордера: {e}")
            raise

    def save_position(
        self,
        account_name: str,
        market: str,
        side: str,
        size: float,
        entry_price: float,
        exit_price: Optional[float] = None,
        pnl: Optional[float] = None,
        close_reason: Optional[str] = None
    ) -> int:
        """
        Сохранить позицию

        Args:
            account_name: Имя аккаунта
            market: Рынок
            side: Направление (LONG/SHORT)
            size: Размер позиции
            entry_price: Цена входа
            exit_price: Цена выхода
            pnl: PnL
            close_reason: Причина закрытия

        Returns:
            ID записи
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            if exit_price is not None:
                # Закрытая позиция
                cursor.execute('''
                    INSERT INTO positions (
                        account_name, market, side, size, entry_price,
                        exit_price, pnl, closed_at, close_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ''', (
                    account_name, market, side, size, entry_price,
                    exit_price, pnl, close_reason
                ))
            else:
                # Открытая позиция
                cursor.execute('''
                    INSERT INTO positions (
                        account_name, market, side, size, entry_price
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (account_name, market, side, size, entry_price))

            record_id = cursor.lastrowid

            conn.commit()
            conn.close()

            return record_id

        except Exception as e:
            self.logger.error(f"Ошибка сохранения позиции: {e}")
            raise

    def get_statistics(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Получить статистику за период

        Args:
            days: Количество дней

        Returns:
            Статистика по дням
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM statistics
                WHERE date >= date('now', '-' || ? || ' days')
                ORDER BY date DESC
            ''', (days,))

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            self.logger.error(f"Ошибка получения статистики: {e}")
            raise

    def clear_all_data(self):
        """Очистить все данные из БД (для тестирования)"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            cursor.execute('DELETE FROM orders')
            cursor.execute('DELETE FROM positions')
            cursor.execute('DELETE FROM statistics')
            # Аккаунты не удаляем

            conn.commit()
            conn.close()

            self.logger.warning("Все данные очищены из БД")

        except Exception as e:
            self.logger.error(f"Ошибка очистки данных: {e}")
            raise

    # =========================================================================
    # Методы для работы с приватными ключами и проверкой статуса onboarding
    # =========================================================================

    def save_private_key(
        self,
        eth_private_key: str,
        eth_address: Optional[str] = None,
        proxy: Optional[str] = None,
        is_onboarded: bool = False,
        account_name: Optional[str] = None
    ) -> int:
        """
        Сохранить приватный ключ в БД для отслеживания.

        Args:
            eth_private_key: Ethereum приватный ключ
            eth_address: Ethereum адрес (опционально)
            proxy: Прокси для этого ключа
            is_onboarded: Прошел ли onboarding
            account_name: Имя аккаунта после onboarding

        Returns:
            ID записи
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            # Нормализуем ключ
            if not eth_private_key.startswith("0x"):
                eth_private_key = f"0x{eth_private_key}"

            # Проверяем существование
            cursor.execute(
                'SELECT id FROM private_keys WHERE eth_private_key = ?',
                (eth_private_key,)
            )
            existing = cursor.fetchone()

            if existing:
                # Обновляем существующий
                cursor.execute('''
                    UPDATE private_keys SET
                        eth_address = COALESCE(?, eth_address),
                        proxy = COALESCE(?, proxy),
                        is_onboarded = ?,
                        account_name = COALESCE(?, account_name)
                    WHERE eth_private_key = ?
                ''', (eth_address, proxy, int(is_onboarded), account_name, eth_private_key))
                record_id = existing[0]
            else:
                # Создаем новый
                cursor.execute('''
                    INSERT INTO private_keys (
                        eth_private_key, eth_address, proxy, is_onboarded, account_name
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (eth_private_key, eth_address, proxy, int(is_onboarded), account_name))
                record_id = cursor.lastrowid

            conn.commit()
            conn.close()

            return record_id

        except Exception as e:
            self.logger.error(f"Ошибка сохранения приватного ключа: {e}")
            raise

    def get_unonboarded_keys(self) -> List[Dict[str, Any]]:
        """
        Получить список приватных ключей, которые ещё не прошли onboarding.

        Returns:
            Список словарей с данными ключей
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM private_keys
                WHERE is_onboarded = 0
                ORDER BY id
            ''')
            rows = cursor.fetchall()

            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            self.logger.error(f"Ошибка получения незарегистрированных ключей: {e}")
            raise

    def mark_key_as_onboarded(
        self,
        eth_private_key: str,
        account_name: str
    ):
        """
        Отметить приватный ключ как прошедший onboarding.

        Args:
            eth_private_key: Ethereum приватный ключ
            account_name: Имя созданного аккаунта
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            # Нормализуем ключ
            if not eth_private_key.startswith("0x"):
                eth_private_key = f"0x{eth_private_key}"

            cursor.execute('''
                UPDATE private_keys
                SET is_onboarded = 1, account_name = ?
                WHERE eth_private_key = ?
            ''', (account_name, eth_private_key))

            conn.commit()
            conn.close()

        except Exception as e:
            self.logger.error(f"Ошибка обновления статуса ключа: {e}")
            raise

    def update_referral_status(
        self,
        account_name: str,
        referral_applied: bool,
        referral_code: Optional[str] = None
    ):
        """
        Обновить статус применения реферального кода для аккаунта.

        Args:
            account_name: Имя аккаунта
            referral_applied: Применен ли реферальный код
            referral_code: Применённый код
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE accounts
                SET referral_applied = ?, referral_code = ?, updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
            ''', (int(referral_applied), referral_code, account_name))

            conn.commit()
            conn.close()

            self.logger.debug(
                f"Статус реферала обновлен для {account_name}: "
                f"applied={referral_applied}, code={referral_code}"
            )

        except Exception as e:
            self.logger.error(f"Ошибка обновления статуса реферала: {e}")
            raise

    def get_accounts_without_referral(self) -> List[Dict[str, Any]]:
        """
        Получить аккаунты, для которых не применен реферальный код.

        Returns:
            Список аккаунтов без реферального кода
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM accounts
                WHERE referral_applied = 0 OR referral_applied IS NULL
                ORDER BY name
            ''')
            rows = cursor.fetchall()

            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            self.logger.error(f"Ошибка получения аккаунтов без реферала: {e}")
            raise

    def get_account_by_eth_address(self, eth_address: str) -> Optional[Dict[str, Any]]:
        """
        Получить аккаунт по Ethereum адресу.

        Args:
            eth_address: Ethereum адрес

        Returns:
            Данные аккаунта или None
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                'SELECT * FROM accounts WHERE eth_address = ?',
                (eth_address,)
            )
            row = cursor.fetchone()

            conn.close()

            if row:
                return dict(row)
            return None

        except Exception as e:
            self.logger.error(f"Ошибка получения аккаунта по адресу: {e}")
            raise

    def sync_private_keys_from_file(
        self,
        private_keys_file: str = "user_data/private_keys.txt",
        proxies_file: str = "user_data/proxies.txt"
    ) -> Dict[str, int]:
        """
        Синхронизирует приватные ключи из файла с БД.
        Добавляет новые ключи, помечает существующие как onboarded если есть аккаунт.

        Args:
            private_keys_file: Путь к файлу с приватными ключами
            proxies_file: Путь к файлу с прокси

        Returns:
            Статистика: {'total': N, 'new': N, 'existing': N, 'need_onboarding': N}
        """
        from pathlib import Path
        from eth_account import Account

        stats = {'total': 0, 'new': 0, 'existing': 0, 'need_onboarding': 0}

        try:
            # Читаем приватные ключи
            if not Path(private_keys_file).exists():
                self.logger.warning(f"Файл {private_keys_file} не найден")
                return stats

            with open(private_keys_file, 'r', encoding='utf-8') as f:
                eth_keys = [line.strip() for line in f if line.strip()]

            stats['total'] = len(eth_keys)

            # Читаем прокси
            proxies = []
            if Path(proxies_file).exists():
                with open(proxies_file, 'r', encoding='utf-8') as f:
                    proxies = [line.strip() for line in f if line.strip()]

            # Синхронизируем каждый ключ
            for idx, eth_key in enumerate(eth_keys):
                # Нормализуем ключ
                if not eth_key.startswith("0x"):
                    eth_key = f"0x{eth_key}"

                # Получаем ETH адрес из ключа
                try:
                    eth_account = Account.from_key(eth_key)
                    eth_address = eth_account.address
                except Exception as e:
                    self.logger.warning(f"Невалидный ключ #{idx + 1}: {e}")
                    continue

                # Получаем прокси
                proxy = proxies[idx] if idx < len(proxies) else None

                # Проверяем, есть ли уже аккаунт с этим адресом
                existing_account = self.get_account_by_eth_address(eth_address)

                if existing_account:
                    # Ключ уже зарегистрирован
                    self.save_private_key(
                        eth_private_key=eth_key,
                        eth_address=eth_address,
                        proxy=proxy,
                        is_onboarded=True,
                        account_name=existing_account['name']
                    )
                    stats['existing'] += 1
                else:
                    # Новый ключ, нужен onboarding
                    self.save_private_key(
                        eth_private_key=eth_key,
                        eth_address=eth_address,
                        proxy=proxy,
                        is_onboarded=False
                    )
                    stats['new'] += 1
                    stats['need_onboarding'] += 1

            self.logger.info(
                f"Синхронизация ключей: всего={stats['total']}, "
                f"новых={stats['new']}, существующих={stats['existing']}, "
                f"требуют onboarding={stats['need_onboarding']}"
            )

            return stats

        except Exception as e:
            self.logger.error(f"Ошибка синхронизации ключей: {e}")
            raise
