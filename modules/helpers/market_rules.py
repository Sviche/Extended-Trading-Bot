"""
Market Rules Helper - Получение правил трейдинга для монет Extended Exchange
"""

from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional, List
from modules.core.market_rules_config import MARKET_RULES


class MarketRulesHelper:
    """Хелпер для работы с правилами трейдинга Extended Exchange"""

    def __init__(self):
        self.rules = MARKET_RULES

    def get_market_rules(self, market: str) -> Optional[Dict]:
        """
        Получить правила трейдинга для конкретной монеты

        Args:
            market: Тикер монеты без суффикса (например, 'BTC', 'ETH', 'SOL')

        Returns:
            Словарь с правилами или None, если монета не найдена
        """
        # Убираем суффикс -USD если есть
        clean_market = market.replace('-USD', '')
        return self.rules.get(clean_market)

    def get_min_trade_size(self, market: str) -> Optional[Decimal]:
        """Получить минимальный размер позиции для монеты"""
        rules = self.get_market_rules(market)
        if rules:
            return Decimal(rules['min_trade_size'])
        return None

    def get_min_change_size(self, market: str) -> Optional[Decimal]:
        """Получить минимальное изменение размера позиции"""
        rules = self.get_market_rules(market)
        if rules:
            return Decimal(rules['min_change_size'])
        return None

    def get_min_price_change(self, market: str) -> Optional[Decimal]:
        """Получить минимальное изменение цены"""
        rules = self.get_market_rules(market)
        if rules:
            return Decimal(rules['min_price_change'])
        return None

    def get_limit_price_cap(self, market: str) -> Optional[float]:
        """Получить лимит отклонения цены (в процентах, например 0.05 = 5%)"""
        rules = self.get_market_rules(market)
        if rules:
            return rules['limit_price_cap']
        return None

    def get_max_position_usd(self, market: str) -> Optional[int]:
        """Получить максимальный размер позиции в USD"""
        rules = self.get_market_rules(market)
        if rules:
            return rules['max_position']
        return None

    def get_max_market_order_usd(self, market: str) -> Optional[int]:
        """Получить максимальный размер маркет-ордера в USD"""
        rules = self.get_market_rules(market)
        if rules:
            return rules['max_market_order']
        return None

    def get_max_limit_order_usd(self, market: str) -> Optional[int]:
        """Получить максимальный размер лимит-ордера в USD"""
        rules = self.get_market_rules(market)
        if rules:
            return rules['max_limit_order']
        return None

    def get_group(self, market: str) -> Optional[int]:
        """Получить группу монеты (1-5)"""
        rules = self.get_market_rules(market)
        if rules:
            return rules['group']
        return None

    def is_tradfi_market(self, market: str) -> bool:
        """
        Проверить, является ли маркет TradFi активом (group 5).

        TradFi активы (XAU, XAG, EUR, XCU, XNG, XPT, XBR, SPX500m, TECH100m и др.)
        имеют особые требования API — например, trigger_price_type должен быть MARK.

        Args:
            market: Тикер монеты (например, 'XAU', 'XAG', 'EUR')

        Returns:
            True если маркет из группы 5 (TradFi / Commodities / Forex)
        """
        group = self.get_group(market)
        return group == 5

    def is_market_supported(self, market: str) -> bool:
        """
        Проверить, поддерживается ли монета

        Args:
            market: Тикер монеты (например, 'BTC', 'ETH', 'DOGE')

        Returns:
            True если монета поддерживается
        """
        return self.get_market_rules(market) is not None

    def get_all_supported_markets(self) -> List[str]:
        """Получить список всех поддерживаемых монет"""
        return list(self.rules.keys())

    def get_markets_by_group(self, group: int) -> List[str]:
        """
        Получить список монет по группе

        Args:
            group: Номер группы (1-5)

        Returns:
            Список тикеров монет в данной группе
        """
        return [
            market for market, rules in self.rules.items()
            if rules['group'] == group
        ]

    def validate_trade_size(self, market: str, size: Decimal) -> bool:
        """
        Проверить, что размер позиции соответствует правилам

        Args:
            market: Тикер монеты
            size: Размер позиции в базовом активе

        Returns:
            True если размер корректен
        """
        min_size = self.get_min_trade_size(market)
        if min_size is None:
            return False
        return size >= min_size

    def validate_price_for_limit_order(self, market: str, limit_price: Decimal,
                                       mark_price: Decimal, is_buy: bool) -> bool:
        """
        Проверить, что цена лимитного ордера в допустимых пределах

        Args:
            market: Тикер монеты
            limit_price: Цена лимитного ордера
            mark_price: Текущая маркетная цена
            is_buy: True для покупки (лонг), False для продажи (шорт)

        Returns:
            True если цена в допустимых пределах
        """
        cap = self.get_limit_price_cap(market)
        if cap is None:
            return False

        if is_buy:
            # Long limit order price cannot exceed Mark Price × (1 + cap)
            max_price = mark_price * Decimal(1 + cap)
            return limit_price <= max_price
        else:
            # Short limit order price cannot be below Mark Price × (1 - cap)
            min_price = mark_price * Decimal(1 - cap)
            return limit_price >= min_price

    def round_size_to_min_change(self, market: str, size: Decimal) -> Decimal:
        """
        Округлить размер позиции до минимального изменения

        Args:
            market: Тикер монеты
            size: Размер позиции

        Returns:
            Округленный размер
        """
        min_change = self.get_min_change_size(market)
        if min_change is None:
            return size

        # Конвертируем в Decimal если еще не Decimal
        size = Decimal(str(size))
        min_change = Decimal(str(min_change))

        # Округляем вниз до ближайшего min_change
        multiplier = (size / min_change).quantize(Decimal('1'), rounding=ROUND_DOWN)
        return multiplier * min_change

    def round_price_to_min_change(self, market: str, price: Decimal) -> Decimal:
        """
        Округлить цену до минимального изменения

        Args:
            market: Тикер монеты
            price: Цена

        Returns:
            Округленная цена
        """
        min_change = self.get_min_price_change(market)
        if min_change is None:
            return price

        # Конвертируем в Decimal если еще не Decimal
        price = Decimal(str(price))
        min_change = Decimal(str(min_change))

        # Округляем до ближайшего min_change (вниз)
        # Используем quantize для правильного округления
        multiplier = (price / min_change).quantize(Decimal('1'), rounding=ROUND_DOWN)
        return multiplier * min_change

    def get_market_info_str(self, market: str) -> str:
        """
        Получить строковое представление информации о монете

        Args:
            market: Тикер монеты

        Returns:
            Строка с информацией
        """
        rules = self.get_market_rules(market)
        if not rules:
            return f"Market {market} не поддерживается"

        return f"""
Market: {market}
Group: {rules['group']}
Min Trade Size: {rules['min_trade_size']}
Min Change Size: {rules['min_change_size']}
Min Price Change: ${rules['min_price_change']}
Limit Price Cap: {rules['limit_price_cap'] * 100}%
Max Market Order: ${rules['max_market_order']:,}
Max Limit Order: ${rules['max_limit_order']:,}
Max Position: ${rules['max_position']:,}
        """.strip()


# Глобальный экземпляр
market_rules = MarketRulesHelper()


# Удобные функции для быстрого доступа
def get_min_trade_size(market: str) -> Optional[Decimal]:
    """Получить минимальный размер позиции"""
    return market_rules.get_min_trade_size(market)


def is_market_supported(market: str) -> bool:
    """Проверить поддержку монеты"""
    return market_rules.is_market_supported(market)


def get_all_markets() -> List[str]:
    """Получить все поддерживаемые монеты"""
    return market_rules.get_all_supported_markets()


def validate_trade_size(market: str, size: Decimal) -> bool:
    """Проверить корректность размера позиции"""
    return market_rules.validate_trade_size(market, size)


if __name__ == "__main__":
    # Примеры использования
    helper = MarketRulesHelper()

    print("=== Все поддерживаемые монеты ===")
    print(", ".join(helper.get_all_supported_markets()))

    print("\n=== Монеты Group 1 (Major) ===")
    print(", ".join(helper.get_markets_by_group(1)))

    print("\n=== Монеты Group 2 (Large caps) ===")
    print(", ".join(helper.get_markets_by_group(2)))

    print("\n=== Информация о BTC ===")
    print(helper.get_market_info_str('BTC'))

    print("\n=== Информация о DOGE ===")
    print(helper.get_market_info_str('DOGE'))

    print("\n=== Проверка размера позиции ===")
    print(f"BTC 0.0001: {helper.validate_trade_size('BTC', Decimal('0.0001'))}")  # True
    print(f"BTC 0.00001: {helper.validate_trade_size('BTC', Decimal('0.00001'))}")  # False

    print("\n=== Округление размера ===")
    print(f"BTC 0.00123 → {helper.round_size_to_min_change('BTC', Decimal('0.00123'))}")
    print(f"DOGE 1234 → {helper.round_size_to_min_change('DOGE', Decimal('1234'))}")
