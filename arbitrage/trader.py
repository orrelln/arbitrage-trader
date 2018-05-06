import ccxt
from collections import namedtuple
from time import sleep


class Trader:
    """
    WIP
    Performs inter-exchange trades, tracks information for wallets, and performs balancing mechanisms
    """
    Order = namedtuple('Order', ['buy_ex', 'sell_ex', 'symbol', 'buy_price', 'sell_price', 'volume'])

    def __init__(self, exchanges, order):
        self.order = order
        self.exchanges = exchanges
        self.amount = 0

    def perform_arbitrage(self):
        """Most basic configuration for performing arbitrage knowing specific order."""
        self.purchase(self.order)
        self.send(self.order)
        self.receive(self.order)
        self.sell(self.order)

    def purchase(self, order):
        """Creates a buy order above market price for specified exchange and pair."""
        buy_exchange = self.exchanges[order.buy_ex]
        buy_exchange.create_order(order.symbol, 'limit', 'buy', order.volume, order.buy_price * 1.10)

    def sell(self, order):
        """Creates a sell order below market price for specified exchange and pair."""
        sell_exchange = self.exchanges[order.sell_ex]
        sell_exchange.create_order(order.symbol, 'limit', 'sell', self.amount, order.sell_price * 0.90)

    def send(self, order):
        """Retrieves address and balance for symbol, then sends it to another exchange."""
        send_exchange = self.exchanges[order.buy_ex]
        receive_exchange = self.exchanges[order.sell_ex]

        split_pair = order.symbol.split('/')
        currency = split_pair[0]

        address = self._fetch_address(receive_exchange, currency)

        amount = 0
        while amount < order.volume * .95:
            amount = self._fetch_balances(send_exchange, currency)

        send_exchange.withdraw(currency, amount, address, tag="")

    def receive(self, order):
        """Waits for balance to arrive on specified exchange."""
        receive_exchange = self.exchanges[order.sell_ex]

        split_pair = order.symbol.split('/')
        currency = split_pair[0]

        og_amount = self._fetch_balances(receive_exchange, currency)
        while self.amount <= og_amount:
            self.amount = self._fetch_balances(receive_exchange, currency)
            sleep(5)

    def _fetch_balances(self, exchange, currency):
        """Fetches balances using exchange function."""
        balances = None
        while balances is None:
            try:
                balances = exchange.fetch_balance()
            except Exception as e:
                sleep(0.1)
        return balances[currency]['free']

    def _fetch_address(self, exchange, currency):
        """Fetches address using exchange function."""
        address = None
        while address is None:
            try:
                address = exchange.fetch_deposit_address(currency)
            except Exception as e:
                sleep(0.1)
        return address['address']