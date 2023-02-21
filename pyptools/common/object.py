import os
from datetime import datetime, date, time
from typing import List, Dict
from collections import namedtuple, defaultdict
from dataclasses import dataclass
from functools import wraps

import numpy as np

from .constant import *

"""
Ticker, Product  没有重复实例

对于Ticker和Product，不使用 枚举类Exchange，而是用简单的 string表示，降低复杂度，
因为交易所/合约/品种可能随时增减，不能使用枚举类来使之被限制，
而且此处的exchange没有实质的作用，仅需要用string表示。
对于Ticker和Product，它们的Exchange的定义是： s.split(".")[-1]，没有其他更多的含义


ProductInfo, TradingSession
使用不可修改的单例模式（目前不觉得存在一个域内需要多套参数的情况,使用单例可以做到完全统一）,

ProductInfoFile, TradingSessionFile 功能实现

"""


class Singleton:
    def __new__(cls, *args, **kwargs):
        if not getattr(cls, '_singleton', None):
            cls._singleton = super().__new__(cls, *args, **kwargs)
        return cls._singleton


class UnsetDict(dict):
    def __setitem__(self, key, value):
        print(f'{self.__class__.__name__} 不能直接修改键值，请使用 .set()')

    def set(self, key, value):
        super(UnsetDict, self).__setitem__(key, value)


class Ticker:
    """
    标的
    """
    _instances = {}
    count = 0

    def __new__(cls, symbol: str, exchange: str):
        if (symbol, exchange) in cls._instances.keys():
            pass
        else:
            _instance = super().__new__(cls)
            cls._instances[(symbol, exchange)] = _instance
            cls.count += 1
        return cls._instances[(symbol, exchange)]

    def __init__(self, symbol: str, exchange):
        self.symbol = symbol
        self.exchange = exchange
        self.product_name = self._product_name()
        self.product = Product(symbol=self.product_name, exchange=self.exchange)
        self.name = f'{self.symbol}.{self.exchange}'

    def __repr__(self):
        return f'Ticker: {self.name}'

    @classmethod
    def from_name(cls, name: str):
        if '.' in name:
            exchange = name.split('.')[-1]
            symbol = '.'.join(name.split('.')[:-1])
        else:
            exchange = ''
            symbol = name
        return cls(symbol=symbol, exchange=exchange)

    def _product_name(self) -> str:
        # if self.exchange.value in ['DCE', 'CZCE', 'SHFE', 'INE']:
        _num = 0
        for _num, s in enumerate(self.symbol[::-1]):
            if not str(s).isdigit():
                break
        product_name = self.symbol[:len(self.symbol)-_num]
        return product_name

    def __lt__(self, other):
        return self.name.lower() < other.name.lower()

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))


class Product:
    """
    品种
    eg:
        symbol=AP
        exchange=CZCE
        name=AP.CZCE
        InternalProduct=ZZAP

        symbol=ES
        exchange=CME
        name=ES.CME
        InternalProduct=ES

    """
    _instances = {}
    count = 0

    def __new__(cls, symbol: str, exchange: str):
        if (symbol, exchange) in cls._instances.keys():
            pass
        else:
            _instance = super().__new__(cls)
            cls._instances[(symbol, exchange)] = _instance
            cls.count += 1
        return cls._instances[(symbol, exchange)]

    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange
        self.InternalProduct = self._internal_product()
        self.name = f'{self.symbol}.{self.exchange}'

    @classmethod
    def from_name(cls, name):
        if '.' in name:
            exchange = name.split('.')[-1]
            symbol = '.'.join(name.split('.')[:-1])
        else:
            exchange = ''
            symbol = name
        return cls(symbol=symbol, exchange=exchange)

    @classmethod
    def from_internal_name(cls, name):
        if name == 'ZZTC':
            return cls.from_name("ZC.CZCE")
        elif name == "SQau2":
            return cls.from_name("au.SHFE")

        if name.find('DL') == 0:
            new_name = name[2:] + ".DCE"
            return cls.from_name(new_name)
        if name.find('SQ') == 0:
            if name[2:] in ["sc", "lu", "nr", "bc"]:
                new_name = name[2:] + ".INE"
                return cls.from_name(new_name)
            else:
                new_name = name[2:] + ".SHFE"
                return cls.from_name(new_name)
        if name.find('ZZ') == 0:
            new_name = name[2:] + ".CZCE"
            return cls.from_name(new_name)


    def _internal_product(self, ):
        # 特殊例子
        if self.exchange == 'CZCE':
            if self.symbol == 'ZC':
                return 'ZZTC'
        elif self.exchange == 'SHFE':
            if self.symbol == 'au':
                return 'SQau2'
        elif self.exchange == 'CFFEX':
            if self.symbol == 'IF':
                return 'CSI300'
            elif self.symbol == 'IC':
                return 'CSI500'
            elif self.symbol == 'IH':
                return 'SSE50'
        elif self.exchange == 'LME':
            if self.symbol == 'AH3M':
                return 'LmeAH'
            elif self.symbol == 'CA3M':
                return 'LmeCA'
            elif self.symbol == 'L-ZS3M':
                return 'LmeZS'
            elif self.symbol == 'NI3M':
                return 'LmeNI'
            elif self.symbol == 'PB3M':
                return 'LmePB'
            elif self.symbol == 'SN3M':
                return 'LmeSN'

        # 一般情况
        if self.exchange == 'DCE':
            return f'DL{self.symbol}'
        elif self.exchange == 'CZCE':
            return f'ZZ{self.symbol}'
        elif self.exchange in ['SHFE', 'INE']:
            return f'SQ{self.symbol}'
        elif self.exchange == 'LME':
            return f'Lme{self.symbol}'
        elif self.exchange in ['CFFEX', 'CME', 'CME_CBT', 'NYBOT', 'SGXQ']:
            # TT TF
            return self.symbol
        else:
            return self.symbol

    def __lt__(self, other):
        return self.name.lower() < other.name.lower()

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))

    def __repr__(self):
        return f'Product: {self.name}'



"""
    dataclass
"""


@dataclass(order=True)
class TradeData:
    datatime: datetime
    ticker: Ticker
    direction: Direction
    offset_flag: OffsetFlag
    price: float = 0
    volume: float = 0
    commission: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime("%Y%m%d %H%M%S.%f"),
            self.ticker.name,
            self.direction.name,
            self.offset_flag.name,
            str(self.price),
            str(self.volume),
            str(self.commission)
        ])


@dataclass(order=True)
class PositionData:
    datatime: datetime
    ticker: Ticker
    direction: Direction
    volume: float = 0
    volume_today: float = None
    price: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.ticker.name,
            self.direction.name,
            str(self.volume),
            str(self.volume_today) if self.volume_today else '',
            str(self.price),
        ])


@dataclass(order=True)
class AccountData:
    datatime: datetime
    account: str
    balance: float = 0
    available: float = 0
    risk_ration: float = 0

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.account,
            str(self.balance),
            str(self.available),
            str(self.risk_ration),
        ])


@dataclass(order=True)
class TraderPnLData:
    # 对应QMReport.Pnl_.csv
    # 未确定
    datatime: datetime
    trader: str = ''
    pnl: float = 0
    commission: float = 0
    initX: float = np.nan

    def __str__(self):
        return ','.join([
            self.datatime.strftime('%Y%m%d %H%M%S'),
            self.trader,
            str(self.pnl),
            str(self.commission),
            str(self.initX),
        ])

    @classmethod
    def _get_header(cls) -> str:
        return ','.join(['date', 'trader', 'pnl', 'commission', 'initX'])


@dataclass
class TickData:
    """"""

    ticker: Ticker
    date: date
    time: time

    # volume: float = 0
    # open_interest: float = 0
    # last_price: float = 0
    # last_volume: float = 0
    # limit_up: float = 0
    # limit_down: float = 0

    open: float
    high: float
    low: float
    close: float
    volume: float
    price: float

    bid_price_1: float = 0
    bid_price_2: float = 0
    bid_price_3: float = 0
    bid_price_4: float = 0
    bid_price_5: float = 0

    ask_price_1: float = 0
    ask_price_2: float = 0
    ask_price_3: float = 0
    ask_price_4: float = 0
    ask_price_5: float = 0

    bid_volume_1: float = 0
    bid_volume_2: float = 0
    bid_volume_3: float = 0
    bid_volume_4: float = 0
    bid_volume_5: float = 0

    ask_volume_1: float = 0
    ask_volume_2: float = 0
    ask_volume_3: float = 0
    ask_volume_4: float = 0
    ask_volume_5: float = 0

    localtime: time = None


@dataclass
class BarData:
    """
    Candlestick bar data of a certain trading period.
    """

    ticker: Ticker
    date: date
    time: time

    open: float
    high: float
    low: float
    close: float
    volume: float
    price: float
    open_interest: float


# @dataclass
# class LogData:
#     """
#     Log data is used for recording log messages on GUI or in log files.
#     """
#
#     msg: str
#     # level: int = INFO
#
#     def __post_init__(self):
#         """"""
#         self.time = datetime.now()



"""
/platinum/Release/Data/Holiday
"""


class HolidayFile:
    FileName = 'Holiday.csv'

    @classmethod
    def read(cls, p) -> Dict[str, List[date]]:
        assert os.path.isfile(p)
        with open(p) as f:
            l_lines = f.readlines()
        data = defaultdict(list)
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            assert len(line_split) == 2
            _exchange = line_split[0]
            _holiday = datetime.strptime(line_split[1], '%Y/%m/%d').date()
            data[_exchange].append(_holiday)
        return data


class HolidayManager:
    def __init__(self, path):
        self._data: Dict[str, List[date]] = HolidayFile.read(path)

    @property
    def data(self):
        return self._data.copy()


# class TradeSeriesFile:
#     def __init__(self):
#         pass
#
#     @classmethod
#     def to_csv(cls, root, data: List[TradeData]):
#         output_lines = []
#         for n, trade_data in enumerate(sorted(data)):
#             output_lines.append(str(trade_data))
#
#         root = os.path.abspath(root)
#         if not os.path.isdir(root):
#             os.makedirs(root)
#         path = os.path.join(root, 'TradeSeries.csv')
#         with open(path, 'w+') as f:
#             f.write('datetime,ticker,direction,offset_flag,price,volume,commission\n')
#             f.write('\n'.join(output_lines))
#
#
# class PositionSeriesFile:
#     def __init__(self):
#         pass
#
#     @classmethod
#     def to_csv(cls, root, data: List[PositionData], date: str or None = None):
#         output_lines = []
#         for n, trade_data in enumerate(sorted(data)):
#             output_lines.append(str(trade_data))
#
#         root = os.path.abspath(root)
#         if date:
#             root = os.path.join(root, str(date))
#         if not os.path.isdir(root):
#             os.makedirs(root)
#         path = os.path.join(root, 'PositionSeries.csv')
#         with open(path, 'w+') as f:
#             f.write('datetime,ticker,direction,volume,volume_today,price\n')
#             f.write('\n'.join(output_lines))
#
