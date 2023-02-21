"""
General constant string used in VN Trader.
"""

from enum import Enum
from datetime import datetime, timedelta, time
from typing import List


class Direction(Enum):
    """
    用于表示 盈亏方向 与 价格波动方向，是否一致：一致，为Long；不一致，为Short
    """
    Long = 1
    Short = -1

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))


class OffsetFlag(Enum):
    """
    用于表示 持有数量 的增减。
    """
    Open = 1
    Flat = -1
    FlatToday = -1
    FlatHistory = -2

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return bool(1 - self.__lt__(other))


class HedgeFlag(Enum):
    SPECULATION = 2


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"


class Prefix(Enum):
    """
    Product class.
    """
    EQUITY = "股票"
    Futures = "期货"
    Commodities = "商品"
    Option = "期权"
    Index = "指数"
    FOREX = "外汇"
    SPOT = "现货"
    ETF = "ETF"
    BOND = "债券"
    WARRANT = "权证"
    SPREAD = "价差"
    FUND = "基金"


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"
    RFQ = "询价"


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "看涨期权"
    PUT = "看跌期权"


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"         # China Financial Futures Exchange
    SHFE = "SHFE"           # Shanghai Futures Exchange
    CZCE = "CZCE"           # Zhengzhou Commodity Exchange
    DCE = "DCE"             # Dalian Commodity Exchange
    INE = "INE"             # Shanghai International Energy Exchange
    LME = "LME"
    CME = "CME"
    CME_CBT = "CME_CBT"
    HKEX = "HKEX"
    ICE = "ICE"
    KRX = "KRX"
    NYBOT = "NYBOT"
    SFE = "SFE"
    SGXQ = "SGXQ"
    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange
    SGE = "SGE"             # Shanghai Gold Exchange
    WXE = "WXE"             # Wuxi Steel Exchange
    CFETS = "CFETS"         # China Foreign Exchange Trade System


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"


class Interval(Enum):
    """
    Interval of bar data.
    """
    MINUTE = 60
    HOUR = 3600
    DAILY = 86400
    WEEKLY = "w"
    TICK = "tick"


def _gen_all_time() -> List[time]:
    s = datetime(year=2000, month=1, day=1)
    e = datetime(year=2000, month=1, day=2)
    _ = []
    while s < e:
        _.append(s.time())
        s += timedelta(minutes=1)
    return _


AllMinuteTime: List[time] = _gen_all_time()


class BarDataMode(Enum):
    BackAdjustedData = 'BackAdjustedData'
    NormalData = 'NormalData'
