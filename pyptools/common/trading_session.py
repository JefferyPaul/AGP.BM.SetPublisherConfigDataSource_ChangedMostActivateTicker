"""
TradingSessionData: [Date, Product, TradingSession, ExchangeTimezone]
TradingSessionFile.read() -> Dict[(Product, date), TradingSessionData]

TradingSessionManager.data -> Dict[{_time_zone_index}, Dict[(Product, date), TradingSessionData]]
"""

import os
from datetime import date, time, datetime
from dataclasses import dataclass
from typing import Dict, List
from .object import Product
from collections import defaultdict


@dataclass
class TradingSessionData:
    Date: date              # 信息日期
    Product: Product
    TradingSession: List[List[time]]        #
    ExchangeTimezone: str           # 交易所所在时区，很少情况需要用到，所以作废（乱填）


class TradingSessionDataSet:
    def __init__(self, data):
        self._data: Dict[Product, List[TradingSessionData]] = data

    def get(self, product: Product, checking_date=datetime.today().date()) -> List[List[time]] or None:
        _product_ts_list: List[TradingSessionData] or None = self._data.get(product)
        if not _product_ts_list:
            return None
        else:
            if len(_product_ts_list) == 1:
                return _product_ts_list[0].TradingSession
            else:
                _nearest_ts = [_ts for _ts in _product_ts_list if _ts.Date <= checking_date]
                if _nearest_ts:
                    return max(_nearest_ts, key=lambda x: x.Date).TradingSession
                else:
                    return min(_product_ts_list, key=lambda x: x.Date).TradingSession


def _gen_trading_session(s) -> List[List[time]]:
    """ str to trading-session-data-list"""
    _l = []
    for _pair in s.split('&'):
        _s = datetime.strptime(_pair.split('-')[0], '%H%M%S').time()
        _e = datetime.strptime(_pair.split('-')[1], '%H%M%S').time()
        _l.append([_s, _e])
    return _l


class TradingSessionFile:
    FileName = 'TradingSession.csv'
    Header = 'Date,ProductInfo,DaySession,NightSession,ExchangeTimezone'

    def __init__(self):
        pass

    @classmethod
    def read(cls, p) -> TradingSessionDataSet or None:
        assert os.path.isfile(p)
        d_trading_session = defaultdict(list)

        with open(p) as f:
            l_lines = f.readlines()
        l_lines = [_.strip() for _ in l_lines if _.strip()]
        if len(l_lines) <= 1:
            return None

        for line in l_lines[1:]:
            _line_split = line.split(',')
            assert len(_line_split) == 5
            _start_date = datetime.strptime(_line_split[0], '%Y%m%d').date()
            _product = Product.from_name(_line_split[1])
            _trading_session_data = TradingSessionData(
                Date=_start_date,
                Product=_product,
                TradingSession=_gen_trading_session(_line_split[2]),
                ExchangeTimezone=_line_split[4]
            )
            d_trading_session[_product].append(_trading_session_data)
        return TradingSessionDataSet(d_trading_session)


class TradingSessionManager:
    """
    读取，
        输出目录, 如 "C:/D/_workspace/Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index
    获取，


    数据:
        self.data -> Dict[{_time_zone_index}, Dict[(Product, date), TradingSessionData]]


    """

    def __init__(self, path):
        self._data: Dict[str, TradingSessionDataSet] = {}
        self._set(path)

    @property
    def data(self):
        return self._data.copy()

    def _set(self, path):
        assert os.path.isdir(path)
        for _name in os.listdir(path):
            path_sub = os.path.join(path, _name)
            path_ts_file = os.path.join(path_sub, 'TradingSession.csv')
            if not os.path.isfile(path_ts_file):
                continue
            else:
                try:
                    _ts: TradingSessionDataSet = TradingSessionFile.read(path_ts_file)
                except Exception as e:
                    raise e
                else:
                    _time_zone_index = '.'.join(_name.split('.')[1:])
                    self._data[_time_zone_index] = _ts

    def get(self, product, time_zone_index='210', checking_date=datetime.today().date()) -> List[List[time]] or None:
        if time_zone_index in self._data:
            return self._data[time_zone_index].get(product=product, checking_date=checking_date)
        else:
            return None

    def get_time_zone_data(self, time_zone_index='210') -> TradingSessionDataSet or None:
        if time_zone_index in self._data:
            return self._data[time_zone_index]
        else:
            return None

