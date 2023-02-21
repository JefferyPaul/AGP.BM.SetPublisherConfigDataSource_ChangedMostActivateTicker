import os
from datetime import date, datetime
from dataclasses import dataclass
from ..common.object import Product, Ticker
from typing import List, Dict
from collections import defaultdict


@dataclass
class MostActivateTickerInfo:
    Product: Product
    Date: date
    Ticker: Ticker
    BackAdjustFactor: float


class MostActivateTickerFile:
    FileName = 'MostActiveTickers.csv'

    @classmethod
    def read(cls, p) -> Dict[Product, List[MostActivateTickerInfo]]:
        assert os.path.isfile(p)
        with open(p) as f:
            l_lines = f.readlines()
        data = defaultdict(list)
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            assert len(line_split) == 4
            _date = line_split[0]
            _product_name = line_split[1]
            _ticker_name = line_split[2]
            _baf = float(line_split[3])
            _ticker = Ticker.from_name(_ticker_name)
            data[_ticker.product].append(MostActivateTickerInfo(
                Product=_ticker.product,
                Date=datetime.strptime(_date, '%Y%m%d').date(),
                Ticker=_ticker,
                BackAdjustFactor=_baf
            ))
        return data


class MostActivateTickerManager:
    def __init__(self, path):
        self._data: Dict[Product, List[MostActivateTickerInfo]] = MostActivateTickerFile.read(path)

    @property
    def data(self) -> Dict[Product, List[MostActivateTickerInfo]]:
        return self._data.copy()

    def get_a_most_activate_ticker(self, product: Product, tdate: date = datetime.now().date()) -> Ticker or None:
        """获取某个Product在某天时的最活跃合约"""
        _product_mai: List[MostActivateTickerInfo] = self._data.get(product)
        _the_info = max([_ for _ in _product_mai if _.Date <= tdate], key=lambda x: x.Date)
        if _the_info:
            return _the_info.Ticker
        else:
            return None

    def get_most_activate_tickers_at_date(self, tdate: date = datetime.now().date()) -> Dict[Product, Ticker]:
        """获取所有Product在某天时的最活跃合约"""
        _d_most_act_infos = {}
        for _product, _l_most_act_infos in self._data.items():
            _d_most_act_infos[_product] = max(
                [_ for _ in _l_most_act_infos if _.Date <= tdate], key=lambda x: x.Date).Ticker
        return _d_most_act_infos
