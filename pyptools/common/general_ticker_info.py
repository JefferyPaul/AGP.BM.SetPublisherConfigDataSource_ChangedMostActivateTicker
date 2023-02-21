"""
GeneralTickerInfo.csv

- TickerInfoData
  数据类型,存储数据.
    product: Product
    prefix: str  # Futures / Stock / Options / ...
    currency: str
    point_value: float  # 1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
    min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值
    lot_size: float  # 最少交易多少手 （的倍数），1手 是多少 张(shares)
    commission_on_rate: float  # 手续费，交易价值的比率
    commission_per_share: float  # 手续费，每张多少钱
    slippage_points: float
    flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
    margin: float  # 保证金率
- GeneralTickerInfoFile
  .read() 文件读取, -> Dict[Product, TickerInfoData]
- GeneralTickerInfoManager
  输入目录, 如 "./Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index
  作为在Platinum组件中使用的用于管理GeneralTickerInfo的工具
"""

import os
from dataclasses import dataclass
from typing import Dict, List
from .object import Product


@dataclass
class TickerInfoData:
    product: Product
    prefix: str  # Futures / Stock / Options / ...
    currency: str

    point_value: float  # 1张合约 的 “价值 / 报价”乘数，   value / share = price * point_value
    min_move: float  # 价格最小变动幅度；1个tick 对应的 价格变动数值
    lot_size: float  # 最少交易多少手 （的倍数），1手 是多少 张(shares)
    commission_on_rate: float  # 手续费，交易价值的比率
    commission_per_share: float  # 手续费，每张多少钱
    slippage_points: float
    flat_today_discount: float  # 平今佣金倍率。1：相同；0：不收钱；2：收2倍
    margin: float  # 保证金率


class GeneralTickerInfoFile:
    FileName = 'GeneralTickerInfo.csv'
    Header = 'Adapter,InternalProduct,Exchange,Prefix,TradingExchangeZoneIndex,Currency,' \
             'PointValue,MinMove,LotSize,ExchangeRateXxxUsd,CommissionOnRate,CommissionPerShareInXxx,' \
             'MinCommissionInXxx,MaxCommissionInXxx,StampDutyRate,' \
             'SlippagePoints,Product,FlatTodayDiscount,Margin,IsLive'

    def __init__(self):
        pass

    @classmethod
    def read(cls, p,) -> Dict[Product, TickerInfoData]:
        assert os.path.isfile(p)
        d_ticker_infos = {}

        with open(p) as f:
            l_lines = f.readlines()
        l_lines = [_.strip() for _ in l_lines if _.strip()]
        if len(l_lines) <= 1:
            return d_ticker_infos

        for line in l_lines[1:]:
            _line_split = line.split(',')
            assert len(_line_split) == 20
            _product_symbol = _line_split[16]
            _exchange = _line_split[2]
            _product = Product(symbol=_product_symbol, exchange=_exchange)
            _product_info_data = TickerInfoData(
                product=_product,
                prefix=_line_split[3],
                currency=_line_split[5],
                point_value=float(_line_split[6]),
                min_move=float(_line_split[7]),
                lot_size=float(_line_split[8]),
                commission_on_rate=float(_line_split[10]),
                commission_per_share=float(_line_split[11]),
                slippage_points=float(_line_split[15]),
                flat_today_discount=float(_line_split[17]),
                margin=float(_line_split[18]),
            )
            d_ticker_infos[_product] = _product_info_data
        return d_ticker_infos


class GeneralTickerInfoManager:
    """
    一般每个platinum工具都需要 GeneralTickerInfo 信息

    读取，
        输入目录, 如 "C:/D/_workspace/Platinum/Platinum.Ds/Release/Data", 查找该目录下的 文件夹, 文件夹名作为 time zone index
    获取，

    """

    def __init__(self, path):
        self._data = {}
        self._set(path)

    @property
    def data(self):
        return self._data.copy()

    def _set(self, path):
        assert os.path.isdir(path)
        for _name in os.listdir(path):
            path_sub = os.path.join(path, _name)
            path_gti_file = os.path.join(path_sub, 'GeneralTickerInfo.csv')
            if not os.path.isfile(path_gti_file):
                continue
            else:
                try:
                    _gti: Dict[Product, TickerInfoData] = GeneralTickerInfoFile.read(path_gti_file)
                except Exception as e:
                    raise e
                else:
                    _time_zone_index = '.'.join(_name.split('.')[1:])
                    self._data[_time_zone_index] = _gti

    def get(self, product, time_zone_index='210') -> TickerInfoData or None:
        return self._data[time_zone_index][product]

    def get_time_zone_data(self, time_zone_index='210') -> Dict[Product, TickerInfoData] or None:
        return self._data[time_zone_index]
