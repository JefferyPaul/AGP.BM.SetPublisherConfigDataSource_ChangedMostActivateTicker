"""
定位梳理
    不提供tcp服务,
    主要用于读取DS目录下的文件，例如bar mostAct GeneralTickerInfo，
    为一些简单功能的实现，提供基础的功能服务


功能
    数据检查
        Bar数据检查,
            根据mostAct检查最活跃合约;
            分钟数据是否有缺失,OHLCV检查;
        计算MostAct,
        盘中Tick数据接收监控,
    数据获取
        指定交易所获取品种
        指定品种获取合约
        指定品种/合约获取单日/日期区间的bar/tick数据

其他
    对于 DS(bar/tick)数据文件的管理，有两种思路，
        1 查询触发。每次需要时，重新查询一遍。方便，准确；但效率低
        2 缓存管理。定时、或主动进行扫描，信息存档在缓存。难于管理；但效率高

#TODO 重新做一个全天候的基于tcp的DS服务

"""

import os
from datetime import datetime, date, time, timedelta
from typing import Dict, List
from collections import defaultdict
import logging

from ..common.object import (
    Product, Ticker,
    BarData, TickData,
    HolidayFile
)
from ..common.constant import AllMinuteTime, BarDataMode
from ..common.common_util import (gen_date_range, gen_list_diff)
from ..common.trading_session import TradingSessionDataSet, TradingSessionData, TradingSessionManager
from ..common.general_ticker_info import GeneralTickerInfoFile, GeneralTickerInfoManager, TickerInfoData
from .most_activate_ticker import MostActivateTickerInfo, MostActivateTickerFile, MostActivateTickerManager


class DSManager:
    """
    1) 提供基础数据信息
        general_ticker_infos_manager;  ProductInfoData
        trading_session_infos_manager;
        most_activate_ticker_infos; 最活跃合约; Dict[Product, List[MostActivateTickerInfo]]
        holiday_infos; 按交易所分类; Dict[str, List[date]];

    2) 提供基本的数据读取方法
        基础方法:
            1) 获取最活跃合约/复权因子
                get_product_mat(),
                    通过 MostActivateTickerManager 实现
            2) 获取交易日（可考虑假期）
                _gen_trading_dates()
            3) 获取数据/数据文件
                (Ticker, date)
                (Product, date)
                (Exchange, date)
                (date)


        获取数据:
            1) 获取数据文件路径
                ge_bar_data_file()
            2) 获取bar数据, List[BarData]
                get_bar_data()
                数据类型 ({Type}_{Mode}) :
                    1) Ticker_NormalData
                    2) Product_NormalData
                    3) Product_BackAdjustedData




    """

    PrefixFolderName = ["Futures", "Bonds", "Commodities", "Funds", "Indices", "Options", "Repos", "Stocks"]
    BarDataFolderRelpath = 'BarData/60'
    TickDataFolderName = 'TickData'
    DataFolderName = 'Date'
    ReleaseDataFolderRelpath = 'Release/Data'
    MostActivateTickersFileRelpath = 'Data/MostActiveTickers.csv'
    HolidayFileRelpath = 'Release/Data/Holidays.csv'
    DefaultTradingSessionFolderRelpath = 'Release/Data/China.210'

    #
    _instances = {}

    def __new__(cls, root: str):
        """同一个DS目录，只能有1个实例"""
        if root in cls._instances.keys():
            pass
        else:
            _instance = super().__new__(cls)
            cls._instances[root] = _instance
        return cls._instances[root]

    def __init__(self, root, logger=logging.Logger('DSManager')):
        assert os.path.isdir(root)
        self._root = root
        self.logger = logger

        # 配置文件路径
        self._most_activate_ticker_file = os.path.join(self._root, self.MostActivateTickersFileRelpath)
        self._holiday_file = os.path.join(self._root, self.HolidayFileRelpath)
        assert os.path.isfile(self._most_activate_ticker_file)
        assert os.path.isfile(self._holiday_file)

        if os.path.isdir(os.path.join(self._root, 'Release', 'Data')):
            self._release_data_folder = os.path.join(self._root, 'Release', 'Data')
        elif os.path.isdir(os.path.join(self._root, 'Debug', 'Data')):
            self._release_data_folder = os.path.join(self._root, 'Debug', 'Data')
        else:
            raise NotADirectoryError

        # 初始化
        # 假期信息
        self._holiday_infos: Dict[str, List[date]] = self._read_holiday_file()
        # 主力合约、复权因子
        self.most_activate_tickers_manager = MostActivateTickerManager(self._most_activate_ticker_file)
        # 合约基本信息
        self.general_ticker_info_manager = GeneralTickerInfoManager(self._release_data_folder)
        # 交易时间
        self.trading_session_manager = TradingSessionManager(self._release_data_folder)

        # bar 数据文件路径
        self.bar_data_files = {}

    def _read_holiday_file(self):
        _d = defaultdict(list)
        with open(self._holiday_file) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            _exchange, _date = line.split(',')
            _date = datetime.strptime(_date, '%Y/%m/%d').date()
            _d[_exchange].append(_date)
        return _d

    # 基础方法-获取数据/数据文件
    # (1) ticker
    def _get_ticker_bar_data_file(self, ticker: Ticker, query_date: date) -> str or None:
        """获取某个ticker某一天的 bar 数据文件路径"""
        p_file = None
        # 查找文件
        for _prefix in self.PrefixFolderName:
            _file = os.path.join(
                self._root, self.BarDataFolderRelpath, _prefix,
                query_date.strftime('%Y%m%d'),
                ticker.name + '.csv'
            )
            if os.path.isfile(_file):
                p_file = _file
                break
        return p_file

    # (2) product
    def _get_product_bar_data_file(self, product: Product, query_date: date) -> List[str]:
        """"""
        l_p_file = []
        # 查找文件
        for _prefix in self.PrefixFolderName:
            d_ticker_files: Dict[Ticker, str] = self._get_date_bar_data_file(query_date, _prefix)
            for _ticker in d_ticker_files:
                if _ticker.product == product:
                    l_p_file.append(d_ticker_files[_ticker])
            if l_p_file:
                break
        return l_p_file

    # (3) exchange
    def _get_exchange_bar_data_file(self, exchange: str, query_date: date) -> List[str]:
        l_files = []
        for _prefix in self.PrefixFolderName:
            d_ticker_files: Dict[Ticker, str] = self._get_date_bar_data_file(query_date, _prefix)
            for _ticker in d_ticker_files:
                if _ticker.exchange == exchange:
                    l_files.append(d_ticker_files[_ticker])
        return l_files

    # 获取某一天的所有bar文件
    def _get_date_bar_data_file(self, query_date: date, prefix: str or None = None) -> Dict[Ticker, str]:
        _checking_prefix = []
        if prefix:
            if prefix in self.PrefixFolderName:
                _checking_prefix.append(prefix)
        if not _checking_prefix:
            _checking_prefix = self.PrefixFolderName

        d_ticker_files = {}
        for _prefix in _checking_prefix:
            _path_date = os.path.join(
                self._root, self.BarDataFolderRelpath, _prefix,
                query_date.strftime('%Y%m%d'),
            )
            if not os.path.isdir(_path_date):
                continue
            for ticker_file_name in os.listdir(_path_date):
                _ticker_name = ticker_file_name.replace('.csv', '')
                _ticker = Ticker.from_name(_ticker_name)
                _ticker_file = os.path.join(_path_date, ticker_file_name)
                d_ticker_files[_ticker] = _ticker_file
        return d_ticker_files

    #
    def get_product_mat(self, product: Product, query_date: date) -> Ticker:
        return self.most_activate_tickers_manager.get_a_most_activate_ticker(product, query_date)

    # 交易日列表
    def _gen_trading_dates(
            self, symbol: Ticker or Product, start: date, end: date or None, using_holiday=True) -> List[date]:
        """
        处理日期和假期，返回交易日
        :param symbol:
        :param start:
        :param end:
        :param using_holiday:
        :return:
        """
        # holiday 处理
        _holidays = []
        if not using_holiday:
            _holidays = []
        else:
            holiday: Dict[str, List[date]] = self._holiday_infos.copy()
            if symbol.exchange in holiday.keys():
                _holidays: List[date] = holiday[symbol.exchange]
            else:
                _holidays: List[date] = holiday.get('SHFE')
        # 查询日期
        if end:
            l_dates: List[date] = gen_date_range(start, end)
        else:
            l_dates: List[date] = [start]
        # 剔除 holiday
        if _holidays:
            l_dates: List[date] = gen_list_diff(l_dates, _holidays)
        return l_dates

    # 交易日
    def _gen_exchange_trading_dates(
            self, exchange: str, start: date, end: date or None, using_holiday=True):
        """
        将 ticker/product 和 exchange区分开。
            对于holiday，目前系统是只按exchange区分，所以可以合并两个方法；
            但是考虑到更严格而言，应该是按照 product、ticker区分的，所以还是用2个方法来区分开。
        :param exchange:
        :param start:
        :param end:
        :param using_holiday:
        :return:
        """
        _holidays = []
        if not using_holiday:
            _holidays = []
        else:
            holiday: Dict[str, List[date]] = self._holiday_infos.copy()
            if exchange in holiday.keys():
                _holidays: List[date] = holiday[exchange]
            else:
                _holidays: List[date] = holiday.get('SHFE')
        # 查询日期
        if end:
            l_dates: List[date] = gen_date_range(start, end)
        else:
            l_dates: List[date] = [start]
        # 剔除 holiday
        if _holidays:
            l_dates: List[date] = gen_list_diff(l_dates, _holidays)
        return l_dates

    # 获取bar文件，exchange
    def get_bar_data_file_in_exchange(
            self,
            exchange: str,
            start: date, end: date or None,
            using_holiday: bool = True
    ) -> Dict[date, list]:
        """返回文件路径"""
        _d_result = defaultdict(list)
        # 获取查询日期
        l_query_dates = self._gen_exchange_trading_dates(exchange, start, end, using_holiday)
        for query_date in l_query_dates:
            _l_file: List[str] = self._get_exchange_bar_data_file(exchange, query_date)
            _d_result[query_date] = _l_file
        return _d_result

    # 获取bar文件，ticker or product
    def get_bar_data_file(
            self,
            symbol: Ticker or Product,
            start: date, end: date or None,
            mode: BarDataMode = BarDataMode.NormalData,
            using_holiday: bool = True
    ) -> Dict[date, list]:
        """返回文件路径"""
        _d_result = defaultdict(list)
        # 获取查询日期
        l_query_dates = self._gen_trading_dates(symbol, start, end, using_holiday)

        if type(symbol) is Ticker:
            for query_date in l_query_dates:
                _file: str or None = self._get_ticker_bar_data_file(symbol, query_date)
                if not _file:
                    _d_result[query_date] = []
                else:
                    _d_result[query_date].append(_file)
        elif type(symbol) is Product:
            if mode == BarDataMode.NormalData:
                for query_date in l_query_dates:
                    _file_list: List[str] = self._get_product_bar_data_file(symbol, query_date)
                    _d_result[query_date] = _file_list
            elif mode == BarDataMode.BackAdjustedData:
                for query_date in l_query_dates:
                    # 查找主力合约
                    _mat: Ticker = self.get_product_mat(symbol, query_date)
                    if _mat:
                        _file: str or None = self._get_ticker_bar_data_file(_mat, query_date)
                    else:
                        _file = None
                    if not _file:
                        _d_result[query_date] = []
                    else:
                        _d_result[query_date].append(_file)
        return _d_result

    # 获取 bar 数据
    def get_bar_data(
            self,
            symbol: Ticker or Product,
            start: date, end: date or None,
            mode: BarDataMode = BarDataMode.NormalData,
            using_holiday: bool = True
    ) -> List[List[BarData]]:
        """读取并返回BarData"""
        # TODO 未完成
        # holiday 处理
        # 查询日期
        l_query_dates = self._gen_trading_dates(symbol, start, end, using_holiday)

        #
        d_files: Dict[date, list] = self.get_bar_data_file(symbol, start, end, mode, using_holiday)
        # 获取数据
        l_data = []
        if type(symbol) is Ticker:
            for _date, _files in d_files:
                for _file in _files:
                    l_data.append(self._read_a_bar_file(_file))
        elif type(symbol) is Product:
            if mode == BarDataMode.NormalData:
                for _date, _files in d_files:
                    for _file in _files:
                        l_data.append(self._read_a_bar_file(_file))
            elif mode == BarDataMode.BackAdjustedData:
                pass

    @staticmethod
    def _read_a_bar_file(file) -> List[BarData]:
        file_name = os.path.basename(file)
        s_date = os.path.basename(os.path.dirname(file))
        ticker_name = file_name[:-4]
        _ticker = Ticker.from_name(ticker_name)
        _date = datetime.strptime(s_date, '%Y%m%d').date()

        _l_data = []
        with open(file) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            if len(line_split) != 8:
                print(f'Bar数据文件错误, {file}, {line}')
                raise ValueError
            _l_data.append(BarData(
                ticker=_ticker,
                date=_date,
                time=datetime.strptime(line_split[0], '%H:%M:%S').time(),
                open=float(line_split[1]),
                high=float(line_split[2]),
                low=float(line_split[3]),
                close=float(line_split[4]),
                volume=float(line_split[5]),
                price=float(line_split[6]),
                open_interest=float(line_split[7]),
            ))
        return _l_data



    def _get_product_mat_bar(self, product: Product, query_date: date, _baj=False):
        """
        获取product的bar数据; 两种方式: 最活跃合约(不进行baj) / 全部合约
        :param product:
        :param query_date:
        :param _in_most_act:
        :return:
        """
        pass


class DSChecker:
    """
        1) Bar数据检查,
            分钟数据是否有缺失,OHLCV检查;
                MostActivate
                TradingSession
        2) Tick数据盘中监控,
    """

    def __init__(self, root, logger=logging.Logger('DSChecker')):
        self.ds_manager = DSManager(root, logger=logger)
        self.logger = logger

    def _check_bar_data(self, data: List[BarData]):
        for _a_bar in data:
            pass

    def get_most_activate_ticker_in_exchange(self, tdate: date, products: List[Product]):
        for _product in products:
            # 获取 最活跃合约
            _mat: Ticker = self.ds_manager.get_a_most_activate_ticker(_product, tdate)
            if not _mat:
                self.logger.warning(f'找不到此Product的MostActivateTicker, {_product.name}, {tdate.strftime("%Y%m%d")}')
                continue

    def check_ticker_bar(self, tdate: date, tickers: List[Ticker]):
        _default_timezone = '210'
        _trading_session_table = self.ds_manager.trading_session_manager
        for _ticker in tickers:
            # 读取 bar数据
            _bar_data: List[BarData] = self.ds_manager._get_ticker_bar(ticker=_ticker, query_date=tdate)
            if not _bar_data:
                self.logger.warning(f'找不到此Ticker的Bar数据, {_ticker.name}, {tdate.strftime("%Y%m%d")}')
                continue
            # 检查数据
            # 获取trading session
            _ticker_trading_session = _trading_session_table.get(_default_timezone, _ticker.product, tdate)
            # check1 连续

    @staticmethod
    def _check_trading_session(
            data: List[BarData], trading_session_data: TradingSessionData) -> List[time]:
        _data_times: List[time] = [_.time for _ in data]        # 所有bar 时间
        _data_times.sort()
        _checking_time_n = -1       # 用于遍历 需要检查的 交易时间
        _data_time_n = 0            # 用于遍历 所有bar的 数据时间
        l_losing_time = []      # 存储缺少的时间
        # 根据trading session 指定的时间区间进行检查
        for _a_session in trading_session_data.TradingSession:
            _session_start: time = _a_session[0]
            _session_end: time = _a_session[1]
            while True:
                # 需要检查的 交易时间
                _checking_time_n += 1
                _checking_time = AllMinuteTime[_checking_time_n]
                if _checking_time < _session_start:
                    continue
                if _checking_time > _session_end:
                    break
                # 看看bar数据中有没有此时间
                while True:
                    _data_time = _data_times[_data_time_n]
                    if _data_time < _checking_time:
                        _data_time_n += 1
                        continue
                    elif _data_time == _checking_time:
                        # 存在此时间的数据
                        break
                    else:
                        # 不存在此时间的数据
                        l_losing_time.append(_checking_time)
                        break
        return l_losing_time



