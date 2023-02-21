import os
from datetime import datetime, date, time
from dataclasses import dataclass
from typing import List

"""
./TraderPnls.csv
./RawSignals.csv
"""


# TraderPnls.csv
@dataclass(order=True)
class TraderPnlsData:
    Date: date
    Trader: str
    Pnl: float
    Commission: float
    Slippage: float
    TradeAmount: float
    PeakMarketValue: float
    PeakHedgeValue: float
    PeakMarginValue: float


class TraderPnlsCsv:
    header = list(TraderPnlsData.__annotations__.keys())

    # 解析 TraderPnls.csv 中的一行字符，返回 TraderPnlsData 数据对象
    @classmethod
    def _parse_line_data(cls, s) -> TraderPnlsData or None:
        """
         解析 TraderPnls.csv 中的一行字符，返回 TraderPnlsData 数据对象;
         若解析出现错误，则返回None
        :param s:
        :return:
        """
        l_data = s.split(',')
        if len(l_data) != len(TraderPnlsData.__annotations__):
            return None
        try:
            _ = TraderPnlsData(
                Date=datetime.strptime(l_data[0], '%Y%m%d').date(),
                Trader=l_data[1],
                Pnl=float(l_data[2]),
                Commission=float(l_data[3]),
                Slippage=float(l_data[4]),
                TradeAmount=float(l_data[5]),
                PeakMarketValue=float(l_data[6]),
                PeakHedgeValue=float(l_data[7]),
                PeakMarginValue=float(l_data[8]),
            )
        except:
            return None
        else:
            return _

    # 读取整个 TraderPnls.csv文件，返回所有数据
    @classmethod
    def read_file(cls, p) -> List[TraderPnlsData]:
        """
        输入 TraderPnls.csv 文件路径，
        读取整个 TraderPnls.csv文件，返回所有数据
        :param p:
        :return:
        """
        assert os.path.isfile(p)
        with open(p) as f:
            l_lines = f.readlines()
        if len(l_lines) <= 1:
            return []
        l_datas = []
        for line in l_lines[1:]:
            line = line.strip()
            if line == '':
                continue
            _data: TraderPnlsData or None = cls._parse_line_data(line)
            if _data is None:
                raise ValueError
            l_datas.append(_data)
        return l_datas

    # 读取 TraderPnls.csv 文件，返回文件所有数据的日期
    @classmethod
    def get_trader_pnls_csv_dates(cls, p) -> List[date]:
        """
        输入 TraderPnls.csv 文件路径，
        读取 TraderPnls.csv 文件，返回文件所有数据的日期
        :param p:
        :return:
        """
        assert os.path.isfile(p)
        l_trader_pnls_datas: List[TraderPnlsData] = cls.read_file(p)
        return [_.Date for _ in l_trader_pnls_datas]


# RawSignals.csv
@dataclass(order=True)
class RawSignalsData:
    Date: date
    Time: time
    Trader: str
    Ticker: str
    TargetPosition: float
    Price: float
    ModifiedPosition: float
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Bid: float
    Ask: float
    TradingSession: str
    InitX: float


class RawSignalsCsv:
    header = list(RawSignalsData.__annotations__.keys())
    
    @classmethod
    def _parse_line_data(cls, s) -> RawSignalsData or None:
        """
         解析 RawSignals.csv 中的一行字符，返回 RawSignalsData 数据对象;
         若解析出现错误，则返回None
        :param s:
        :return:
        """
        s = s.strip()
        l_data = s.split(',')
        if len(l_data) != len(RawSignalsData.__annotations__):
            return None
        try:
            _ = RawSignalsData(
                Date=datetime.strptime(l_data[0], '%Y-%m-%d').date(),
                Time=datetime.strptime(l_data[1], '%H:%M:%S').time(),
                Trader=l_data[2],
                Ticker=l_data[3],
                TargetPosition=float(l_data[4]),
                Price=float(l_data[5]),
                ModifiedPosition=float(l_data[6]),
                Open=float(l_data[7]),
                High=float(l_data[8]),
                Low=float(l_data[9]),
                Close=float(l_data[10]),
                Volume=float(l_data[11]),
                Bid=float(l_data[12]),
                Ask=float(l_data[13]),
                TradingSession=l_data[14],
                InitX=float(l_data[15]),
            )
        except:
            return None
        else:
            return _

    # 读取整个 RawSignals.csv文件，返回所有数据
    @classmethod
    def read_file(cls, p) -> List[RawSignalsData]:
        """
        输入 RawSignals.csv 文件路径，
        读取整个 RawSignals.csv文件，返回所有数据
        :param p:
        :return:
        """
        assert os.path.isfile(p)
        l_datas = []
        with open(p) as f:
            f.readline()
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if line == '':
                    continue
                _data: RawSignalsData or None = cls._parse_line_data(line)
                if _data is None:
                    raise ValueError
                l_datas.append(_data)
        return l_datas

    @classmethod
    def get_first_good_signal(cls, p) -> RawSignalsData or None:
        """
        返回第一条正常的信号数据，即第一条非0，非nan信号。若没有正常信号，则返回None
        :param p:
        :return:
        """
        assert os.path.isfile(p)
        _data = None
        with open(p) as f:
            f.readline()     # 第一行为列头
            while True:
                _line = f.readline()
                _data: RawSignalsData or None = cls._parse_line_data(_line)
                if _data:
                    if _data.TargetPosition != 0:
                        break
        return _data

    @classmethod
    def get_last_n_days_signals(cls, p, n) -> (List[RawSignalsData], bool):
        """
        返回最后N天的raw signal 数据。 包含 n+1 天的第一条数据
        :param p:
        :param n:
        :return:
        """
        assert os.path.isfile(p)
        l_datas = cls.read_file(p)
        l_last_datas = []
        l_data_days = []
        for data in l_datas[::-1]:
            l_last_datas.append(data)
            if data.Date not in l_data_days:
                l_data_days.append(data.Date)
            if len(l_data_days) == n:
                return l_last_datas, True
        return l_last_datas, False

    @classmethod
    def check_data(cls, p):
        pass


# 找最新的simulation文件夹
def find_bm_simulation_sub_folder(p, exclude_fake=True, reverse=True, ) -> None or str:
    """
    输入bm路径，返回最新（或最旧）的simulation文件夹路径。
    :param p: bm文件夹路径
    :param exclude_fake: 是否跳过假的simulation文件夹
    :param reverse: 默认True，返回最新的文件夹；若False，则返回最旧的文件夹
    :return:
    """
    p = os.path.abspath(p)
    if not os.path.isdir(p):
        print(f'不存在此bm文件夹, {p}')
        return None
    p_simulation_root = os.path.join(p, 'Simulation')
    if not os.path.isdir(p_simulation_root):
        print(f'不存在此simulation文件夹, {p_simulation_root}')
        return None

    # 查找符合要求的文件夹
    l_simulation_folder = []
    for _simulation_folder_name in os.listdir(p_simulation_root):
        p_simulation_folder = os.path.join(p_simulation_root, _simulation_folder_name)
        if not os.path.isdir(p_simulation_folder):
            continue
        if exclude_fake:
            if not _simulation_folder_name.isdigit():
                continue
        try:
            _name_dt = datetime.strptime(_simulation_folder_name[:14], '%Y%m%d%H%M%S')
        except:
            continue
        else:
            l_simulation_folder.append([_name_dt, p_simulation_folder])
    # 返回最新或最旧的文件夹
    if len(l_simulation_folder) == 0:
        return None
    return sorted(l_simulation_folder, key=lambda x: x[0], reverse=reverse)[0][1]
