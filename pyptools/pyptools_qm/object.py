"""
./Reports/Pnl_xxx.csv
./Reports/Position_xxx.csv
./Reports/Trades_xxx.csv
"""

import re
import os
from typing import List
from dataclasses import dataclass
from datetime import datetime

from ..common.object import (
    Ticker, Direction, OffsetFlag)
from ..common.object import (
    TradeData, PositionData, AccountData, TraderPnLData)


@dataclass
class QMReportPnLData:
    """

    """
    Datatime: datetime
    Trader: str
    InitX: float
    PositionProfit: float
    CloseProfit: float
    Commission: float
    NetProfit: float
    NetPnlPerX: float
    LongValue: float
    ShortValue: float
    LongShortDiff: float
    Multiplier: float

    def to_internal_structure(self) -> TraderPnLData:
        return TraderPnLData(
            datatime=self.Datatime,
            trader=self.Trader,
            pnl=float(self.NetProfit),
            commission=float(self.Commission),
            initX=float(self.InitX)
        )


@dataclass
class QMReportPositionData:
    Datatime: datetime
    Trader: str
    Ticker: str
    HedgeFlag: str
    LongPosition: float
    LongAvgPx: float
    ShortPosition: float
    ShortAvgPx: float
    LastPx: float
    SettlePx: float
    # PositionProfitLast: float    文件的列名有这个，但是数据内容中并没有。很奇怪
    # PositionProfitSettle: float
    ClosedProfit: float
    Account: str

    def to_internal_structure(self) -> List[PositionData]:
        return [
            PositionData(
                datatime=self.Datatime,
                ticker=Ticker.from_name(self.Ticker),
                direction=Direction['Long'],
                volume=self.LongPosition,
                price=self.LongAvgPx
            ),
            PositionData(
                datatime=self.Datatime,
                ticker=Ticker.from_name(self.Ticker),
                direction=Direction['Short'],
                volume=self.ShortPosition,
                price=self.ShortAvgPx
            )
        ]


@dataclass
class QMReportTradeData:
    InternalId: str
    ExternalId: str
    Account: str
    Trader: str
    Ticker: str
    Direction: str
    OffsetFlag: str
    HedgeFlag: str
    Price: float
    Volume: float
    Commission: float
    ClosedProfit: float
    Comment: str
    TradeTime: datetime
    LastTime: datetime

    def to_internal_structure(self) -> TradeData:
        return TradeData(
            datatime=self.TradeTime,
            ticker=Ticker.from_name(self.Ticker),
            direction=Direction[self.Direction],
            offset_flag=OffsetFlag[self.OffsetFlag],
            price=self.Price,
            volume=self.Volume,
            commission=self.Commission
        )


@dataclass
class QMReportAccountCheckData:
    FundName: str
    Account: str
    Balance: float
    Available: float
    RiskRation: float
    IsConnect: bool

    def to_internal_structure(self, datatime: datetime) -> AccountData:
        return AccountData(
            datatime=datatime,
            account=self.Account,
            balance=self.Balance,
            available=self.Available,
            risk_ration=self.IsConnect
        )


class QMReportBaseFile:
    name_pattern = re.compile(r"")
    has_header = True
    header_len = 1

    def __init__(self, path,
                 broker_id=None, strategy_name=None,
                 create_datetime: datetime or None = None):
        assert os.path.isfile(path)
        match = self.name_pattern.match(os.path.basename(path))
        # if match is None:
        #     print('Error in %s.__init__(), file name wrong' % self.__class__.__name__)
        #     return
        try:
            self.BrokerId = match.group('BrokerId')
            self.StrategyName = match.group('StrategyName')
            self.CreateDatetime = datetime.strptime(match.group('date') + ' ' + match.group('time'), '%Y%m%d %H%M%S')
        except Exception as e:
            # print(e)
            self.BrokerId = None
            self.StrategyName = None
            self.CreateDatetime = None
        if broker_id:
            self.BrokerId = broker_id
        if strategy_name:
            self.StrategyName = broker_id
        if create_datetime:
            self.CreateDatetime = create_datetime

        self.path = os.path.abspath(path)
        self.data = []

    def read(self) -> list:
        self.data = []
        with open(self.path, encoding='gb2312') as f:
            l_lines = f.readlines()
        if len(l_lines) == 0:
            print('Error in %s.read(), the file is empty' % self.__class__.__name__)
            return []
        if self.has_header:
            l_lines = l_lines[1:]
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            if len(line_split) != self.header_len:
                print('Error in %s.read(), wrong line : %s' % (self.__class__.__name__, line))
                return []
            try:
                self.data.append(
                    self._parse_line_data(line_split)
                )
            except Exception as e:
                print('Error in %s.read(), wrong line : %s' % (self.__class__.__name__, line))
                print(e)
                return []
        return self.data

    def _parse_line_data(self, line_split):
        # 需要实现
        return


class QMReportPnLFile(QMReportBaseFile):
    name_pattern = re.compile(r"^Pnl_(?P<BrokerId>\d{4})_(?P<StrategyName>.+)_(?P<date>\d{8})(?P<time>\d{6})\.csv$")
    has_header = True
    header_len = 11

    def _parse_line_data(self, line_split) -> QMReportPnLData:
        return QMReportPnLData(
            Datatime=self.CreateDatetime,
            Trader=line_split[0],
            InitX=float(line_split[1]),
            PositionProfit=float(line_split[2]),
            CloseProfit=float(line_split[3]),
            Commission=float(line_split[4]),
            NetProfit=float(line_split[5]),
            NetPnlPerX=float(line_split[6]),
            LongValue=float(line_split[7]),
            ShortValue=float(line_split[8]),
            LongShortDiff=float(line_split[9]),
            Multiplier=float(line_split[10]),
        )


class QMReportPositionFile(QMReportBaseFile):
    name_pattern = re.compile(
        r"^Position_(?P<BrokerId>\d{4})_(?P<StrategyName>.+)_(?P<date>\d{8})(?P<time>\d{6})\.csv$")
    has_header = True
    header_len = 11

    def _parse_line_data(self, line_split) -> QMReportPositionData:
        return QMReportPositionData(
            Datatime=self.CreateDatetime,
            Trader=line_split[0],
            Ticker=line_split[1],
            HedgeFlag=str(line_split[2]),
            LongPosition=float(line_split[3]),
            LongAvgPx=float(line_split[4]),
            ShortPosition=float(line_split[5]),
            ShortAvgPx=float(line_split[6]),
            LastPx=float(line_split[7]),
            SettlePx=float(line_split[8]),
            ClosedProfit=float(line_split[9]),
            Account=str(line_split[10]),
        )


class QMReportTradesFile(QMReportBaseFile):
    name_pattern = re.compile(r"^Trades_(?P<BrokerId>\d{4})_(?P<StrategyName>.+)_(?P<date>\d{8})(?P<time>\d{6})\.csv$")
    has_header = False
    header_len = 15

    def _parse_line_data(self, line_split) -> QMReportTradeData:
        if '.' in line_split[13]:
            trade_time = datetime.strptime(line_split[13], '%Y%m%d %H:%M:%S.%f')
        else:
            trade_time = datetime.strptime(line_split[13], '%Y%m%d %H:%M:%S')
        if '.' in line_split[14]:
            last_time = datetime.strptime(line_split[14], '%Y%m%d %H:%M:%S.%f')
        else:
            last_time = datetime.strptime(line_split[14], '%Y%m%d %H:%M:%S')
        return QMReportTradeData(
            InternalId=line_split[0],
            ExternalId=line_split[1],
            Account=line_split[2],
            Trader=line_split[3],
            Ticker=line_split[4],
            Direction=self._fix_direction(line_split[5]),
            OffsetFlag=self._fix_offset_flag(line_split[6]),
            HedgeFlag=str(line_split[7]),
            Price=float(line_split[8]),
            Volume=float(line_split[9]),
            Commission=float(line_split[10]),
            ClosedProfit=float(line_split[11]),
            Comment=str(line_split[12]),
            TradeTime=trade_time,
            LastTime=last_time,
        )

    @staticmethod
    def _fix_direction(s: str):
        if str(s).lower() in ['buy', 'long']:
            return 'Long'
        elif str(s).lower() in ['sell', 'short']:
            return 'Short'
        else:
            return s

    @staticmethod
    def _fix_offset_flag(s: str):
        if str(s).lower() == 'open':
            return 'Open'
        elif str(s).lower() == 'flat':
            return 'Flat'
        elif str(s).lower() == 'flattoday':
            return 'FlatToday'
        elif str(s).lower() == 'flathistory':
            return 'FlatHistory'
        else:
            return s


class QMReportAccountCheckFile(QMReportBaseFile):
    name_pattern = re.compile('MorningCheck_Account.csv')
    has_header = True
    header_len = 6

    def _parse_line_data(self, line_split) -> QMReportAccountCheckData:
        return QMReportAccountCheckData(
            FundName=line_split[0],
            Account=line_split[1],
            Balance=float(line_split[2]),
            Available=float(line_split[3]),
            RiskRation=float(line_split[4].strip('%')) / 100,
            IsConnect=(line_split[5] == 'Online')
        )


#
def find_newest_report(p):
    """
    查找最新的 report
    :param p:
    :return:
    """
    pass
