"""

"""

from datetime import datetime, date
import json

from sqlalchemy import Column, Integer, String, Float, Date, Text, Boolean, ForeignKey, PrimaryKeyConstraint, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from ..common.constant import Direction

Base = declarative_base()


class PnL(Base):
    """
    """
    __tablename__ = 'PnL'

    Trader = Column(String(128),)
    DataTime = Column(DateTime,)
    PnL = Column(Float)
    Commission = Column(Float)
    InitX = Column(Float, nullable=True)

    # 联合主键
    __table_args__ = (
        PrimaryKeyConstraint('Trader', 'DataTime'),
        {},
    )

    def __init__(
            self,
            trader: str,
            datatime: datetime,
            pnl: float,
            commission: float,
            initx: float
    ):
        assert type(trader) == str
        assert type(datatime) == datetime
        pnl = float(pnl)
        commission = float(commission)
        if initx:
            initx = float(initx)

        self.Trader = trader
        self.DataTime = datatime
        self.PnL = pnl
        self.Commission = commission
        self.InitX = initx

    def to_dict(self):
        out = {}
        for column in self.__table__.columns:
            out[column.name] = getattr(self, column.name, None)
        return out

    def to_string(self):
        return ','.join([str(i) for i in list(self.to_dict().values())])

    def __repr__(self):
        return '<QMReportPnL Trader=%s, DataTime=%s>' % (self.Trader, self.DataTime.strftime('%Y%m%d %H%M%S'))

    def __str__(self):
        return json.dumps(str(self.to_dict()), indent=4, ensure_ascii=False)


class Position(Base):
    """
    """
    __tablename__ = 'Position'

    Trader = Column(String(128), )
    DataTime = Column(DateTime, )  #
    Ticker = Column(String(128),)
    Position = Column(Float)
    LongPosition = Column(Float)
    ShortPosition = Column(Float)

    # 联合主键
    __table_args__ = (
        PrimaryKeyConstraint('Trader', 'DataTime', 'Ticker'),
        {},
    )

    def __init__(
            self,
            trader: str,
            datatime: datetime,
            ticker: str,
            position: float or None = None,
            long_position: float = 0,
            short_position: float = 0
    ):
        assert type(trader) == str
        assert type(datatime) == datetime
        if not position:
            position = long_position - short_position

        self.Trader = trader
        self.DataTime = datatime
        self.Ticker = ticker
        self.Position = position
        self.LongPosition = long_position
        self.ShortPosition = short_position

    def to_dict(self):
        out = {}
        for column in self.__table__.columns:
            out[column.name] = getattr(self, column.name, None)
        return out

    def to_string(self):
        return ','.join([str(i) for i in list(self.to_dict().values())])

    def __repr__(self):
        return '<QMReportPosition Trader=%s, Datatime=%s>' % (self.Trader, self.DataTime.strftime('%Y%m%d %H%M%S'))

    def __str__(self):
        return json.dumps(str(self.to_dict()), indent=4, ensure_ascii=False)


class Trades(Base):
    """
    """
    __tablename__ = 'Trades'

    Id = Column(String(256), primary_key=True)  # 主键
    Trader = Column(String(128), )
    DataTime = Column(DateTime, )  #
    Ticker = Column(String(128),)
    Direction = Column(String(128),)
    Volume = Column(Float)
    Price = Column(Float)
    Commission = Column(Float)

    def __init__(
            self,
            id: str,
            trader: str,
            datatime: datetime,
            ticker: str,
            direction: Direction,
            volume: float,
            price: float,
            commission: float = 0,
    ):
        assert type(trader) == str
        assert type(datatime) == datetime

        self.Id = id
        self.Trader = trader
        self.DataTime = datatime
        self.Ticker = ticker
        self.Direction = direction.name
        self.Volume = volume
        self.Price = price
        self.Commission = commission

    def to_dict(self):
        out = {}
        for column in self.__table__.columns:
            out[column.name] = getattr(self, column.name, None)
        return out

    def to_string(self):
        return ','.join([str(i) for i in list(self.to_dict().values())])

    def __repr__(self):
        return '<QMReportTrades Trader=%s, Datatime=%s, Ticker=%s>' % (
            self.Trader, self.DataTime.strftime('%Y%m%d %H%M%S'), self.Ticker)

    def __str__(self):
        return json.dumps(str(self.to_dict()), indent=4, ensure_ascii=False)
