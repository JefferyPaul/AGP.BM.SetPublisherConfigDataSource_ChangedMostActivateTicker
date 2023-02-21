"""

"""

from urllib import parse
from typing import Dict, List
from collections import defaultdict
from enum import Enum
from datetime import datetime, date

from sqlalchemy import Column, String, Integer, Date, Float, ForeignKey, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


class OrderState(Enum):
    ordered = 1
    filled = 4
    canceled = 5
    error = 6


class Direction(Enum):
    Long = 1
    Short = -1


Base = declarative_base()  # 创建对象的基类


class Order(Base):  # 定义一个类，继承Base
    __tablename__ = 'OrderBooks'

    InternalId = Column(String(256), primary_key=True)
    ExternalId = Column(String(256))
    Account = Column(String(256))
    Trader = Column(String(256))
    Ticker = Column(String(256))
    OrderStatus = Column(Integer)
    OrderType = Column(Integer)
    Direction = Column(Integer)
    LimitPrice = Column(Float)
    Volume = Column(Float)
    TradedPrice = Column(Float)
    TradedVolume = Column(Float)
    HedgeFlag = Column(Integer)
    OffsetFlag = Column(Integer)
    CreateTime = Column(DateTime)
    UpdateTime = Column(DateTime)
    CacheTime = Column(DateTime)
    FillingTime = Column(DateTime)
    Remark = Column(String(256))
    BatchId = Column(String(64))
    IsBatchOrder = Column(String(64))

    def __repr__(self):
        return f'<Order(InternalId={self.InternalId}, ExternalId={self.ExternalId}, ' \
               f'Account={self.Account}, Trader={self.Trader}, ' \
               f'Ticker={self.Ticker}, Direction={self.Direction}, ' \
               f'LimitPrice={str(self.LimitPrice)}, Volume={str(self.Volume)}, ' \
               f'OrderStatus={self.OrderStatus}, OrderType={self.OrderType},  ' \
               f'TradedPrice={str(self.TradedPrice)}, TradedVolume={str(self.TradedVolume)},  ' \
               f'HedgeFlag={self.HedgeFlag}, OffsetFlag={self.OffsetFlag},  ' \
               f'Remark={self.Remark}, BatchId={self.BatchId}, IsBatchOrder={self.IsBatchOrder}, ' \
               f'CreateTime={str(self.CreateTime)}, UpdateTime={str(self.UpdateTime)},' \
               f'CacheTime={str(self.CacheTime)}, FillingTime={str(self.FillingTime)})>'

    def __str__(self):
        return ','.join(str(_) for _ in [
            self.InternalId, self.ExternalId,
            self.Account, self.Trader, self.Ticker, self.Direction,
            self.LimitPrice, self.Volume,
            self.OrderStatus, self.OrderType,
            str(self.TradedPrice), str(self.TradedVolume),
            self.HedgeFlag, self.OffsetFlag,
            self.Remark, self.BatchId, self.IsBatchOrder,
            str(self.CreateTime), str(self.UpdateTime),
            str(self.CacheTime), str(self.FillingTime)
        ])


class OrderLogs(Base):  # 定义一个类，继承Base
    __tablename__ = 'OrderBookLogs'

    Date = Column(String(64), primary_key=True)
    InternalId = Column(String(256), primary_key=True)
    ExternalId = Column(String(256))
    Account = Column(String(256))
    Trader = Column(String(256))
    Ticker = Column(String(256))
    OrderStatus = Column(Integer)
    OrderType = Column(Integer)
    Direction = Column(Integer)
    LimitPrice = Column(Float)
    Volume = Column(Float)
    TradedPrice = Column(Float)
    TradedVolume = Column(Float)
    HedgeFlag = Column(Integer)
    OffsetFlag = Column(Integer)
    CreateTime = Column(DateTime)
    UpdateTime = Column(DateTime)
    CacheTime = Column(DateTime)
    FillingTime = Column(DateTime)
    Remark = Column(String(256))
    BatchId = Column(String(64))
    IsBatchOrder = Column(String(64))

    def __repr__(self):
        return f'<Order(InternalId={self.InternalId}, ExternalId={self.ExternalId}, ' \
               f'Account={self.Account}, Trader={self.Trader}, ' \
               f'Ticker={self.Ticker}, Direction={self.Direction}, ' \
               f'LimitPrice={str(self.LimitPrice)}, Volume={str(self.Volume)}, ' \
               f'OrderStatus={self.OrderStatus}, OrderType={self.OrderType}, ' \
               f'TradedPrice={str(self.TradedPrice)}, TradedVolume={str(self.TradedVolume)}, ' \
               f'HedgeFlag={self.HedgeFlag}, OffsetFlag={self.OffsetFlag}, ' \
               f'Remark={self.Remark}, BatchId={self.BatchId}, IsBatchOrder={self.IsBatchOrder}, ' \
               f'CreateTime={str(self.CreateTime)}, UpdateTime={str(self.UpdateTime)},' \
               f'CacheTime={str(self.CacheTime)}, FillingTime={str(self.FillingTime)})>'


class Trade(Base):  # 定义一个类，继承Base
    __tablename__ = 'TradeBooks'

    TradeId = Column(String(256), primary_key=True)
    ExternalId = Column(String(256))
    Account = Column(String(256))
    Trader = Column(String(256))
    Ticker = Column(String(256))
    Direction = Column(Integer)
    TradedPrice = Column(Float)
    TradedVolume = Column(Float)
    Commission = Column(Float)
    CloseProfit = Column(Float)
    HedgeFlag = Column(Integer)
    OffsetFlag = Column(Integer)
    CreateTime = Column(DateTime)
    Remark = Column(String(256))
    BatchId = Column(String(64))
    CommissionAsset = Column(String(64))

    def __repr__(self):
        return f'<Trade(TradeId={self.TradeId}, ExternalId={self.ExternalId}, CreateTime={str(self.CreateTime)} ' \
               f'Account={self.Account}, Trader={self.Trader}, ' \
               f'Ticker={self.Ticker}, Direction={self.Direction}, ' \
               f'TradedPrice={str(self.TradedPrice)}, TradedVolume={str(self.TradedVolume)}, ' \
               f'Commission={str(self.Commission)}, CloseProfit={str(self.CloseProfit)}, ' \
               f'HedgeFlag={self.HedgeFlag}, OffsetFlag={self.OffsetFlag}, ' \
               f'Remark={self.Remark}, BatchId={self.BatchId}, CommissionAsset={self.CommissionAsset}'


class TradeLogs(Base):  # 定义一个类，继承Base
    __tablename__ = 'TradeBookLogs'

    Date = Column(String(64), primary_key=True)
    TradeId = Column(String(256), primary_key=True)
    ExternalId = Column(String(256))
    Account = Column(String(256))
    Trader = Column(String(256))
    Ticker = Column(String(256))
    Direction = Column(Integer)
    TradedPrice = Column(Float)
    TradedVolume = Column(Float)
    Commission = Column(Float)
    CloseProfit = Column(Float)
    HedgeFlag = Column(Integer)
    OffsetFlag = Column(Integer)
    CreateTime = Column(DateTime)
    Remark = Column(String(256))
    BatchId = Column(String(64))
    CommissionAsset = Column(String(64))

    def __repr__(self):
        return f'<Trade(TradeId={self.TradeId}, ExternalId={self.ExternalId}, CreateTime={str(self.CreateTime)} ' \
               f'Account={self.Account}, Trader={self.Trader}, ' \
               f'Ticker={self.Ticker}, Direction={self.Direction}, ' \
               f'TradedPrice={str(self.TradedPrice)}, TradedVolume={str(self.TradedVolume)}, ' \
               f'Commission={str(self.Commission)}, CloseProfit={str(self.CloseProfit)}, ' \
               f'HedgeFlag={self.HedgeFlag}, OffsetFlag={self.OffsetFlag}, ' \
               f'Remark={self.Remark}, BatchId={self.BatchId}, CommissionAsset={self.CommissionAsset}'


class TraderPosition(Base):  # 定义一个类，继承Base
    __tablename__ = 'TraderPositionBooks'

    Trader = Column(String(256), primary_key=True)
    Account = Column(String(256))
    Ticker = Column(String(256), primary_key=True)
    HedgeFlag = Column(String(256))
    LongVolume = Column(Float)
    LongVolumeToday = Column(Float)
    LongPrice = Column(Float)
    ShortVolume = Column(Float)
    ShortVolumeToday = Column(Float)
    ShortPrice = Column(Float)
    CreateTime = Column(DateTime)
    UpdateTime = Column(DateTime)

    def __repr__(self):
        return f'<TraderPosition(Account={self.Account}, Trader={self.Trader}, Ticker={self.Ticker}, ' \
               f'LongVolume={str(self.LongVolume)}, LongVolumeToday={str(self.LongVolumeToday)}, ' \
               f'LongPrice={str(self.LongPrice)}, ' \
               f'ShortVolume={str(self.ShortVolume)}, ShortVolumeToday={str(self.ShortVolumeToday)}, ' \
               f'ShortPrice={str(self.ShortPrice)}, ' \
               f'CreateTime={str(self.CreateTime)}, UpdateTime={str(self.UpdateTime)}, ' \
               f'HedgeFlag={str(self.HedgeFlag)}'


class OmsDbManagement:
    def __init__(self, db, host, user, pwd, echo=False):
        # 初始化数据库连接
        # self.PMSession = PMDbGlobal(db=db, host=host, user=user, pwd=pwd, echo=echo)
        # 初始化数据库连接
        self.engine = create_engine(
            f'mssql+pymssql://{str(user)}:{parse.quote_plus(pwd)}@{str(host)}/{str(db)}',
            echo=echo,
            max_overflow=50,    # 超过连接池大小之后，允许最大扩展连接数；
            pool_size=50,    # 连接池的大小
            pool_timeout=600,   # 连接池如果没有连接了，最长的等待时间
            pool_recycle=-1,    # 多久之后对连接池中连接进行一次回收
        )
        # 创建DBSession类
        self.DBSession = sessionmaker(bind=self.engine)
        self.session = self.DBSession()

    def close(self):
        self.session.close()

    def query_orders(self) -> List[Order]:
        return self.session.query(Order).all()

    def query_order_logs(self, ):
        return

    def query_trades(self):
        return

    def query_trade_logs(self):
        return

    def query_positions(self):
        return
