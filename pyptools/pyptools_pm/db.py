'''
参考:
    SQLAlchemy:
        https://docs.sqlalchemy.org/en/14/orm/loading_objects.html
        https://zhuanlan.zhihu.com/p/444930067
        https://zhuanlan.zhihu.com/p/387078089
        https://blog.csdn.net/qq_36019490/article/details/96883453
        https://blog.csdn.net/T_I_A_N_/article/details/100007144?spm=1001.2014.3001.5502
        https://www.cnblogs.com/ljhdo/p/10671273.html
        https://www.jianshu.com/p/0ad18fdd7eed

    多线程(不采用):
        https://www.cnblogs.com/kaichenkai/p/11088144.html
'''

from urllib import parse
from typing import Dict, List
from collections import defaultdict
from datetime import datetime, date

from sqlalchemy import Column, String, Integer, Date, Float, ForeignKey, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()  # 创建对象的基类


class Strategy(Base):  # 定义一个类，继承Base
    __tablename__ = 'StrategyDbo'
    Id = Column(String(200), primary_key=True)
    InitCapital = Column(Integer)
    IsOnline = Column(Integer)
    OutSampleDate = Column(Date())
    Power = Column(Integer)
    Type = Column(String(200), nullable=False)
    Name = Column(String(200), nullable=False)
    OnlineDate = Column(Date())
    Currency = Column(String(200))

    def __repr__(self):
        return f'<Strategy(Id={self.Id}, Type={self.Type}, Name={self.Name}, ' \
               f'InitCapital={self.InitCapital}, Currency={self.Currency}, ' \
               f'OutSampleDate={str(self.OutSampleDate)}, OnlineDate={str(self.OnlineDate)}, ' \
               f'Power={self.Power}, IsOnline={self.IsOnline})>'


class Trader(Base):
    __tablename__ = 'TraderDbo'
    Id = Column(String(200), primary_key=True)
    StrategyId = Column(String(200), ForeignKey('StrategyDbo.Id'))
    # 跟数据库无关，不会新增字段，只用于快速链表操作
    # 类名，backref用于反向查询
    Strategy = relationship('Strategy', backref='traders')

    def __repr__(self):
        return f'Trader(Id={self.Id}, StrategyId={self.StrategyId})>'


class TraderLog(Base):
    __tablename__ = 'TraderLogDbo'
    Date = Column(String(8), primary_key=True)
    TraderId = Column(String(200), ForeignKey('TraderDbo.Id'), primary_key=True, )
    Pnl = Column(Float, nullable=False)
    Commission = Column(Float)
    Slippage = Column(Float)
    Capital = Column(Float)

    Trader = relationship('Trader', backref='logs')

    def __repr__(self):
        return f'TraderLog(Date={str(self.Date)}, TraderId={self.TraderId}, ' \
               f'Pnl={str(self.Pnl)}, Commission={str(self.Commission)}, Slippage={str(self.Slippage)}, ' \
               f'Capital={str(self.Capital)})>'


# === === === === === ===

# class PMDbGlobal:
#     __instance = None
#     __engine = None
#
#     def __new__(cls, *args, **kwargs):
#         if not cls.__instance:
#             cls.__instance = object.__new__(cls)
#         return cls.__instance
#
#     def __init__(self, db, host, user, pwd, echo=False):
#         self._db = db
#         self._host = host
#         self._user = user
#         self._pwd = pwd
#         self._echo = echo
#         self.gen_engine()
#         # 创建DBSession类
#         # self.DBSession = sessionmaker(bind=self.engine)
#         # self.session = self.DBSession()
#
#     def gen_engine(self):
#         if not PMDbGlobal.__engine:
#             PMDbGlobal.__engine = create_engine(
#                 f'mssql+pymssql://{str(self._user)}:{parse.quote_plus(self._pwd)}@{str(self._host)}/{str(self._db)}',
#                 echo=self._echo,
#                 max_overflow=50,  # 超过连接池大小之后，允许最大扩展连接数；
#                 pool_size=50,  # 连接池的大小
#                 pool_timeout=600,  # 连接池如果没有连接了，最长的等待时间
#                 pool_recycle=-1,  # 多久之后对连接池中连接进行一次回收
#             )
#             # engine = create_engine("mysql+{driver}://{username}:{password}@{server}/{database}?charset={charset}"
#             #                        .format(driver=MYSQL_DRIVER,
#             #                                username=MYSQL_USERNAME,
#             #                                password=MYSQL_PASSWORD,
#             #                                server=MYSQL_SERVER,
#             #                                database=DB_NAME,
#             #                                charset=DB_CHARSET),
#             #                        pool_size=100,
#             #                        max_overflow=100,
#             #                        # pool_recycle=7200,
#             #                        pool_recycle=2,
#             #                        echo=False)
#             # engine.execute("SET NAMES {charset};".format(charset=DB_CHARSET))
#         return PMDbGlobal.__engine
#
#     @classmethod
#     def sql_session(cls):
#         # self.gen_engine()
#         mysql_db = sessionmaker(bind=PMDbGlobal.__engine)
#         return mysql_db()
#
#
# def sql_session(method):
#     @functools.wraps(method)
#     def wrapper(*args, **kwargs):
#         session = PMDbGlobal.sql_session()
#         return method(*args, session, **kwargs)
#     return wrapper


class PMDbManagement:
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

    def query_all_strategy(self) -> List[Strategy]:
        return self.session.query(Strategy).all()

    def query_all_trader(self) -> List[Trader]:
        return self.session.query(Trader).order_by(Trader.Id).all()

    def query_trader_pnls(self, trader_id,) -> List[TraderLog]:
        return self.session.query(TraderLog).filter(TraderLog.TraderId == trader_id).order_by(TraderLog.Date).all()

    def query_strategy_traders_pnls(self, strategy_id,) -> Dict[str, Dict[str, List[TraderLog]]]:
        """
        return {strategy_id: {trader_name: [TraderLog,], } }
        """
        _d = dict()
        _d[strategy_id] = defaultdict(list)
        # 下面方法，有一个bug，当某个trader没有pnl（trader log）时，此trader并不会出现在数据dict中，导致无法察觉此数据丢失
        # _: List[TraderLog] = self.session.query(TraderLog).join(Trader).filter(
        #     Trader.StrategyId == strategy_id).order_by(TraderLog.Date).all()
        # for _data in _:
        #     trader_name = _data.TraderId
        #     _d[strategy_id][trader_name].append(_data)

        # 先查询 trader id
        all_traders: List[Trader] = self.session.query(Trader).filter(Trader.StrategyId == strategy_id).order_by(Trader.Id).all()
        all_traders_logs: List[TraderLog] = self.session.query(TraderLog).join(Trader).filter(
            Trader.StrategyId == strategy_id).order_by(TraderLog.Date).all()
        d_all_traders_logs = defaultdict(list)
        for _trader_log in all_traders_logs:
            d_all_traders_logs[_trader_log.TraderId].append(_trader_log)
        for _trader_item in all_traders:
            _trader_id = _trader_item.Id
            if _trader_id in d_all_traders_logs:
                _d[strategy_id][_trader_id] = d_all_traders_logs[_trader_id]
            else:
                _d[strategy_id][_trader_id] = []
        return _d

    def query_strategy_trader_log_dates(self, strategy_id) -> Dict[str, Dict[str, List[date]]]:
        """
        return {strategy_id: {trader_name: [TraderLog,], } }
        """
        _d = defaultdict(dict)
        _d[strategy_id] = defaultdict(list)
        _datas: List[list] = self.session.query(TraderLog.Date, TraderLog.TraderId).join(Trader).filter(
            Trader.StrategyId == strategy_id).all()
        for _data in _datas:
            _date: date = datetime.strptime(_data[0], '%Y%m%d').date()
            _trader_name = _data[1]
            _d[strategy_id][_trader_name].append(_date)
        return _d
