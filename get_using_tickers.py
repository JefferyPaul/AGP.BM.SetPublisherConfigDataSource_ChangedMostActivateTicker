"""

"""
import os
from datetime import datetime, date, timedelta
import json
from typing import List, Dict
import sys
from pprint import pprint
from collections import defaultdict

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

PATH_ROOT = os.path.dirname(__file__)
sys.path.append(PATH_ROOT)

from pyptools.MostActivateTickerDB import MostActivateTicker
from helper.simpleLogger import MyLogger
from helper.tp_WarningBoard import run_warning_board


# =========     查询数据库   ======= #

user = "sa"
pwd = "Alpha2020"
host = "192.168.1.98"
db = "DSData"
query_days = 20

# ================ #

path_checking_product_list = os.path.join(PATH_ROOT, 'Config', 'CheckingProductList.csv')
path_output_newest_tickers = os.path.join(PATH_ROOT, 'Output', 'newest_tickers.csv')
path_output_second_tickers = os.path.join(PATH_ROOT, 'Output', 'second_tickers.csv')
path_output_using_tickers = os.path.join(PATH_ROOT, 'Output', 'all_using_tickers.csv')
if not os.path.isdir(os.path.dirname(path_output_newest_tickers)):
    os.makedirs(os.path.dirname(path_output_newest_tickers))


if __name__ == '__main__':
    #
    logger = MyLogger('检查是否更换主力-远月合约', output_root=os.path.join(PATH_ROOT, 'logs'))
    logger.info('started...')
    #
    engine = create_engine(
        # echo=True参数表示连接发出的 SQL 将被记录到标准输出
        # future=True是为了方便便我们充分利用sqlalchemy2.0样式用法
        f'mssql+pymssql://{user}:{pwd}@{host}/{db}',
        echo=False,
    )
    Session = sessionmaker(bind=engine)
    session = Session()

    query_date = datetime.now() - timedelta(days=query_days)
    l_query_data: List[MostActivateTicker] = session.query(MostActivateTicker).where(
        MostActivateTicker.Date >= query_date).where(
        MostActivateTicker.Num <= 2
    ).all()

    # 读取需要获取的product
    l_querying_products = []
    with open(path_checking_product_list) as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        l_querying_products.append(line)

    # 筛选数据
    l_query_data: List[MostActivateTicker] = [_data for _data in l_query_data if _data.Product in l_querying_products]
    _l_dates = list(set([_data.Date for _data in l_query_data]))
    _l_dates.sort()
    _newest_date = _l_dates[-1]
    _second_date = _l_dates[-2]
    l_data_newest_date: List[MostActivateTicker] = [_data for _data in l_query_data if _data.Date == _newest_date]
    l_data_second_date: List[MostActivateTicker] = [_data for _data in l_query_data if _data.Date == _second_date]

    # 所有ticker
    d_using_tickers = defaultdict(list)
    d_data_newest_date = defaultdict(list)
    d_data_second_date = defaultdict(list)
    for _data in l_data_newest_date:
        d_data_newest_date[_data.Product].append(_data.Ticker)
        d_using_tickers[_data.Product].append(_data.Ticker)
    for _data in l_data_second_date:
        d_data_second_date[_data.Product].append(_data.Ticker)
        d_using_tickers[_data.Product].append(_data.Ticker)
    d_using_tickers = {
        _k: list(set(_v))
        for _k, _v in d_using_tickers.items()
    }


    # 检查是否 包含所有要求的 product
    if set(list(d_data_newest_date.keys())) != set(l_querying_products):
        logger.error(f'Error, missing product in newest date data: '
                     f'{list(d_data_newest_date.keys())}. '
                     f'{l_querying_products}')

        run_warning_board('检查是否更换主力-远月合约')
        os.system('pause')
    if set(list(d_data_second_date.keys())) != set(l_querying_products):
        logger.error(f'Error, missing product in second date data: '
                     f'{list(d_data_second_date.keys())}. '
                     f'{l_querying_products}')

        run_warning_board('检查是否更换主力-远月合约')
        os.system('pause')

    #
    # d_changed_tickers = dict()
    # for _product in l_querying_products:
    #     _newest_tickers = d_data_newest_date[_product]
    #     _second_tickers = d_data_second_date[_product]
    #     if set(_newest_tickers) == set(_second_tickers):
    #         d_changed_tickers[_product] = [max(_newest_tickers)]
    #     else:
    #         _offline_tickers = []
    #         for _ticker in _second_tickers:
    #             if _ticker not in _newest_tickers:
    #                 _offline_tickers.append(_ticker)
    #         if len(_offline_tickers) == 1:
    #             logger.info(f'Offline, {_product}, {_offline_tickers[0]}')
    #             d_changed_tickers[_product] = _offline_tickers
    #         else:
    #             logger.error(f'Error, Offline more than 1 ticker, {_product}, {_offline_tickers[0]}')
    #             run_warning_board('检查是否更换主力-远月合约')
    #             os.system('pause')
    #             d_changed_tickers[_product] = _offline_tickers

    # 输出

    _l_output = []
    for _k in sorted(list(d_using_tickers.keys())):
        _l_output.append(",".join([_k] + sorted(d_data_newest_date[_k])))
    with open(path_output_using_tickers, 'w') as f:
        f.writelines("\n".join(_l_output))

    _l_output = []
    for _k in sorted(list(d_data_newest_date.keys())):
        _l_output.append(",".join([_k] + sorted(d_data_newest_date[_k])))
    with open(path_output_newest_tickers, 'w') as f:
        f.writelines("\n".join(_l_output))

    _l_output = []
    for _k in sorted(list(d_data_second_date.keys())):
        _l_output.append(",".join([_k] + sorted(d_data_second_date[_k])))
    with open(path_output_second_tickers, 'w') as f:
        f.writelines("\n".join(_l_output))

    logger.info('Finished')
    # pprint(d_data_newest_date, indent=4)
    # pprint(d_data_second_date, indent=4)
    # pprint(d_changed_tickers, indent=4)
