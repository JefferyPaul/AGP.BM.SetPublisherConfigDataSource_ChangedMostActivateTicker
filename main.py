import os
import json
import shutil
from time import sleep
from datetime import datetime
from collections import defaultdict
import shutil
import sys
import argparse
from lxml import etree
from typing import Dict, List
from copy import deepcopy


PATH_ROOT = os.path.dirname(__file__)
sys.path.append(PATH_ROOT)

from pyptools.common import Product, Ticker, BarData, GeneralTickerInfoFile, TickerInfoData
from helper.simpleLogger import MyLogger
from helper.tp_WarningBoard import run_warning_board

PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
PATH_TICKERS_FILE = os.path.join(PATH_ROOT, 'Output', 'all_using_tickers.csv')
PATH_SAMPLE = os.path.join(PATH_ROOT, 'Config', 'sample.xml')


class SetPublisherConfigDataSource:
    def __init__(
            self,
            sample_file,
            product_tickers_file,
            logger=MyLogger("SetPublisherConfigDataSource", output_root=os.path.join(PATH_ROOT, 'logs'))
    ):
        assert os.path.isfile(sample_file)
        assert os.path.isfile(product_tickers_file)

        self.sample_etree_root = self._read_sample(sample_file)
        self.product_tickers_map: Dict[str, list] = self._read_product_tickers_map(product_tickers_file)

        self.logger = logger

        self._encoding = 'gb2312'
        self._xml_declaration = True

    @staticmethod
    def _read_product_tickers_map(p) -> Dict[str, list]:
        d_product_tickers = defaultdict(list)
        with open(p) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == "":
                continue
            line_split = line.split(",")
            assert len(line_split) >= 2
            for _ in line_split[1:]:
                d_product_tickers[line_split[0]].append(_)
        return d_product_tickers

    @staticmethod
    def _read_sample(p):
        tree: etree._ElementTree = etree.parse(p)
        root: etree._Element = tree.getroot()
        return root

    def handle(self, bm_path, delete_cache=True):
        assert os.path.isdir(bm_path)
        p_strategy_files_folder = os.path.abspath(os.path.join(bm_path, 'Config', 'Strategies'))
        assert os.path.isdir(p_strategy_files_folder)
        p_strategy_bak = os.path.join(
            p_strategy_files_folder, 'bak_changing_config_tickers_%s' % datetime.now().strftime('%Y%m%d_%H%M%S'))
        self.logger.info(f'handling {bm_path}')

        # 将 config  按product 分组
        d_strategy_file_gb_product = defaultdict(list)
        for file_name in os.listdir(p_strategy_files_folder):
            # 查找 配置文件
            p_file = os.path.join(p_strategy_files_folder, file_name)
            if not os.path.isfile(p_file):
                continue
            if file_name.split('.')[-1] != 'config':
                continue
            file_product_internal_name = file_name.split('@')[0]
            product = Product.from_internal_name(file_product_internal_name)
            product_name = product.name
            d_strategy_file_gb_product[product_name].append(p_file)

        # 修改
        for product_name, l_strategy_files in d_strategy_file_gb_product.items():
            self.logger.info(product_name)
            # 查找product ticker
            if product_name not in self.product_tickers_map.keys():
                self.logger.error('找不到此Product的 Ticker列表,%s' % product_name)
                continue
            l_tickers = self.product_tickers_map[product_name]

            if len(l_strategy_files) < len(l_tickers):
                self.logger.error('需要修改的ticker数量大于trader config文件数量')
                run_warning_board('需要修改的ticker数量大于trader config文件数量')
                raise Exception

            # 修改 配置文件
            for n, p_file in enumerate(l_strategy_files):
                file_name = os.path.basename(p_file)
                if n+1 > len(l_tickers):
                    target_ticker = l_tickers[-1]
                else:
                    target_ticker = l_tickers[n]
                # 判斷是否需要修改
                tree: etree._ElementTree = etree.parse(p_file)
                root: etree._Element = tree.getroot()
                root_universe = root.find('Universe')
                l_old_universe_ticker_name = [root_universe[i].find("Name").text for i in range(len(root_universe))][0]
                if l_old_universe_ticker_name == target_ticker:
                    ticker_list_is_changed = False
                else:
                    ticker_list_is_changed = True
                if not ticker_list_is_changed:
                    # 不作修改，返回
                    continue
                # 需要修改
                # 备份
                if not os.path.isdir(p_strategy_bak):
                    os.makedirs(p_strategy_bak)
                p_file_bak = os.path.join(p_strategy_bak, file_name)
                shutil.copyfile(p_file, p_file_bak)

                #
                self.logger.info('file ticker list changed, %s' % file_name)
                root.remove(root_universe)
                root_universe = etree.Element('Universe')
                # 修改 universe
                _ticker_data_source = deepcopy(self.sample_etree_root)
                _ticker_data_source.find('Name').text = target_ticker
                _ticker_data_source.find('Type').text = "Ticker"
                _ticker_data_source.find('Mode').text = "NormalData"
                _ticker_data_source.find('Id').text = str(int(n+1))
                root_universe.append(_ticker_data_source)
                root.append(root_universe)
                # 输出
                tree_str = etree.tostring(
                    tree, pretty_print=True, xml_declaration=self._xml_declaration, encoding=self._encoding).decode()
                with open(p_file, 'w') as f:
                    f.write(tree_str)

                if delete_cache:
                    file_short_name = file_name.replace(".config", "")
                    p_cache_file = os.path.join(bm_path, 'CacheFiles', f'{file_short_name}_CacheFile.cache')
                    p_cache_file_2 = os.path.join(bm_path, 'CacheFiles', f'{file_short_name}_TradingTickers.cache')
                    self.logger.info('delete cache file')
                    if os.path.isfile(p_cache_file):
                        os.remove(p_cache_file)
                    if os.path.isfile(p_cache_file_2):
                        os.remove(p_cache_file_2)


if __name__ == '__main__':
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    l_bm_path = d_config['bm']

    obj = SetPublisherConfigDataSource(
        sample_file=PATH_SAMPLE,
        product_tickers_file=PATH_TICKERS_FILE,
    )
    for path_bm in l_bm_path:
        path_bm = os.path.abspath(path_bm)
        assert os.path.isdir(path_bm)
        obj.handle(path_bm)
