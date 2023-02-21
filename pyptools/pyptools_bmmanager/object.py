"""

"""

import re
import os
from typing import List
from dataclasses import dataclass
from datetime import datetime
import shutil
from lxml import etree

#
SKIPPING_STRING = ['bak', 'offline']


def _contain_skipping_string(s: str) -> bool:
    s_lower = str(s).lower()
    for _str in SKIPPING_STRING:
        if _str in s_lower:
            return True
    return False


class BMRoot:
    def __init__(self, p):
        assert os.path.isdir(p)
        self._path = os.path.abspath(p)
        self._bms: List[BrokerMini] = list()

        self._find_bm_folder()

    @property
    def path(self):
        return self._path

    @property
    def bms(self):
        return self._bms

    def _find_bm_folder(self):
        _bms = []
        for folder_name in os.listdir(self.path):
            p_folder = os.path.join(self.path, folder_name)
            if not os.path.isdir(p_folder):
                continue
            if _contain_skipping_string(folder_name):
                continue
            if not BrokerMini.parse_folder_name(folder_name):
                continue
            _bms.append(BrokerMini(p_folder))
        self._bms = _bms


class BrokerMini:
    folder_name_pattern = re.compile(r"^Broker\.(?P<BrokerId>\d{4})@(?P<FundName>.+)@(?P<StrategyName>.+)$")

    def __init__(self, p):
        assert os.path.isdir(p)
        self._path = os.path.abspath(p)
        self.FolderName = os.path.basename(p)
        assert self.parse_folder_name(self.FolderName)

        self.BrokerConfig = BrokerConfig(os.path.join(self.path, 'Config', 'Broker.config'))
        self.SubscribeCsv = BMSubscribeCsv(os.path.join(self.path, 'Config', 'Subscribe.csv'))
        self.TraderConfigList: List[BMTraderConfig] = []
        for _trader_file_name in os.listdir(os.path.join(self.path, 'Config', 'Strategies')):
            if not _contain_skipping_string(_trader_file_name):
                self.TraderConfigList.append(BMTraderConfig(
                    os.path.join(self.path, 'Config', 'Strategies', _trader_file_name)))

        self.BrokerId = self.BrokerConfig.BrokerId
        self.FundName = self.BrokerConfig.FundName
        self.StrategyName = self.BrokerConfig.StrategyName

    @property
    def path(self):
        return self._path

    @classmethod
    def parse_folder_name(cls, s) -> dict or None:
        try:
            match = cls.folder_name_pattern.match(os.path.basename(s))
            broker_id = match.group('BrokerId')
            fund_name = match.group('FundName')
            strategy_name = match.group('StrategyName')
        except Exception as e:
            return None
        else:
            return {
                "BrokerId": broker_id,
                "FundName": fund_name,
                "StrategyName": strategy_name
            }

    def __str__(self):
        return f'BrokerMini: Path={self._path}, ' \
               f'BrokerId={self.BrokerId}, FundName={self.FundName}, StrategyName={self.StrategyName}'


class BrokerConfig:
    def __init__(self, p):
        assert os.path.isfile(p)
        self.path = os.path.abspath(p)
        
        self.BrokerId = None
        self.StrategyName = None
        self.FundName = None
        self.DataPath = None
        self.Account = None
        self.read()

    def read(self):
        _tree: etree._ElementTree = etree.parse(self.path)
        _root: etree._Element = _tree.getroot()

        self.BrokerId = _root.find('BrokerId').text
        self.StrategyName = _root.find('StrategyName').text
        self.FundName = _root.find('FundName').text
        self.DataPath = _root.find('DataPath').text
        self.Account = _root.find('Accounts').find('AccountConfig').find('Account').text

    def __str__(self):
        return f'BrokerConfig: BrokerId={self.BrokerId}, FundName={self.FundName},' \
               f'Account={self.Account}, StrategyName={self.StrategyName}'


@dataclass
class BMSubscribeData:
    SubTrader: str
    PubTrader: str
    Allocation: float

    def __str__(self):
        return ','.join([self.SubTrader, self.PubTrader, str(self.Allocation)])

    def __repr__(self):
        return f'SubscribeData, SubTrader: {self.SubTrader}, PubTrader: {self.PubTrader}, ' \
               f'Allocation: {str(self.Allocation)}'


class BMSubscribeCsv:
    def __init__(self, p):
        assert os.path.isfile(p)
        self.path = os.path.abspath(p)
        self.data: List[BMSubscribeData] = []
        self.read()
    
    def read(self):
        _data = []
        with open(self.path) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            _sub, _pub, _allocation = line.split(',')
            _allocation = float(_allocation)
            _data.append(BMSubscribeData(_sub, _pub, _allocation))
        self.data = _data
            
    def write(self, bak=True):
        # 备份
        if bak:
            shutil.copyfile(
                src=self.path,
                dst=os.path.join(
                    os.path.dirname(self.path),
                    os.path.basename(self.path) + '.' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.bak'
                )
            )
        # 写
        l_lines = '\n'.join([str(_sub_data) for _sub_data in self.data])
        with open(self.path, 'w') as f:
            f.writelines(l_lines)


class BMTraderConfig:
    def __init__(self, p):
        assert os.path.isfile(p)
        self.path = os.path.abspath(p)
        self.Identity = None
        self.InitX = None
        self.TimeZoneIndex = None
        self.StrategyMode = None
        self.ExecutorUrgency = None
        self.TwapN = None
        self.UseSignalCache = None
        self.UseTwapAdjust = None
        self.EndPoint = None
        self.PreloadDays = None
        self.UsePreloadCache = None
        self.read()

    def read(self):
        _tree: etree._ElementTree = etree.parse(self.path)
        _root: etree._Element = _tree.getroot()

        self.Identity = _root.find('Identity').text
        self.InitX = float(_root.find('Params').find('InitX').text)
        self.TimeZoneIndex = _root.find('Params').find('TimeZoneIndex').text
        self.StrategyMode = _root.find('Params').find('StrategyMode').text
        if self.StrategyMode == 'Subscribe':
            self.ExecutorUrgency = _root.find('Params').find('ExecutorUrgency').text
            self.UseTwapAdjust = bool(str(_root.find('Params').find('UseTwapAdjust').text).lower() == 'true')
            self.TwapN = float(_root.find('Params').find('TwapN').text)
            self.UseSignalCache = bool(str(_root.find('Params').find('UseSignalCache').text).lower() == 'true')
        else:
            self.EndPoint = _root.find('Params').find('EndPoint').text
            self.PreloadDays = float(_root.find('Params').find('PreloadDays').text)
            self.UsePreloadCache = _root.find('Params').find('UsePreloadCache').text

    def __str__(self):
        return f'TraderConfig: Trader={self.Identity}'


@dataclass
class BMInfoData:
    Name: str = ''
    Folder: str = ''
    Portfolio: str = ''


class BMInfoFile:
    @classmethod
    def read(cls, p) -> List[BMInfoData]:
        with open(p) as f:
            l_lines = f.readlines()                   
        if len(l_lines) <= 1:
            return []
        l_infos = []
        l_header = l_lines[0].strip().split(',')
        for line in l_lines[1:]:
            line = line.strip()
            if line == '':
                continue
            values = line.split(',')
            d_k_v = {}
            for n, key in enumerate(l_header):
                if key in BMInfoData.__dict__:
                    d_k_v[key] = values[n]
            _info = BMInfoData(**d_k_v)
            l_infos.append(_info)
        return l_infos

            

