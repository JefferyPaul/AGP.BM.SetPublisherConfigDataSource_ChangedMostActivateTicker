import os
from datetime import datetime
from collections import defaultdict
from typing import Dict, List

from ..platinum_component import PlatinumStructure


class QMStructure(PlatinumStructure):
    def __init__(self, path):
        super().__init__(path)

        # config file
        self.BrokerConfig = os.path.join(self._path, 'Config', 'BrokerClient.json')
        self.BrokerServerConnections = os.path.join(self._path, 'Config', 'BrokerServerConnections.csv')
        self.MorningCheck = os.path.join(self._path, 'Config', 'MorningCheck.csv')
        self.ScreenCapture = os.path.join(self._path, 'Config', 'ScreenCapture.json')

        # Data file
        self.Holiday = ''
        self.GeneralTickerInfo = ''
        self.TradingSession = ''

        # folder
        self.Reports = QMReports(os.path.join(self._path, 'Reports'))


class QMReports(PlatinumStructure):
    def __init__(self, path):
        super().__init__(path)

    def get_newest_reports(self) -> list:
        folders = self.get_folders()
        if not folders:
            return []
        path_newest_folder = max(folders)
        _ = []
        d = defaultdict(dict)
        for name in os.listdir(path_newest_folder):
            if name == 'MorningCheck_Account.csv':
                _.append(os.path.join(path_newest_folder, 'MorningCheck_Account.csv'))
            elif name.split('_')[0] in ['Pnl', 'Position', 'Trades'] and ('bak' not in name.lower()):
                sign, broker_id, strategy_name, dt = name.replace('.csv', '').split('_')
                if broker_id not in d[sign].keys():
                    d[sign][broker_id] = []
                d[sign][broker_id].append(os.path.join(path_newest_folder, name))
        for sign, d_sign in d.items():
            for broker_id, l_files in d_sign.items():
                _.append(max(l_files))
        return _

    def get_folders(self) -> list:
        _ = []
        for name in os.listdir(self._path):
            p = os.path.join(self._path, name)
            if not os.path.isdir(p):
                continue
            else:
                try:
                    datetime.strptime(name, '%Y%m%d')
                except:
                    continue
                else:
                    _.append(p)
        return _
