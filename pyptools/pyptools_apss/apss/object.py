# -*- coding: utf-8 -*-
# @Time    : 2020/12/30 19:23
# @Author  : Jeffery Paul
# @File    : object.py


import datetime
import os
import time
from typing import List, Dict


class AggregatedPnlSeriesData(dict):
    """
    包含日期检查，类型检查；输出csv；等功能
    { date(str): pnl(float), }
    """

    def __init__(self, *args, **kwargs):
        # super(APSData, self).__init__(*args, **kwargs)
        super(AggregatedPnlSeriesData, self).__init__()
        if args or kwargs:
            self.update(*args, **kwargs)

    # 增加属性 检验和转换
    def __setitem__(self, key, value):
        # key 检查 转换
        try:
            if isinstance(key, datetime.date):
                s_key = key.strftime('%Y%m%d')
            elif isinstance(key, datetime.datetime):
                s_key = key.strftime('%Y%m%d')
            elif isinstance(key, int):
                s_key = datetime.datetime.strptime(str(key), '%Y%m%d').strftime('%Y%m%d')
            elif isinstance(key, float):
                if int(key) - key == 0:
                    s_key = datetime.datetime.strptime(str(int(key)), '%Y%m%d').strftime('%Y%m%d')
                else:
                    raise KeyError
            elif isinstance(key, str):
                s_key = datetime.datetime.strptime(str(key), '%Y%m%d').strftime('%Y%m%d')
            else:
                raise KeyError
        except:
            print(key, value)
            raise KeyError
        # value 检查 转换
        try:
            if isinstance(value, int) or isinstance(value, float):
                f_value = value
            elif isinstance(value, str):
                f_value = float(value)
            else:
                raise ValueError
        except:
            raise ValueError
        #
        super(AggregatedPnlSeriesData, self).__setitem__(s_key, f_value)

    def update(self, another):
        for key, value in another.items():
            self.__setitem__(key, value)

    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            dict.__repr__(self)
        )

    def to_csv(self, path):
        with open(path, 'w', encoding='utf-8') as f:
            for key, value in sorted(self.items()):
                f.write('%s,%s\n' % (key, str(value)))
        print('Wrote AggregatedPnlSeries.csv: %s' % path)

    def to_list(self) -> list:
        return sorted(self.items())

    """
    1: { date: pnl, date2: pnl2, }      目标结构
    2: [(date, pnl), (date2, pnl2), (), ]      接受 
    3: [{"date": date", "pnl": pnl}, {"date": date2, "pnl": pnl2}, {}, ]
    """

    @classmethod
    def from_list(cls, data: list):
        """
        接收的数据格式：
            [(date, pnl), ]
        :return:
        """
        if not isinstance(data, list):
            raise TypeError
        # aps_obj = AggregatedPnlSeriesData()
        d = {}
        for num, n_data in enumerate(data):
            if len(n_data) != 2:
                raise ValueError
            else:
                date = n_data[0]
                pnl = n_data[1]
                # if date in aps_obj.keys():
                #     print('有重复的键: %s' % date)
                d[date] = pnl
                # aps_obj[date] = pnl
        return AggregatedPnlSeriesData(d)


class AggregatedPnlSeriesFile:
    """
    不会立刻读取文件数据，需要主动调用才读取数据，
    调用 self.read() 会（重新）读取数据
    调用 self.data 会读取数据
    """
    def __init__(self, path):
        if not os.path.isfile(path):
            raise FileExistsError
        self._path = path
        self._data = None

    @property
    def path(self):
        return self._path

    @property
    def data(self) -> AggregatedPnlSeriesData:
        if self._data is None:
            self._data = self.read()
        return self._data

    # 读取整个文件
    def read(self):
        with open(self._path, encoding='utf-8-sig') as f:
            l_lines = f.readlines()
        data_lines = []
        _last_date = ''  # 用于检查文件 日期顺序
        for line in l_lines:
            line = line.strip()
            # line = line.replace(r'\ufeff', '')  #
            if line == '':
                continue
            line_split = line.split(',')
            if len(line_split) != 2:
                print('warning AggregatedPnlSeries.csv line:%s(%s)' % (line, self._path))
                raise ValueError
            if line_split[0] < _last_date:
                print('warning AggregatedPnlSeries.csv line, wrong date:%s(%s)' % (line, self._path))
                raise ValueError
            data_lines.append((line_split[0], line_split[1]))
            _last_date = line_split[0]
        self._data = AggregatedPnlSeriesData.from_list(data=data_lines)
        return self._data

    # 返回文件最后一条数据
    def get_last_item(self) -> set or None:
        with open(self.path, encoding='utf-8-sig') as f:
            for line in f.readlines()[::-1]:
                line = line.strip()
                if line == '':
                    continue
                line_split = line.split(',')
                if len(line_split) != 2:
                    print('warning aps.csv line:%s(%s)' % (line, self._path))
                    raise ValueError
                return (
                    datetime.datetime.strptime(line_split[0], '%Y%m%d').strftime('%Y%m%d'),
                    float(line_split[1])
                )
        return None

    # 查找某一天的pnl
    # 不读取整个问题，当找到所要的数据时，便停止读取
    def get_date_pnl_fast(self, date: str, start_from_end=True) -> float or None:
        date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y%m%d')

        with open(self._path, encoding='utf-8-sig') as f:
            l_lines = f.readlines()
        if start_from_end:
            step = -1
        else:
            step = 1
        for line in l_lines[::step]:
            line = line.strip()
            if line == '':
                continue
            line_date, line_pnl = line.split(',')
            if line_date == date:
                return float(line_pnl)
        return None


class Allocation(dict):
    def __init__(self, *args, **kwargs):
        super(Allocation, self).__init__()
        if args or kwargs:
            self.update(*args, **kwargs)

    # 增加属性 检验和转换
    def __setitem__(self, key, value):
        try:
            key = str(key)
            value = float(value)
        except:
            raise ValueError
        #
        super(Allocation, self).__setitem__(key, value)

    def update(self, another):
        if '#Scaler' not in another.keys():
            print('请输入#Scaler')
            raise Exception
        for key, value in another.items():
            self.__setitem__(key, value)

    def to_txt(self, path):
        with open(path, 'w', encoding='utf-8') as f:
            for key, value in sorted(self.items()):
                if key == '#Scaler':
                    scaler = value
                    continue
                f.write('%s=%s\n' % (key, str(value)))
            f.write('%s=%s' % ('#Scaler', scaler))
        print('Wrote Allocation.txt: %s' % path)

    @classmethod
    def from_list(cls, data: list):
        if not isinstance(data, list):
            raise TypeError
        d = {}
        for num, n_data in enumerate(data):
            if len(n_data) != 2:
                raise ValueError
            else:
                key = n_data[0]
                value = n_data[1]
                d[key] = value
        return Allocation(d)

    @property
    def scaled_allocation(self) -> dict:
        scaler = self['#Scaler']
        return {
            key: value * scaler
            for key, value in self.items()
            if key != '#Scaler'
        }

    @property
    def item_name(self) -> list:
        item_name = list(self.keys())
        item_name.remove('#Scaler')
        return item_name


class AllocationFile:
    def __init__(self, path):
        if not os.path.isfile(path):
            print('不存在此文件:%s' % path)
            raise FileExistsError
        self._path = path
        self._data: None or Allocation = None

    @property
    def path(self):
        return self._path

    @property
    def data(self) -> Allocation:
        if self._data is None:
            self.read()
        return self._data

    def read(self) -> Allocation:
        d_allocation = {}
        with open(self._path) as f:
            l_lines = f.readlines()
        _has_scaler = False
        for num, line in enumerate(l_lines):
            line = line.strip()
            if line == '':
                if num != len(l_lines):
                    print('Allocation.txt 文件数据格式有误: %s' % self._path)
                    raise Exception
                else:
                    continue
            else:
                if _has_scaler:
                    print('Allocation.txt 文件数据格式有误: %s' % self._path)
                    raise Exception
                line_split = line.split('=')
                if len(line_split) != 2:
                    print('Allocation.txt 文件数据格式有误: %s' % self._path)
                    raise Exception
                key = line_split[0]
                value = line_split[1]
                d_allocation[key] = float(value)
                if key == '#Scaler':
                    _has_scaler = True
        if not _has_scaler:
            print('Allocation.txt 文件数据格式有误: %s' % self._path)
            raise Exception
        self._data = Allocation(d_allocation)
        return self._data


class AggregatedPnlSeriesStructure:
    def __init__(self, path):
        # 初始化
        self._path = os.path.abspath(path)
        self._name = os.path.basename(self._path)

        # 初始化
        self._sub_apss: List[AggregatedPnlSeriesStructure] = []
        self._sub_apss_all: List[AggregatedPnlSeriesStructure] = []

        # 检查是否为有效目录
        self._check()

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def sub_apss(self) -> list:
        if self._sub_apss is None:
            self._get_sub_apss()
        return self._sub_apss

    @property
    def sub_apss_all(self) -> list:
        if self._sub_apss_all is None:
            self._get_all_sub_apss()
        return self._sub_apss_all

    @property
    def allocationfile(self) -> AllocationFile or None:
        path = os.path.join(self.path, 'Allocation.txt')
        if os.path.isfile(path):
            return AllocationFile(path)
        else:
            return None

    @property
    def apsfile(self) -> AggregatedPnlSeriesFile or None:
        path = os.path.join(self.path, 'AggregatedPnlSeries.csv')
        if os.path.isfile(path):
            return AggregatedPnlSeriesFile(path)
        else:
            return None

    # -------------------------------       -------------------------------

    # 检查结构是否符合
    def _check(self):
        """有aps文件，或者有子文件夹"""
        if self.apsfile:
            return True
        else:
            for sub in self.sub_apss:
                try:
                    if sub._check():
                        return True
                except:
                    pass
        print('此目录并不符合APSS结构: %s' % self._path)
        raise Exception

    # 遍历子层
    def _get_sub_apss(self) -> list:
        l_sub = []
        for folder_name in os.listdir(self.path):
            path_folder = os.path.join(self.path, folder_name)
            if not os.path.isdir(path_folder):
                continue
            else:
                l_sub.append(AggregatedPnlSeriesStructure(path=path_folder))
        self._sub_apss = l_sub
        return l_sub

    # 遍历所有子层 APSS
    def _get_all_sub_apss(self) -> list:
        all_sub = []
        for num, sub_apss in enumerate(self.sub_apss):
            all_sub += sub_apss._get_all_sub_apss()
        all_sub += self.sub_apss
        self._sub_apss_all = all_sub
        return all_sub
