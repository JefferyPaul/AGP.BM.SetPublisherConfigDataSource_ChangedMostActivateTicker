# -*- coding: utf-8 -*-
# @Time    : 2020/12/30 19:01
# @Author  : Jeffery Paul
# @File    : apss_viewer_portfolioA.py


import os

import pandas as pd
import numpy as np

from ..apss.object import AggregatedPnlSeriesStructure, AggregatedPnlSeriesFile, AggregatedPnlSeriesData
from ..apss.object import Allocation, AllocationFile


class Viewer:
    def __init__(self, path):
        self._path = os.path.abspath(path)

        self._isfile = os.path.isfile(path)
        self._isdir = os.path.isdir(path)
        if self._isfile:
            self._name = os.path.basename(os.path.dirname(self.path))
        elif self._isdir:
            self._name = os.path.basename(self.path)
        else:
            print('输入的地址不存在')
            raise FileExistsError

    @property
    def path(self):
        return self._path

    @property
    def isfile(self):
        return self._isfile

    @property
    def isdir(self):
        return self._isdir

    @property
    def name(self):
        return self._name

    # ================================      ================================

    def show(self):
        pass

    def describe(self):
        pass

    def optimize(self):
        pass

    def portfolio(self):
        pass

    def portfolio_allocation(self, auto_init=False):
        if not self.isdir:
            print('请输入文件夹目录')
            return None

        apss = AggregatedPnlSeriesStructure(self.path)

        # 查找子文件夹的 aps
        d_sub_apsfile = {
            sub_apss.name: sub_apss.apsfile
            for sub_apss in apss.sub_apss
        }
        # Allocation.txt
        allocation_file: AllocationFile or None = apss.allocationfile
        if not allocation_file:
            # 自动 初始化 Allocation.txt
            if auto_init:
                d = {
                    name: 1
                    for name in d_sub_apsfile.keys()
                }
                d['#Scaler'] = 1
                allocation = Allocation(d)
                allocation.to_txt(path=os.path.join(apss.path, 'Allocation.txt'))
            else:
                print('不存在Allocation.txt')
                raise FileExistsError
        else:
            allocation: Allocation = allocation_file.read()
        # 检查，是否意义对应
        if set(allocation.item_name) != set(d_sub_apsfile.keys()):
            print('Allocation.txt 与 文件夹结构不一致')
            raise Exception

        # 读取 sub apsfile,
        df_sub_apsdata = pd.DataFrame({
            name: apsfile.read()
            for name, apsfile in d_sub_apsfile.items()
        })
        # 计算新aps
        series_allocation = pd.Series(allocation.scaled_allocation)
        df_sub_apsdata_muled: pd.DataFrame = df_sub_apsdata * series_allocation
        series_new_aps_data: pd.Series = df_sub_apsdata_muled.T.sum()

        new_aps = AggregatedPnlSeriesData(series_new_aps_data.to_dict())
        new_aps.to_csv(path=os.path.join(apss.path, 'AggregatedPnlSeries.csv'))


class ViewerPlot:
    def __init__(self, path):
        self._path = path

        self._isfile = os.path.isfile(path)
        self._isdir = os.path.isdir(path)
        if self._isfile:
            self._name = os.path.basename(os.path.dirname(self.path))
        elif self._isdir:
            self._name = os.path.basename(self.path)
        else:
            print('输入的地址不存在')
            raise FileExistsError

    @property
    def path(self):
        return self._path

    @property
    def isfile(self):
        return self._isfile

    @property
    def isdir(self):
        return self._isdir

    @property
    def name(self):
        return self._name
