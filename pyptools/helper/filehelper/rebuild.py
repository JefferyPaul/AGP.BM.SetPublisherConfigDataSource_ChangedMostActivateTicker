# -*- coding: utf-8 -*-
# @Time    : 2020/11/19 19:36
# @Author  : Jeffery Paul
# @File    : rebuild.py


import os
import time
import shutil
import json
import logging
import datetime
from collections import defaultdict


"""
    将input目录和target目录下的文件（或文件夹）视为多个独立的组件，
    将这些组件通过key联系起来，并按target目录的结构重组input目录下的组件。    
    (1)key_by_folder=False:
        将目录下的所有文件视为组件，并将文件名视为key。
    (2)key_by_folder=True:
        将目录下的所有不含有子文件夹的文件夹视为组件，并将文件夹名视为key。

    注意：
        如果input下缺少target下的组件，会报错
        目录下应该避免有重复的key，因为这样的话会无法辨别组件，只保留第一个，报错并pause

"""


# 查找目录下的 组件
# { key: path, }
def get_structure_items(
        path, key_by_folder,
        same_key_check=False,  # 是否检查相同key 并报错
        not_sub_folder=True,  # key_by_folder=True时，是否只讲 没有子文件夹的 文件夹视为组件
        need_file=True,
        _logger: None or logging.Logger = None
) -> dict:
    #
    if isinstance(_logger, logging.Logger):
        logger = _logger
    else:
        logger = logging.Logger(name=__name__)

    d_item = defaultdict(list)
    error_same_key = False
    for root, dirs, files in os.walk(path, topdown=True):
        if key_by_folder:
            # 文件夹，只将没有子文件夹的文件夹作为 key
            if not_sub_folder and dirs:
                continue
            if need_file and len(files) == 0:
                continue
            key_name = os.path.basename(root)
            if same_key_check:
                if key_name in d_item.keys():
                    error_same_key = True
                    logger.error('发现相同的key: %s, %s' % (d_item[key_name], root))
                    continue
            d_item[key_name].append(root)
        else:
            # 文件
            for file in files:
                key_name = file
                if same_key_check:
                    if key_name in d_item.keys():
                        error_same_key = True
                        logger.error('发现相同的key: %s, %s' % (d_item[key_name], os.path.join(root, file)))
                        continue
                d_item[key_name].append(os.path.join(root, file))
    if error_same_key:
        logger.error('存在重复键')
        raise Exception
    return d_item


def rebuild_structure(
        input_root, output_root, path_target,
        key_by_folder, not_sub_folder=True, need_file=True,
        _logger: None or logging.Logger = None
):
    """
    input中项目必须唯一，target中可以多个——一对多
    :param input_root:  输入的结构
    :param output_root: 输出的路径
    :param path_target: 目标结构
    :param key_by_folder:  是否按文件夹重构，或文件
    :param not_sub_folder: 是否允许包含子文件夹
    :param need_file: 是否允许空文件夹
    :param _logger:
    :return:
    """
    if isinstance(_logger, logging.Logger):
        logger = _logger
    else:
        logger = logging.Logger(name=__name__)

    # 遍历input目录，收集input下的组件
    d_input_items = get_structure_items(
        path=input_root,
        key_by_folder=key_by_folder,
        same_key_check=True,        # input 目录，需要唯一
        not_sub_folder=not_sub_folder,
        need_file=need_file
    )
    d_target_items = get_structure_items(
        path=path_target,
        key_by_folder=key_by_folder,
        same_key_check=False,       # target 目录，可以重复
        not_sub_folder=not_sub_folder,
        need_file=need_file
    )

    # 重建
    # 遍历 目标
    _error = False
    for key_name, l_target_item_path in d_target_items.items():
        # input中缺少 target的内容
        if key_name not in d_input_items:
            logger.warning('"input"路径缺少此组件: %s, | %s' % (
                key_name, ','.join([os.path.relpath(path, start=path_target) for path in l_target_item_path])))
            _error = True
        else:
            for target_item_path in l_target_item_path:
                input_item_path = d_input_items[key_name][0]
                target_item_relpath = os.path.relpath(target_item_path, start=path_target)
                output_item_path = os.path.join(output_root, target_item_relpath)
                if key_by_folder:
                    # 拷贝文件夹
                    if os.path.isdir(output_item_path):
                        shutil.rmtree(output_item_path)
                        # time.sleep(0.0001)
                    shutil.copytree(src=input_item_path, dst=output_item_path)
                else:
                    # 拷贝文件
                    os.makedirs(os.path.dirname(output_item_path))
                    shutil.copyfile(src=input_item_path, dst=output_item_path)
    if _error:
        raise Exception
