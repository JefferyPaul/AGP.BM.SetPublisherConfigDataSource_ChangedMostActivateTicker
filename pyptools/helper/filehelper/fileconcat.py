""""""
import shutil

"""

"""

import os
import datetime
from collections import namedtuple, defaultdict
from typing import List, Dict
import logging

# from .csvreader import HeaderCsvReader


FileRelpathInfo = namedtuple(
    'FileRelpathInfo',
    ['path', 'relpath', 'foldername', 'filename']
)


class FileMatch:
    def __init__(
            self,
            paths: list,
            match_method='filename',
            exist: bool = False,
    ):
        """
        :param paths:
        :param match_method: str {'relpath', 'filename', 'foldername'}
        """
        self._paths = paths
        self._match_method = match_method
        self._exist: bool = exist

    @classmethod
    def _get_file_path_info(cls, path) -> List[FileRelpathInfo]:
        """
        """
        l_infos = []
        for root, dirs, files in os.walk(path, topdown=True):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                file_relpath = os.path.relpath(path=file_path, start=path)
                file_folder = os.path.basename(root)
                l_infos.append(
                    FileRelpathInfo(
                        path=file_path,
                        relpath=file_relpath,
                        foldername=file_folder,
                        filename=file_name
                    )
                )
        return l_infos

    def gen_match(self) -> (Dict[str, Dict[str, list]], bool):
        """

        :return:
        """
        d_infos = dict()
        for path in self._paths:
            d_infos[path] = self._get_file_path_info(path)

        # 生成配对
        d_match = defaultdict(lambda: defaultdict(list))
        if self._match_method == 'filename':
            for root_path, infos_list in d_infos.items():
                for file_info in infos_list:
                    d_match[file_info.filename][root_path].append(file_info)
        elif self._match_method == 'foldername':
            for root_path, infos_list in d_infos.items():
                for file_info in infos_list:
                    d_match[file_info.foldername][root_path].append(file_info)
        elif self._match_method == 'relpath':
            for root_path, infos_list in d_infos.items():
                for file_info in infos_list:
                    d_match[file_info.relpath][root_path].append(file_info)

        # 必须 是否唯一
        error = False
        for key, _ in d_match.items():
            for root_name, l_file_path in _.items():
                if len(l_file_path) > 1:
                    error = True
                    print('文件匹配，不唯一: %s' % '; '.join(l_file_path))

        # 是否存在
        if self._exist:
            for key, _ in d_match.items():
                for root in self._paths:
                    if root not in _.keys():
                        error = True
                        print('文件匹配，不存在： %s, %s' % (root, key))

        if error:
            return d_match, True
        else:
            return d_match, False


class DataFileConcator:
    CONCAT_METHOD_OPTIONS = ['base', 'insert', 'all']
    MATCH_METHOD_OPTIONS = ['relpath', 'filename', 'foldername']

    def __init__(
            self,
            path_base_folder,
            path_insert_folder,
            path_output,
            logger: logging.Logger = logging.Logger(name='FileConcat')
    ):
        """
        支持扩张不同模式
        """
        self._base_path = os.path.abspath(path_base_folder)
        self._insert_path = os.path.abspath(path_insert_folder)
        self._output_path = os.path.abspath(path_output)
        self._logger = logger

    """
    如何匹配文件：
    1 必须相同路径
    2 底层文件夹名字
    3 文件名字
    
    如何合并：
    1 A + B
    2 A + B剔除A部分
    3 A剔除B部分 + B    
    
    合并点：
    1 尾部
    2 头部
    """

    def concat(
            self,
            data_key_num: int,
            has_header=True,
            sort_by_key=True,
            data_sperator: str = ",",
            match_method="foldername",  # 文件匹配方式，
            concat_method="base",       # 合并方法
    ) -> dict:
        """

        :param match_method: str {'relpath', 'filename', 'foldername'}
        :param concat_method: str {'base', 'insert', 'all'}
        :return:

        match_method : str {'relpath', 'filename', 'foldername'}

        """

        def _read_file_data(p) -> (Dict[str, str], str) or (None, None):
            with open(p) as f:
                l_lines = f.readlines()
            if len(l_lines) < 1:
                return None, None
            header = ''
            if has_header:
                header = l_lines[0]
                l_lines = l_lines[1:]

            d_data = {}
            for line in l_lines:
                line = line.strip()
                if line == '':
                    continue
                _k = line.split(data_sperator)[data_key_num-1]
                d_data[_k] = line
            return d_data, header

        if concat_method not in self.CONCAT_METHOD_OPTIONS:
            print('FileConcator Error, arg "concat_method" not in %s' % self.CONCAT_METHOD_OPTIONS)
            raise Exception
        if match_method not in self.MATCH_METHOD_OPTIONS:
            print('FileConcator Error, arg "match_method" not in %s' % self.MATCH_METHOD_OPTIONS)
            raise Exception

        # 【1】查找文件
        # d_match: { match_method_A: { rootA: [fileA, fileB], rootB: [fileA, fielB], }, }
        d_match, is_error = FileMatch(
            paths=[self._base_path, self._insert_path],
            match_method=match_method,
        ).gen_match()
        # 不唯一
        if is_error:
            self._logger.error('文件匹配不唯一')
            self._logger.info('暂停,请查看')
            os.system('pause')

        # 【2】遍历 读取 文件
        file_last_key = {}
        for key, match_group in d_match.items():
            # 2个文件的信息
            if self._base_path not in match_group.keys():
                self._logger.warning(f'此Key文件只存在于 insert 中，不存在于 base 中: {key}')
                continue
            base_file_info: FileRelpathInfo = match_group[self._base_path][0]
            d_base_file_line, header = _read_file_data(base_file_info.path)
            if not d_base_file_line:
                self._logger.error('文件读取失败 %s' % base_file_info.path)
                continue

            if self._insert_path not in match_group.keys():
                self._logger.warning(f'此Key文件只存在于 base 中，不存在于 insert 中: {key}')
                # continue
                d_insert_file_line = {}
            else:
                insert_file_info: FileRelpathInfo = match_group[self._insert_path][0]
                d_insert_file_line, _ = _read_file_data(insert_file_info.path)
                if not d_insert_file_line:
                    self._logger.error('文件读取失败: %s' % insert_file_info.path)
                    # continue

            # 【3】合并 / 输出
            path_file_output = os.path.join(self._output_path, base_file_info.relpath)
            if not os.path.isdir(os.path.dirname(path_file_output)):
                os.makedirs(os.path.dirname(path_file_output))
            if concat_method == 'base' or concat_method == 'insert':
                if concat_method == 'base':
                    base_key = list(d_base_file_line.keys()).copy()
                    if d_insert_file_line:
                        for k, v in d_insert_file_line.items():
                            if k not in base_key:
                                d_base_file_line[k] = v
                elif concat_method == 'insert':
                    d_base_file_line.update(d_insert_file_line)

                # 输出
                if sort_by_key:
                    _keys = sorted(d_base_file_line)
                else:
                    _keys = d_base_file_line.keys()
                l_output_lines = []
                if header:
                    l_output_lines.append(header + '\n')
                for k in _keys:
                    l_output_lines.append(d_base_file_line[k] + '\n')
                with open(path_file_output, 'w', encoding='utf-8') as f:
                    f.writelines(l_output_lines)
                file_last_key[path_file_output] = max(_keys)
            elif concat_method == 'all':
                l_output_lines = []
                if header:
                    l_output_lines.append(header + '\n')

                if sort_by_key:
                    _keys_base = sorted(d_base_file_line)
                    _keys_insert = sorted(d_insert_file_line)

                else:
                    _keys_base = d_base_file_line.keys()
                    _keys_insert = d_insert_file_line.keys()
                for k in _keys_base:
                    l_output_lines.append(d_base_file_line[k] + '\n')
                for k in _keys_insert:
                    l_output_lines.append(d_insert_file_line[k] + '\n')
                with open(path_file_output, 'w', encoding='utf-8') as f:
                    f.writelines(l_output_lines)
                file_last_key[path_file_output] = max(max(_keys_base, _keys_insert))
        return file_last_key

