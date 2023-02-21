import os
from datetime import datetime, date, time, timedelta
from typing import List, Dict


def readlines_reverse(p):
    """
    从末端开始，逐行读取文件。
    跳过 \r\n 等换行符；
    :param p:
    :return:
    """
    with open(p, ) as f:
        f.seek(0, os.SEEK_END)
        position = f.tell()
        line = ''
        while position >= 0:
            f.seek(position)
            next_char = f.read(1)
            position -= 1
            if next_char.strip() == '':
                if len(line) > 0:
                    yield line[::-1].strip()
                    line = ''
            else:
                line += next_char
        yield line[::-1].strip()


def read_last_line(p) -> str:
    """
    读取大文件的最后一行，非空行
    :param p:
    :return:
    """
    with open(p, 'rb') as f:
        off = -500
        while True:
            f.seek(off, 2)  # seek(off, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
            lines = f.readlines()  # 读取文件指针范围内所有行
            lines = [_.strip() for _ in lines if _.strip()]  # 跳过空行
            if len(lines) >= 2:  # 判断是否最后至少有两行，这样保证了最后一行是完整的
                last_line = lines[-1]  # 取最后一行
                break
            off *= 2
        return last_line.decode('utf-8')


def gen_date_range(s: date, e: date) -> List[date]:
    l_date = []
    while True:
        if s <= e:
            l_date.append(s)
        else:
            break
        s += timedelta(days=1)
    return l_date


def gen_list_diff(l1, l2) -> list:
    """
    可排序的数据列表，
    返回 l1里不存在于l2的数据,

    当数据量大于 10**4 时，差异显现；
    当数据量大于 10**5 时，旧方法耗时非常明显；
    当数据量大于 10**6 时，旧方法几乎难以进行；
    :param l1:
    :param l2:
    :return:
    """
    l1 = l1.copy()
    l2 = l2.copy()
    l1.sort()
    l2.sort()
    l3 = []
    for i in l1:
        if len(l2) == 0:
            l3.append(i)
        else:
            while len(l2) > 0:
                if l2[0] > i:
                    l3.append(i)
                    break
                elif l2[0] == i:
                    break
                else:
                    l2.pop(0)
            if len(l2) == 0:
                l3.append(i)
    return l3
