import argparse

try:
    from .MessageClient import MessageClient
except:
    from MessageClient import MessageClient


""""
    参数设置
"""

parser = argparse.ArgumentParser()
# ip
parser.add_argument('ip', type=str, help='message server ip')
# port
parser.add_argument('port', type=str, help='message server port')
# 选择运行的功能
parser.add_argument(
    "function", type=str,
    choices=['getfile', 'getmessage', 'sendfile', 'sendmessage', 'status'],
    help="running function"
)
# key
parser.add_argument('-k', '--key', type=str, help='key')
# file path ( send or get)
parser.add_argument('-a', '--arg', type=str, help='arg:file path (sendfile or getfile) or msg(sendmessage)')
# 是否使用 timestamp 模式
parser.add_argument(
    '-t', '--usetimestamp',
    help='使用timestamp模式,仅使用于(getfile, getmessage, sendfile, sendmessage)功能',
    action='store_true'
)
# timestamp gat
parser.add_argument('-g', '--gap', type=int, help='timestamp模式下，允许的最大时间(s),适用于 getfile getmessage')
# 超时时间
parser.add_argument('--timeout', type=int, help='运行超时时间(s)')
# 最大运行次数
parser.add_argument('--maxtry', type=int, help='最大尝试次数')


"""
    运行
"""
args = parser.parse_args()

mc = MessageClient(ip=args.ip, port=args.port)

kwargs = {}
if args.timeout:
    kwargs['timeout'] = args.timeout
if args.maxtry:
    kwargs['maxtry'] = args.maxtry

if args.function.lower() == 'getfile':
    key = args.key
    path = args.arg
    use_timestamp = args.usetimestamp
    if (not key) or (not path):
        print('参数不全')
        raise Exception
    if use_timestamp:
        timestamp_gap = args.gap
        if not timestamp_gap:
            print('参数不全')
            raise Exception
        mc.getfile(key=key, file_path=path, with_timestamp_gap=timestamp_gap, **kwargs)
    else:
        mc.getfile(key=key, file_path=path, **kwargs)
elif args.function.lower() == 'getmessage':
    key = args.key
    use_timestamp = args.usetimestamp
    if not key:
        print('参数不全')
        raise Exception
    if use_timestamp:
        timestamp_gap = args.gap
        if not timestamp_gap:
            print('参数不全')
            raise Exception
        mc.getmessage(key=key, with_timestamp_gap=timestamp_gap, **kwargs)
    else:
        mc.getmessage(key=key, **kwargs)
elif args.function.lower() == 'sendfile':
    key = args.key
    path = args.arg
    use_timestamp = args.usetimestamp
    if (not key) or (not path):
        print('参数不全')
        raise Exception
    if use_timestamp:
        mc.sendfile(key=key, file_path=path, with_timestamp=use_timestamp, **kwargs)
    else:
        mc.sendfile(key=key, file_path=path, **kwargs)
elif args.function.lower() == 'sendmessage':
    key = args.key
    msg = args.arg
    use_timestamp = args.usetimestamp
    if not key:
        print('参数不全')
        raise Exception
    if use_timestamp:
        mc.sendmessage(key=key, message=msg, with_timestamp=use_timestamp, **kwargs)
    else:
        mc.sendmessage(key=key, message=msg, **kwargs)
elif args.function.lower() == 'status':
    mc.status(**kwargs)
else:
    print('function输入值错误')
    raise Exception
