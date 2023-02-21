from datetime import datetime
import os
import subprocess
from enum import Enum
from dataclasses import dataclass

import logging
try:
    from .simpleLogger import MyLogger
except:
    from simpleLogger import MyLogger


PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
PATH_MC = os.path.join(PATH_ROOT, 'TradingPlatform.MessageClient')
PATH_MC_EXE = os.path.join(PATH_MC, 'TradingPlatform.MessageClient.exe')
assert os.path.isfile(PATH_MC_EXE)


class RunException(Enum):
    """
    """
    TimeOut = 0
    Error = -1


class MCTask(Enum):
    SendFile = 'sendfile'
    SendMessage = 'sendmessage'
    GetFile = 'getfile'
    GetMessage = 'getmessage'
    Status = 'status'
    Clear = 'clear'


@dataclass
class MessageClientRtnData:
    datetime: datetime
    exception: RunException or None
    msg: str = ''


class MessageClient:
    def __init__(
            self,
            ip, port,
            logger: logging.Logger or None = None,
    ):
        self._ip = ip
        self._port = port
        self._message_client = PATH_MC
        self._message_client_exe = PATH_MC_EXE
        if isinstance(logger, logging.Logger):
            self.logger = logger
        else:
            self.logger = MyLogger(name='MessageClient')

    @staticmethod
    def _check_timeout_arg(timeout, default=5) -> float:
        try:
            timeout = float(timeout)
        except:
            timeout = default
        else:
            if timeout < 1:
                timeout = 1
        finally:
            return timeout

    @staticmethod
    def _check_maxtry_arg(max_try, default=5) -> int:
        try:
            max_try = int(max_try)
        except:
            max_try = default
        else:
            if max_try < 1:
                max_try = 1
        finally:
            return max_try

    def _run_tp_mc(self, s_cmd, timeout) -> MessageClientRtnData:
        p = subprocess.Popen(
            s_cmd,
            cwd=self._message_client,
            stdout=subprocess.PIPE,
            shell=True,
        )

        try:
            outs, errs = p.communicate(timeout=timeout)
            output_s = str(outs, encoding="utf-8")
        except subprocess.TimeoutExpired as e:
            p.kill()
            self.logger.error('调用 MessageClient 超时:')
            self.logger.error(e)
            return MessageClientRtnData(datetime=datetime.now(), exception=RunException.TimeOut, msg=str(e))
        except Exception as e:
            p.kill()
            self.logger.error('调用 MessageClient 失败:')
            self.logger.error(e)
            return MessageClientRtnData(datetime=datetime.now(), exception=RunException.Error, msg=str(e))
        else:
            if 'Exception' in output_s:
                p.kill()
                self.logger.warning('调用 MessageClient 失败:')
                self.logger.warning(output_s)
                return MessageClientRtnData(datetime=datetime.now(), exception=RunException.Timeout, msg=output_s)
            else:
                p.kill()
                # print(output_s)
                return MessageClientRtnData(
                    # datetime=datetime.now(), exception=None, msg=output_s.split('\n')[1].split('<<')[1].strip())
                    datetime=datetime.now(), exception=None, msg=output_s.split('<<')[-1].strip())

    def _run(self, cmd, timeout=5, max_try=5) -> MessageClientRtnData:
        timeout = self._check_timeout_arg(timeout, default=5)
        max_try = self._check_maxtry_arg(max_try, default=5)

        s_cmd = f'TradingPlatform.MessageClient.exe {self._ip} {self._port} {cmd}'
        for n in range(max_try):
            rtn_data: MessageClientRtnData = self._run_tp_mc(s_cmd=s_cmd, timeout=timeout)
            error_type: RunException or None = rtn_data.exception
            if not error_type:
                # print(rtn_data.msg)
                return rtn_data
            else:
                if error_type == RunException.TimeOut:
                    self.logger.error(f'第{n+1}次运行，超时')
                    continue
                if error_type == RunException.Error:
                    self.logger.error(f'第{n + 1}次运行，失败')
                    continue
        self.logger.error('超过最大运行次数')
        return MessageClientRtnData(datetime=datetime.now(), exception=RunException.Error, msg='')

    def sendfile(self, key, file_path, timeout=5, max_try=5, with_timestamp: bool = False) -> MessageClientRtnData:
        s_cmd = f'{MCTask.SendFile.value} "{key}" "{file_path}"'
        self.logger.info(s_cmd)
        mcr: MessageClientRtnData = self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
        if not with_timestamp:
            return mcr
        else:
            if mcr.exception:
                return mcr
            else:
                dt_key = 'dt#' + key
                mcr_dt = self.sendmessage(key=dt_key, message=datetime.now().strftime('%Y%m%d %H%M%S'))
                if mcr_dt.exception:
                    self.logger.error('send dt key fault')
                return mcr

    def sendmessage(self, key, message, timeout=5, max_try=5, with_timestamp: bool = False) -> MessageClientRtnData:
        s_cmd = f'{MCTask.SendMessage.value} "{key}" "{message}"'
        self.logger.info(s_cmd)
        mcr = self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
        if not with_timestamp:
            return mcr
        else:
            if mcr.exception:
                return mcr
            else:
                dt_key = 'dt#' + key
                mcr_dt = self.sendmessage(key=dt_key, message=datetime.now().strftime('%Y%m%d %H%M%S'))
                if mcr_dt.exception:
                    self.logger.error('send dt key fail')
                return mcr

    def _get_timestamp(self, key, gap) -> None or datetime:
        dt_key = 'dt#' + key
        dt_mcr = self.getmessage(key=dt_key)
        if dt_mcr.exception:
            self.logger.error('get timestamp fail')
            return None
        s_timestamp = str(dt_mcr.msg)
        try:
            dt_timestamp = datetime.strptime(s_timestamp, '%Y%m%d %H%M%S')
        except Exception as e:
            self.logger.error(f'timestamp error {e}')
            return None
        dt_now = datetime.now()
        if (dt_now - dt_timestamp).seconds > gap:
            self.logger.info(f'timestamp error,timestamp={dt_timestamp.strftime("%Y%m%d %H%M%S")}')
            return None
        else:
            self.logger.info(f'timestamp pass,timestamp={dt_timestamp.strftime("%Y%m%d %H%M%S")}')
            return dt_timestamp

    def getfile(self, key, file_path, timeout=5, max_try=5,
                with_timestamp_gap: int or None = None) -> MessageClientRtnData or None:
        file_path = os.path.abspath(file_path)
        if not os.path.isdir(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        s_cmd = f'{MCTask.GetFile.value} "{key}" "{file_path}"'
        self.logger.info(s_cmd)
        if not with_timestamp_gap:
            return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
        else:
            key_dt = self._get_timestamp(key=key, gap=with_timestamp_gap)
            if key_dt:
                return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
            else:
                return None

    def getmessage(self, key, timeout=5, max_try=5,
                   with_timestamp_gap: int or None = None) -> MessageClientRtnData or None:
        s_cmd = f'{MCTask.GetMessage.value} "{key}"'
        self.logger.info(s_cmd)
        if not with_timestamp_gap:
            return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
        else:
            key_dt = self._get_timestamp(key=key, gap=with_timestamp_gap)
            if key_dt:
                return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)
            else:
                return None

    def status(self, timeout=5, max_try=5) -> MessageClientRtnData:
        s_cmd = f'{MCTask.Status.value} '
        self.logger.info(s_cmd)
        return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)

    def clear(self, key, timeout=5, max_try=5) -> MessageClientRtnData:
        s_cmd = f'{MCTask.Clear.value} "{key}" '
        self.logger.info(s_cmd)
        return self._run(cmd=s_cmd, timeout=timeout, max_try=max_try)



""""""

# mc = MessageClient(ip="120.79.58.75", port="11005")
