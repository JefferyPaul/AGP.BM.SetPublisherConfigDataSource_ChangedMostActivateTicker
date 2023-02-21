""""""

import os
from datetime import datetime
import json
import argparse
import socket

# smtplib 用于邮件的发信动作
import smtplib
# email 用于构建邮件内容
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# 用于构建邮件头
from email.header import Header

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))


def _read_config():
    path_config = os.path.join(PATH_ROOT, 'config.json')
    return json.loads(open(path_config).read())


def send_email(
        email, pwd, host, port, to,
        subject, text='', file='', filename='', **kwargs
):
    # d_config: dict = _read_config()
    _from_addr = email
    _pwd = pwd
    _smtp_host = host
    _smtp_port = port
    _to_addr = to

    msg = MIMEMultipart('mixed')
    # 邮件头信息
    msg['From'] = Header(_from_addr)
    msg['To'] = Header(','.join(_to_addr))  # 收件人，多个收件人用逗号隔开
    msg['Subject'] = Header(subject)        # 邮件主题

    # 构造文字内容
    # 邮箱正文内容，第一个参数为内容，第二个参数为格式(plain 为纯文本)，第三个参数为编码
    _ending_text = f'Time:  %s\nFrom:  %s' % (
        datetime.now().strftime('%Y/%m/%d %H:%M:%S'),   # 发送时间
        socket.gethostbyname(socket.gethostname())      # 本地IP
    )
    text_plain = MIMEText(text + '\n\n' + _ending_text, 'plain', 'utf-8')
    msg.attach(text_plain)

    # 构造附件
    if file:
        sendfile = open(file, 'rb').read()
        text_att = MIMEText(sendfile, 'base64', 'utf-8')
        text_att["Content-Type"] = 'application/octet-stream'
        # 以下附件可以重命名成aaa.txt
        # text_att["Content-Disposition"] = 'attachment; filename="aaa.txt"'
        # 另一种实现方式
        if not filename:
            filename = os.path.basename(file)
        text_att.add_header('Content-Disposition', 'attachment', filename=filename)
        # 以下中文测试不ok
        # text_att["Content-Disposition"] = u'attachment; filename="中文附件.txt"'.decode('utf-8')
        msg.attach(text_att)

    # 发送邮件
    # 开启发信服务，这里使用的是加密传输
    server = smtplib.SMTP_SSL(_smtp_host)
    server.connect(host=_smtp_host, port=_smtp_port)
    # 登录发信邮箱
    server.login(_from_addr, _pwd)
    # 发送邮件
    server.sendmail(_from_addr, _to_addr, msg.as_string())
    # 关闭服务器
    server.quit()


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--subject', help='邮件主题', default='')
    arg_parser.add_argument('--text', help='邮件内容', default='')
    arg_parser.add_argument('--file', help='文件附件', default='')
    arg_parser.add_argument('--filename', help='文件附件名称', default='')
    args = arg_parser.parse_args()
    _subject = args.subject
    _text = args.text
    _file = args.file
    _filename = args.filename

    d_config: dict = _read_config()

    send_email(_subject, _text, _file, _filename, **d_config)
