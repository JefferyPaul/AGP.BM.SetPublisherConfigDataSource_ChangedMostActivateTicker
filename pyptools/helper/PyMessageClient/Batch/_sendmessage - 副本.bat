chcp 65001
@echo off

cd %~dp0
cd ..

python runMessageClient.py 192.168.1.81 12005 sendmessage -k "test" -a "success" 

pause
