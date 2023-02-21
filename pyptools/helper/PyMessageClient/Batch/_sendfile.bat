chcp 65001
@echo off

cd %~dp0
cd ..
call "venv\Scripts\activate.bat"

python runMessageClient.py 192.168.1.81 12005 sendfile -k "testfile" -a "..\Batch\a.txt" -t 

pause
