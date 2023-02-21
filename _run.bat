chcp 65001
@echo off


cd %~dp0
call getfile.bat

cd %~dp0
call _run_changefiles.bat


pause
exit

