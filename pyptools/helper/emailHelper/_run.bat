chcp 65001
@echo off

echo.
cd %~dp0
python emailhelper.py --subject "bat测试" --text "错误文件" --file ".\test.csv"


pause