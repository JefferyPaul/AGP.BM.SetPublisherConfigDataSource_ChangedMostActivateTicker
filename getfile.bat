chcp 65001
@echo off

cd %~dp0

call venv/scripts/activate.bat
python gen_changed_tickers.py


pause