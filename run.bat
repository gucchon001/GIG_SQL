@echo off
pushd \\nas\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode
set "VIRTUAL_ENV=.\myenv"
call "%VIRTUAL_ENV%\Scripts\activate.bat"
python main.py
popd
pause
