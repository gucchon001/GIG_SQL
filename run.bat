@echo off
REM pyenv.cfgを動的に更新するPythonスクリプトを実行
python \\NAS\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode\update_pyenv_cfg.py

REM 仮想環境をアクティブにする
call C:\Users\tmnk015\myenv\Scripts\activate.bat

REM NAS上のディレクトリに移動して、main.pyを実行
pushd \\NAS\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode
python main.py

REM エラーレベルの確認
if %ERRORLEVEL% neq 0 (
    echo Pythonスクリプトでエラーが発生しました。
)

REM 一時的なドライブを解除（エラーが発生しても必ず実行）
popd

echo 1分待っています...
timeout /t 60 /nobreak >nul
popd

exit /b





#pushd \\nas\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode
#call \\nas\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode\myenv\Scripts\activate.bat
#python main.py

#echo 1分待っています...
#timeout /t 60 /nobreak >nul
#exit/b