@echo off
REM pyenv.cfg�𓮓I�ɍX�V����Python�X�N���v�g�����s（ローカル実行のためコメントアウト）
REM python \\NAS\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode\update_pyenv_cfg.py

REM ���z�����A�N�e�B�u�ɂ���
call C:\Users\tmnk015\myenv\Scripts\activate.bat

REM NAS��̃f�B���N�g���Ɉړ����āAmain.py�����s
REM ローカルディレクトリでmain.pyを実行（ネットワークドライブを使用しない）
REM pushd \\NAS\public\事務業務資料\100000管理部門\101000システム\事業部プログラム\講師求人部門\GIG塾STMYSQL\sourcecode
python main.py

REM �G���[���x���̊m�F
if %ERRORLEVEL% neq 0 (
    echo Python�X�N���v�g�ŃG���[���������܂����B
)

REM �ꎞ�I�ȃh���C�u�������i�G���[���������Ă��K�����s�j
REM popd

echo 1���҂��Ă��܂�...
timeout /t 60 /nobreak >nul
REM popd

exit /b





#pushd \\nas\public\�����Ɩ�����\100000�Ǘ�����\101000�V�X�e��\���ƕ��v���O����\�u�t���l����\GIG�mSTMYSQL\sourcecode
#call \\nas\public\�����Ɩ�����\100000�Ǘ�����\101000�V�X�e��\���ƕ��v���O����\�u�t���l����\GIG�mSTMYSQL\sourcecode\myenv\Scripts\activate.bat
#python main.py

#echo 1���҂��Ă��܂�...
#timeout /t 60 /nobreak >nul
#exit/b