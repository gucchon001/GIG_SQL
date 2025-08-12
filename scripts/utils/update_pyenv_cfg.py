# import os
# import subprocess

# # 仮想環境のpyenv.cfgファイルのパス
# venv_path = r'C:\Users\tmnk015\myenv'  # 仮想環境が作成されたディレクトリ
# pyenv_cfg_path = os.path.join(venv_path, 'pyenv.cfg')

# # Python 3.11のパスを明示的に取得
# # ここで、Python 3.11がインストールされているパスを明示的に指定
# python_executable = r'C:\Users\tmnk015\AppData\Local\Programs\Python\Python311\python.exe'

# # Pythonのバージョンを取得
# python_version = subprocess.check_output([python_executable, '--version']).decode().strip().split()[1]

# # pyenv.cfgファイルの新しい内容
# pyenv_content = f"""home = {os.path.dirname(python_executable)}
# include-system-site-packages = false
# version = {python_version}
# executable = {python_executable}
# command = {python_executable} -m venv {venv_path}
# """

# # pyenv.cfgファイルを書き換え
# with open(pyenv_cfg_path, 'w') as f:
#     f.write(pyenv_content)

# print(f"pyenv.cfg updated with Python 3.11 path: {python_executable}")

import sys
import os

# 仮想環境のpyenv.cfgファイルのパスを指定
venv_path = r'C:\Users\tmnk015\myenv'
pyenv_cfg_path = os.path.join(venv_path, 'pyenv.cfg')

# 現在のPCのPythonインストールパスを取得
python_executable = sys.executable
python_home = os.path.dirname(python_executable)

# pyenv.cfgファイルの新しい内容
pyenv_content = f"""home = {python_home}
include-system-site-packages = false
version = {sys.version.split()[0]}
executable = {python_executable}
command = {python_executable} -m venv {venv_path}
"""

# pyenv.cfgファイルを書き換え
try:
    with open(pyenv_cfg_path, 'w') as f:
        f.write(pyenv_content)
    print(f"pyenv.cfg updated with Python path: {python_executable}")
except Exception as e:
    print(f"Error writing to pyenv.cfg: {e}")
