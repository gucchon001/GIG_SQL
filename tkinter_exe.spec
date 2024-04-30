# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['C:\\Users\\tmnk013\\Desktop\\pyinstall\\20240416\\tkinter_exe.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['tkcalendar', 'mysql.connector.connection', 'mysql.connector.cursor', 'mysql.connector.pooling'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='tkinter_exe',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='tkinter_exe',
)
