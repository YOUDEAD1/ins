@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
cd /d "%~dp0"
title LARA STOCK - Bot
where py >nul 2>nul
if not errorlevel 1 (
    py -3.12 -c "import sys" >nul 2>nul
    if not errorlevel 1 (
        py -3.12 start_bot.py
    ) else (
        py -3 start_bot.py
    )
) else (
    where python >nul 2>nul
    if errorlevel 1 (
        echo Python was not found. Install Python 3.12, then run this file again.
        echo Official download: https://www.python.org/downloads/windows/
        pause
        exit /b 1
    )
    python start_bot.py
)
echo.
echo The bot has stopped. Review the messages above.
pause
