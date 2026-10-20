@echo off
setlocal
cd /d "%~dp0"
title LARA STOCK - Read-only referral report
echo This tool only reads the database. It does not start the bot or change balances.
echo If settings are missing, choose the filled .env file used by your bot.
echo You may drag that settings file onto this launcher to select it directly.
if "%~1"=="" goto auto_settings
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" referral_report.py --env "%~1"
  goto finished
)
where py >nul 2>&1
if errorlevel 1 goto system_python_file
py -3 referral_report.py --env "%~1"
goto finished
:system_python_file
python referral_report.py --env "%~1"
goto finished
:auto_settings
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" referral_report.py
  goto finished
)
where py >nul 2>&1
if errorlevel 1 goto system_python
py -3 referral_report.py
goto finished
:system_python
python referral_report.py
:finished
echo.
pause
