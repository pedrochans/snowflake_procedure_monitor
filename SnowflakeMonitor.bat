@echo off
title Snowflake Monitor
cd /d "%~dp0"

REM Ensure logs directory exists
if not exist "logs" mkdir "logs"

REM Try pythonw first (no console window)
where pythonw >nul 2>nul
if %errorlevel% equ 0 (
    start "" pythonw "%~dp0landing\desktop_app.py"
    exit /b 0
)

REM Fallback: try python (shows console briefly)
where python >nul 2>nul
if %errorlevel% equ 0 (
    start "" python "%~dp0landing\desktop_app.py"
    exit /b 0
)

REM If no Python found, show error
echo ============================================
echo   ERROR: Python not found in PATH
echo.
echo   Please install Python 3.8+ and ensure
echo   it is added to your system PATH.
echo ============================================
pause
