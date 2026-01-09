@echo off
cd /d "%~dp0"

REM Intenta ejecutar con pythonw (sin ventana de consola)
pythonw "%~dp0landing\desktop_app.py"

REM Si falla, intenta con python normal para ver errores
if errorlevel 1 (
    echo Error al iniciar el monitor. Presiona cualquier tecla para ver detalles...
    pause
    python "%~dp0landing\desktop_app.py"
    pause
)
