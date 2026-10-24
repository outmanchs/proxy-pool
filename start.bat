@echo off
cd /d "%~dp0"
set "PYTHON_EXE=%USERPROFILE%\.pyenv\pyenv-win\versions\3.12.8\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
"%PYTHON_EXE%" start.py
pause
