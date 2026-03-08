@echo off
chcp 65001 >nul
echo ========================================
echo   Create Windows Service
echo ========================================
echo.

:: Get current directory
set SERVICE_DIR=%~dp0

:: Create VBScript file for background running
echo Set objShell = CreateObject("WScript.Shell") > "%SERVICE_DIR%start_hidden.vbs"
echo objShell.Run "cmd.exe /c cd /d %SERVICE_DIR% ^&^& streamlit run app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true", 0, False >> "%SERVICE_DIR%start_hidden.vbs"

echo [OK] Created background startup script
echo.
echo Usage:
echo Double-click: start_hidden.vbs
echo.
echo Or configure in Task Scheduler:
echo 1. Open Task Scheduler
echo 2. Create Basic Task
echo 3. Program: wscript.exe
echo 4. Arguments: %SERVICE_DIR%start_hidden.vbs
echo.
pause
