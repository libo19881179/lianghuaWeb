@echo off
chcp 65001 >nul
title A-Share Portfolio System

echo ========================================
echo   Starting A-Share Portfolio System

echo ========================================
echo.
echo 1. Starting Auto Update Service...
echo 2. Starting Streamlit Server...
echo.
echo Press Ctrl+C to stop
echo ========================================
echo.

cd /d "%~dp0"

:: 检查 Python 是否可用
echo Checking Python installation...
python --version
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

:: 检查 schedule 库是否安装
echo Checking required packages...
pip list | findstr schedule
if errorlevel 1 (
    echo Installing schedule package...
    pip install schedule
)

:: 启动自动更新服务（在新窗口中运行）
echo Starting Auto Update Service...
start "Auto Update Service" cmd /c "python auto_update_service.py"

:: 等待几秒钟，确保自动更新服务启动
echo Waiting for Auto Update Service to start...
ping 127.0.0.1 -n 5 >nul

echo Starting Streamlit Server...
echo Port: 8501
echo Address: 0.0.0.0
echo.

:: 启动 Streamlit 服务器
streamlit run app.py --server.address 0.0.0.0 --server.port 8501

pause
