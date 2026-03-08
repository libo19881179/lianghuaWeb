@echo off
chcp 65001 >nul
echo ========================================
echo   A-Share Portfolio Rebalancing System
echo   Server Installation Script
echo ========================================
echo.

:: Check Python installation
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found
    echo.
    echo Please install Python 3.10:
    echo 1. Visit: https://www.python.org/downloads/
    echo 2. Download Python 3.10.x for Windows
    echo 3. Check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)

echo [OK] Python found
python --version
echo.

:: Check and install dependencies
echo [CHECK] Checking dependencies...
python -c "import streamlit" >nul 2>&1
if %errorlevel% neq 0 (
    echo [INSTALL] Installing dependencies...
    echo.
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install dependencies
        pause
        exit /b 1
    )
    echo.
    echo [OK] Dependencies installed
) else (
    echo [OK] Dependencies already installed
)

echo.
echo ========================================
echo   Installation Complete!
echo ========================================
echo.
echo Next Steps:
echo 1. Configure firewall to allow port 8501
echo 2. Run run_server.bat to start the service
echo 3. Access http://localhost:8501
echo.
pause
