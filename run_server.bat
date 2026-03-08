@echo off
chcp 65001 >nul
title A-Share Portfolio System

echo ========================================
echo   Starting Streamlit Server
echo ========================================
echo.
echo Port: 8501
echo Address: 0.0.0.0
echo.
echo Press Ctrl+C to stop
echo ========================================
echo.

cd /d %~dp0
streamlit run app.py --server.address 0.0.0.0 --server.port 8501

pause
