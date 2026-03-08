@echo off
chcp 65001 >nul
title A 股股票组合再平衡回测系统 - 生产环境

echo ========================================
echo   A 股股票组合再平衡回测系统
echo   正在启动服务...
echo ========================================
echo.
echo 启动参数:
echo - 监听地址：0.0.0.0 (允许外部访问)
echo - 端口：8501
echo - 模式：生产环境
echo.
echo 访问地址:
echo - 本地：http://localhost:8501
echo - 远程：http://你的服务器IP:8501
echo.
echo 按 Ctrl+C 停止服务
echo ========================================
echo.

cd /d %~dp0

:: 启动 Streamlit（使用配置文件）
streamlit run app.py

echo.
echo 服务已停止
pause
