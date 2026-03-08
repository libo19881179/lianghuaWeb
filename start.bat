@echo off
chcp 65001 >nul
echo ========================================
echo   A 股股票组合再平衡回测系统
echo ========================================
echo.

:: 检查 Python 是否安装
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Python，请先安装 Python 3.8+
    pause
    exit /b 1
)

echo [✓] Python 已安装
echo.

:: 检查依赖是否安装
echo [检查] 检查依赖包...
python -c "import streamlit" >nul 2>&1
if %errorlevel% neq 0 (
    echo [安装] 正在安装依赖包...
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    if %errorlevel% neq 0 (
        echo [错误] 依赖包安装失败
        pause
        exit /b 1
    )
    echo [✓] 依赖包安装完成
) else (
    echo [✓] 依赖包已安装
)

echo.
echo ========================================
echo   启动 Streamlit 服务...
echo ========================================
echo.
echo 浏览器将自动打开：http://localhost:8501
echo.
echo 按 Ctrl+C 可停止服务
echo.

:: 关键修改：用python -m调用streamlit，绕过环境变量问题
python -m streamlit run app.py --server.address localhost --server.port 8501 --browser.gatherUsageStats false

pause