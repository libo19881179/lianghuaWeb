@echo off

:: 自动更新服务启动脚本

cd /d "%~dp0"

:: 激活虚拟环境
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
    echo 已激活虚拟环境
) else (
    echo 未找到虚拟环境，使用系统Python
)

:: 启动自动更新服务
echo 启动自动更新服务...
python auto_update_service.py

pause
