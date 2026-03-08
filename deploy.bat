@echo off
chcp 65001 >nul
echo ========================================
echo   Streamlit 公网部署助手
echo ========================================
echo.

:: 检查 Git 是否安装
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Git，请先安装 Git
    echo 下载地址：https://git-scm.com/download/win
    pause
    exit /b 1
)

echo [✓] Git 已安装
echo.

:: 选择部署方案
echo 请选择部署方案:
echo.
echo 1. Streamlit Cloud (免费，最简单，国内访问慢)
echo 2. Hugging Face Spaces (免费，国内可访问)
echo 3. Railway (免费额度，需要信用卡)
echo 4. 阿里云/腾讯云 (付费，约 100 元/年，国内最快)
echo 5. Ngrok 内网穿透 (免费，临时使用)
echo.
set /p choice="请输入选择 (1-5): "

if "%choice%"=="1" goto streamlit_cloud
if "%choice%"=="2" goto huggingface
if "%choice%"=="3" goto railway
if "%choice%"=="4" goto aliyun
if "%choice%"=="5" goto ngrok

echo [错误] 无效的选择
pause
exit /b 1

:streamlit_cloud
echo.
echo ========================================
echo   方案 1: Streamlit Cloud
echo ========================================
echo.
echo 步骤:
echo 1. 访问 https://github.com 并登录
echo 2. 创建新仓库 (Repository)
echo 3. 按提示推送代码到 GitHub
echo 4. 访问 https://streamlit.io/cloud
echo 5. 点击 "Deploy now"
echo 6. 选择你的 GitHub 仓库
echo 7. 点击 "Deploy!"
echo.
echo 等待 2-3 分钟后，您会获得一个公网地址:
echo https://你的仓库名 - 用户名-app-xxxxxx.streamlit.app/
echo.
set /p open="是否打开 Streamlit Cloud 网站？(Y/N): "
if /i "%open%"=="Y" start https://streamlit.io/cloud
goto end

:huggingface
echo.
echo ========================================
echo   方案 2: Hugging Face Spaces
echo ========================================
echo.
echo 步骤:
echo 1. 访问 https://huggingface.co 并注册
echo 2. 点击右上角头像 → "New Space"
echo 3. 填写信息:
echo    - Space name: a-stock-backtest
echo    - SDK: 选择 "Streamlit"
echo 4. 创建后上传代码
echo 5. 等待部署完成
echo.
echo 部署成功后访问:
echo https://huggingface.co/spaces/你的用户名/a-stock-backtest
echo.
set /p open="是否打开 Hugging Face 网站？(Y/N): "
if /i "%open%"=="Y" start https://huggingface.co/new-space
goto end

:railway
echo.
echo ========================================
echo   方案 3: Railway
echo ========================================
echo.
echo 步骤:
echo 1. 访问 https://railway.app
echo 2. 使用 GitHub 账号登录
echo 3. 点击 "New Project"
echo 4. 选择 "Deploy from GitHub repo"
echo 5. 选择你的仓库
echo 6. Railway 会自动部署
echo.
echo 注意：需要添加信用卡（有免费额度）
echo.
echo 部署成功后访问:
echo https://你的项目名.up.railway.app
echo.
set /p open="是否打开 Railway 网站？(Y/N): "
if /i "%open%"=="Y" start https://railway.app
goto end

:aliyun
echo.
echo ========================================
echo   方案 4: 阿里云/腾讯云
echo ========================================
echo.
echo 步骤:
echo 1. 访问阿里云或腾讯云官网
echo 2. 购买云服务器（约 100 元/年）
echo 3. 连接服务器并安装依赖
echo 4. 运行 Streamlit
echo 5. 配置防火墙开放 8501 端口
echo.
echo 详细步骤请查看 DEPLOYMENT.md 文档
echo.
set /p open="是否打开部署文档？(Y/N): "
if /i "%open%"=="Y" start DEPLOYMENT.md
goto end

:ngrok
echo.
echo ========================================
echo   方案 5: Ngrok 内网穿透
echo ========================================
echo.
echo 步骤:
echo 1. 下载 Ngrok: https://ngrok.com/download
echo 2. 本地运行：streamlit run app.py
echo 3. 在另一个终端运行：ngrok http 8501
echo 4. 获得临时公网地址
echo.
echo 注意：每次重启地址会变化（临时使用）
echo.
set /p open="是否打开 Ngrok 下载页面？(Y/N): "
if /i "%open%"=="Y" start https://ngrok.com/download
goto end

:end
echo.
echo ========================================
echo   部署提示
echo ========================================
echo.
echo 1. 部署前请确保本地运行正常
echo 2. 查看 DEPLOYMENT.md 获取详细帮助
echo 3. 遇到问题可查看各平台的日志
echo.
echo 祝您部署成功！
echo.
pause
