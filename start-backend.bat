@echo off
chcp 65001 >nul
echo ========================================
echo   股票涨停统计分析系统 - 启动脚本
echo ========================================
echo.

:: 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Python，请先安装Python 3.10+
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

:: 进入后端目录
cd /d "%~dp0backend"

:: 检查虚拟环境
if not exist "venv" (
    echo [信息] 创建虚拟环境...
    python -m venv venv
)

:: 激活虚拟环境
call venv\Scripts\activate.bat

:: 安装依赖
echo [信息] 安装后端依赖...
pip install -r requirements.txt -q

:: 创建数据目录
if not exist "data" mkdir data
if not exist "logs" mkdir logs

:: 启动后端
echo.
echo [信息] 启动后端服务 (端口: 8000)...
echo [信息] API文档: http://localhost:8000/docs
echo.
start cmd /k "venv\Scripts\activate.bat && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"

echo.
echo [成功] 后端服务已在新窗口启动
echo.
pause
