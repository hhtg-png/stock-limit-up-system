@echo off
chcp 65001 >nul
echo ========================================
echo   股票涨停统计分析系统 - 前端启动
echo ========================================
echo.

:: 检查Node.js
node --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Node.js，请先安装Node.js 18+
    echo 下载地址: https://nodejs.org/
    pause
    exit /b 1
)

:: 进入前端目录
cd /d "%~dp0frontend"

:: 安装依赖
if not exist "node_modules" (
    echo [信息] 安装前端依赖...
    npm install
)

:: 启动前端
echo.
echo [信息] 启动前端开发服务器 (端口: 3000)...
echo [信息] 访问地址: http://localhost:3000
echo.
npm run dev

pause
