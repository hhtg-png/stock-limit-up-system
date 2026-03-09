@echo off
chcp 65001 >nul
echo ========================================
echo   股票涨停统计分析系统 - 一键启动
echo ========================================
echo.

:: 启动后端
start "" "%~dp0start-backend.bat"

:: 等待后端启动
echo [信息] 等待后端服务启动...
timeout /t 5 /nobreak >nul

:: 启动前端
start "" "%~dp0start-frontend.bat"

echo.
echo [成功] 系统启动完成!
echo.
echo 后端API: http://localhost:8000/docs
echo 前端界面: http://localhost:3000
echo.
pause
