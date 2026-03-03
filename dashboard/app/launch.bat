@echo off
title FMD Operations Dashboard
echo.
echo  ============================================================
echo   FMD Data Pipeline Control Dashboard
echo  ============================================================
echo.

cd /d "%~dp0"

:: Check Python
where python >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found in PATH.
    pause
    exit /b 1
)

:: Check Node/npm
where npm >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] npm not found in PATH.
    pause
    exit /b 1
)

:: Install npm deps if needed
if not exist "node_modules" (
    echo  [SETUP] Installing npm dependencies...
    call npm install
    echo.
)

echo  Starting API server    http://127.0.0.1:8787 ...
start "FMD-API" /min cmd /k "cd /d %~dp0api && call start_api.bat"

:: Give the API a moment to bind the port
timeout /t 2 /nobreak >nul

echo  Starting Vite dev      http://localhost:5173 ...
echo.
echo  ============================================================
echo   Dashboard ready - open http://localhost:5173
echo   Close this window to stop the Vite dev server.
echo   The API server runs in a separate minimized window.
echo  ============================================================
echo.

call npm run dev
