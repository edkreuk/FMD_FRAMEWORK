@echo off
REM ──────────────────────────────────────────────────────
REM FMD Operations Dashboard — Production Build
REM Builds the React frontend and packages for deployment.
REM Run from: dashboard\app\
REM ──────────────────────────────────────────────────────

echo.
echo ========================================
echo  FMD Dashboard — Production Build
echo ========================================
echo.

REM Check Node.js
where node >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Node.js not found. Install from https://nodejs.org
    exit /b 1
)

REM Install dependencies if needed
if not exist "node_modules" (
    echo Installing dependencies...
    call npm install
    if %ERRORLEVEL% neq 0 (
        echo ERROR: npm install failed
        exit /b 1
    )
)

REM Build frontend
echo Building React frontend...
call npm run build
if %ERRORLEVEL% neq 0 (
    echo ERROR: Build failed
    exit /b 1
)

echo.
echo Build complete. Output in dist\
echo.

REM Package for deployment
set DEPLOY_DIR=deploy
if exist "%DEPLOY_DIR%" rmdir /s /q "%DEPLOY_DIR%"
mkdir "%DEPLOY_DIR%"
mkdir "%DEPLOY_DIR%\dist"
mkdir "%DEPLOY_DIR%\api"

echo Packaging deployment...
xcopy /s /e /q "dist\*" "%DEPLOY_DIR%\dist\" >nul
copy "api\server.py" "%DEPLOY_DIR%\api\" >nul
copy "api\config.json" "%DEPLOY_DIR%\api\config.json.template" >nul
copy "install-service.bat" "%DEPLOY_DIR%\" >nul 2>nul

REM Create a README for the deployment package
(
echo FMD Operations Dashboard — Deployment Package
echo ================================================
echo.
echo Prerequisites:
echo   - Python 3.10+
echo   - pyodbc: pip install pyodbc
echo   - ODBC Driver 18 for SQL Server
echo.
echo Setup:
echo   1. Copy this folder to the target server
echo   2. Copy api\config.json.template to api\config.json
echo   3. Edit api\config.json with production credentials
echo   4. Test: python api\server.py
echo   5. Install as service: install-service.bat
echo.
echo The dashboard will be available at http://localhost:8787
) > "%DEPLOY_DIR%\README.txt"

echo.
echo ========================================
echo  Deployment package ready: %DEPLOY_DIR%\
echo ========================================
echo.
echo Contents:
echo   deploy\dist\          — Frontend static files
echo   deploy\api\           — Python API server
echo   deploy\api\config.json.template — Config template
echo   deploy\install-service.bat     — Windows Service installer
echo   deploy\README.txt     — Setup instructions
echo.
