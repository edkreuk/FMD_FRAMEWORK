@echo off
REM ──────────────────────────────────────────────────────
REM FMD Operations Dashboard — Windows Service Installer
REM Uses NSSM (Non-Sucking Service Manager) to register
REM the Python API server as a Windows Service.
REM
REM Prerequisites:
REM   - NSSM installed and on PATH (https://nssm.cc)
REM   - Python 3.10+ installed
REM   - pyodbc installed
REM   - config.json configured with production credentials
REM ──────────────────────────────────────────────────────

set SERVICE_NAME=FMD-Dashboard
set DISPLAY_NAME=FMD Operations Dashboard
set DESCRIPTION=FMD Data Pipeline Operations Dashboard and API Server

REM Auto-detect paths
set SCRIPT_DIR=%~dp0
set API_DIR=%SCRIPT_DIR%api
set SERVER_PY=%API_DIR%\server.py

REM Find Python
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python not found on PATH
    exit /b 1
)
for /f "tokens=*" %%i in ('where python') do set PYTHON_EXE=%%i

REM Check NSSM
where nssm >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: NSSM not found on PATH
    echo Download from https://nssm.cc and add to PATH
    exit /b 1
)

REM Check config exists
if not exist "%API_DIR%\config.json" (
    echo ERROR: config.json not found in %API_DIR%
    echo Copy config.json.template to config.json and edit with production values
    exit /b 1
)

echo.
echo ========================================
echo  Installing %SERVICE_NAME%
echo ========================================
echo.
echo  Python:  %PYTHON_EXE%
echo  Script:  %SERVER_PY%
echo  Config:  %API_DIR%\config.json
echo.

REM Remove existing service if present
nssm stop %SERVICE_NAME% >nul 2>&1
nssm remove %SERVICE_NAME% confirm >nul 2>&1

REM Install service
nssm install %SERVICE_NAME% "%PYTHON_EXE%" "%SERVER_PY%"
nssm set %SERVICE_NAME% AppDirectory "%API_DIR%"
nssm set %SERVICE_NAME% DisplayName "%DISPLAY_NAME%"
nssm set %SERVICE_NAME% Description "%DESCRIPTION%"
nssm set %SERVICE_NAME% Start SERVICE_AUTO_START
nssm set %SERVICE_NAME% AppStdout "%API_DIR%\service-stdout.log"
nssm set %SERVICE_NAME% AppStderr "%API_DIR%\service-stderr.log"
nssm set %SERVICE_NAME% AppRotateFiles 1
nssm set %SERVICE_NAME% AppRotateBytes 10485760

echo.
echo Service installed. Starting...
nssm start %SERVICE_NAME%

echo.
echo ========================================
echo  %SERVICE_NAME% is running
echo ========================================
echo.
echo  Dashboard:  http://localhost:8787
echo  Health:     http://localhost:8787/api/health
echo.
echo  Management:
echo    nssm status %SERVICE_NAME%
echo    nssm stop %SERVICE_NAME%
echo    nssm start %SERVICE_NAME%
echo    nssm restart %SERVICE_NAME%
echo    nssm remove %SERVICE_NAME% confirm
echo.
