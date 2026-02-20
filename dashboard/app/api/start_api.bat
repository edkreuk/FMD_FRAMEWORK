@echo off
title FMD API Server
cd /d "%~dp0"

:: Load .env file if it exists
if exist ".env" (
    for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
        set "%%A=%%B"
    )
)

python server.py
