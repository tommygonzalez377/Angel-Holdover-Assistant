@echo off
setlocal EnableDelayedExpansion
title Angel Holdover Assistant

cd /d "%~dp0"
set "PROJ_DIR=%CD%"

:: â”€â”€ Check setup was completed â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if not exist "%PROJ_DIR%\venv\Scripts\python.exe" (
    echo ERROR: Setup has not been run yet.
    echo Please double-click setup.bat first.
    echo.
    pause
    exit /b 1
)

:: â”€â”€ Auto-update from GitHub â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if exist "%PROJ_DIR%\.git\" (
    echo Checking for updates...
    git -C "%PROJ_DIR%" pull --quiet 2>nul && echo OK  Up to date || echo    Could not reach GitHub -- starting anyway
)

:: â”€â”€ Stop any previous server on port 8766 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
for /f "tokens=5" %%P in ('netstat -ano 2^>nul ^| findstr /R ":8080 "') do (
    taskkill /F /PID %%P >nul 2>&1
)

:: â”€â”€ Start server in a new background window â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo Starting Angel Holdover Assistant...
start "Angel Holdover Assistant Server" /min "%PROJ_DIR%\venv\Scripts\python.exe" "%PROJ_DIR%\launcher.py"

:: â”€â”€ Wait for server to respond (up to 15 seconds) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo Waiting for server...
set READY=0
for /l %%i in (1,1,30) do (
    if !READY!==0 (
        timeout /t 1 /nobreak >nul
        curl -sf http://localhost:8766 >nul 2>&1
        if not errorlevel 1 set READY=1
    )
)

if !READY!==0 (
    echo.
    echo Server did not start. Check the server window for errors.
    pause
    exit /b 1
)

:: ── Open in Chrome (fall back to default browser if Chrome not found) ────
set CHROME=
for %%P in (
  "%PROGRAMFILES%\Google\Chrome\Application\chrome.exe"
  "%PROGRAMFILES(X86)%\Google\Chrome\Application\chrome.exe"
  "%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"
) do (
  if not defined CHROME if exist %%P set CHROME=%%~P
)
if defined CHROME (
  start "" "%CHROME%" http://localhost:8766
) else (
  start "" http://localhost:8766
)

echo.
echo ======================================================
echo   Angel Holdover Assistant is running
echo   URL: http://localhost:8766
echo.
echo   The server is running in a minimized window.
echo   Close the server window to stop the app.
echo ======================================================
echo.
echo This window can be closed.
timeout /t 5 /nobreak >nul

