@echo off
setlocal EnableDelayedExpansion
title Angel Holdover Assistant - Setup

:: Resolve project directory
cd /d "%~dp0"
set "PROJ_DIR=%CD%"

echo ======================================================
echo   Angel Holdover Assistant  --  Setup
echo ======================================================
echo.

:: ── 1. Find Python 3.11+ ─────────────────────────────────────────────────────
set "PYTHON="
for %%C in (python3 python) do (
    if "!PYTHON!"=="" (
        %%C --version >nul 2>&1
        if not errorlevel 1 (
            for /f "tokens=2 delims= " %%V in ('%%C --version 2^>^&1') do (
                for /f "tokens=1,2 delims=." %%A in ("%%V") do (
                    set /a "VER_CHECK=%%A*100+%%B"
                    if !VER_CHECK! GEQ 311 set "PYTHON=%%C"
                )
            )
        )
    )
)

if "!PYTHON!"=="" (
    echo ERROR: Python 3.11 or newer is required but was not found.
    echo.
    echo Please install Python from:
    echo   https://www.python.org/downloads/
    echo.
    echo IMPORTANT: During install, check "Add Python to PATH".
    echo Then double-click setup.bat again.
    echo.
    pause
    exit /b 1
)

for /f "tokens=*" %%V in ('!PYTHON! --version 2^>^&1') do echo OK  %%V
echo.

:: ── 2. Create virtual environment ────────────────────────────────────────────
if not exist "%PROJ_DIR%\venv\Scripts\python.exe" (
    echo Creating virtual environment...
    !PYTHON! -m venv "%PROJ_DIR%\venv"
    if errorlevel 1 (
        echo ERROR: Could not create virtual environment.
        pause & exit /b 1
    )
    echo OK  Virtual environment created
) else (
    echo OK  Virtual environment already exists
)

:: ── 3. Install Python packages ───────────────────────────────────────────────
echo.
echo Installing Python packages (this may take a minute^)...
call "%PROJ_DIR%\venv\Scripts\pip.exe" install --quiet --upgrade pip
call "%PROJ_DIR%\venv\Scripts\pip.exe" install -r "%PROJ_DIR%\requirements.txt"
if errorlevel 1 (
    echo ERROR: Package installation failed.
    pause & exit /b 1
)
echo OK  Python packages installed

:: ── 4. Install Playwright Chromium ───────────────────────────────────────────
echo.
echo Installing Playwright browser (~120 MB download, one-time only^)...
call "%PROJ_DIR%\venv\Scripts\playwright.exe" install chromium
if errorlevel 1 (
    echo ERROR: Playwright browser install failed.
    pause & exit /b 1
)
echo OK  Playwright Chromium installed

:: ── 5. Create .env from template if not present ──────────────────────────────
if not exist "%PROJ_DIR%\.env" (
    copy "%PROJ_DIR%\.env.example" "%PROJ_DIR%\.env" >nul
    echo OK  Created .env from template
) else (
    echo OK  .env already exists -- leaving as-is
)

:: ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ======================================================
echo   Setup complete!
echo.
echo   Next steps:
echo   1. Double-click  start.bat  to launch the app
echo   2. When the browser opens, click your name in the
echo      top-right and go to Profile to save your
echo      Comscore and Mica credentials
echo ======================================================
echo.
pause
