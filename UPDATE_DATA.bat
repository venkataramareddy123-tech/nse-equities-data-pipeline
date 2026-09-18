@echo off
setlocal
REM One-click rolling update: fills missing NSE bhavcopies, rebuilds, self-checks + repairs.
cd /d "%~dp0"
where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found on PATH. Install Python 3.11+ and retry.
  set "STATUS=2"
  goto :finish
)
echo === Rolling update started: %DATE% %TIME% ===
python pipeline\roll_update.py --auto
set "STATUS=%ERRORLEVEL%"
if not "%STATUS%"=="0" (
  echo.
  echo [ATTENTION] Finished with warnings/errors. See reports\health.json and latest logs\roll_update_*.log
) else (
  echo.
  echo [OK] Data is fresh. See reports\health.json
)

:finish
if /i not "%CI%"=="1" pause
exit /b %STATUS%
