@echo off
REM One-click rolling update: fills missing NSE bhavcopies, rebuilds, self-checks + repairs.
cd /d "%~dp0"
where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found on PATH. Install Python 3.11+ and retry.
  pause
  exit /b 2
)
echo === Rolling update started: %DATE% %TIME% ===
python pipeline\roll_update.py --auto
if errorlevel 1 (
  echo.
  echo [ATTENTION] Finished with warnings/errors. See reports\health.json and latest logs\roll_update_*.log
) else (
  echo.
  echo [OK] Data is fresh. See reports\health.json
)
pause
