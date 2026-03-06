@echo off
REM Startup script for Document Analyzer UI
REM This script starts both the worker process and the Streamlit UI

echo Starting Document Analyzer...
echo.

REM Get the database path from config (same as UI uses)
echo [1/2] Determining database path...
python -c "import sys; sys.path.insert(0, '.'); from utils.config import get_config, get_job_queue_database; config = get_config(); db_path = get_job_queue_database(config); print(db_path)" > temp_db_path.txt
set /p DB_PATH=<temp_db_path.txt
del temp_db_path.txt

echo Database path: %DB_PATH%
echo.

REM Start worker in background with the correct database path
echo [2/2] Starting worker process...
start "Document Analyzer Worker" cmd /k "python worker/run_worker.py --db %DB_PATH%"
timeout /t 2 /nobreak >nul

REM Start Streamlit UI
echo [3/3] Starting Streamlit UI...
echo.
echo ========================================
echo UI will open in your default browser
echo ========================================
echo.
echo Database: %DB_PATH%
echo.
echo To stop:
echo   - Close this window to stop the UI
echo   - Close the Worker window to stop the worker
echo.

streamlit run ui/app.py

pause

