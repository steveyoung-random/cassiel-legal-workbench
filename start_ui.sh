#!/bin/bash
# Startup script for Document Analyzer UI
# This script starts both the worker process and the Streamlit UI

echo "Starting Document Analyzer..."
echo ""

# Get the database path from config (same as UI uses)
echo "[1/3] Determining database path..."
DB_PATH=$(python -c "import sys; sys.path.insert(0, '.'); from utils.config import get_config, get_job_queue_database; config = get_config(); print(get_job_queue_database(config))")

if [ -z "$DB_PATH" ]; then
    echo "Error: Could not determine database path"
    exit 1
fi

echo "Database path: $DB_PATH"
echo ""

# Start worker in background with the correct database path
echo "[2/3] Starting worker process..."
python worker/run_worker.py --db "$DB_PATH" &
WORKER_PID=$!
sleep 2

# Start Streamlit UI
echo "[3/3] Starting Streamlit UI..."
echo ""
echo "========================================"
echo "UI will open in your default browser"
echo "========================================"
echo ""
echo "Database: $DB_PATH"
echo ""
echo "To stop:"
echo "  - Press Ctrl+C to stop the UI"
echo "  - Worker PID: $WORKER_PID (kill with: kill $WORKER_PID)"
echo ""

streamlit run ui/app.py

# Cleanup: kill worker when UI stops
echo ""
echo "Stopping worker process..."
kill $WORKER_PID 2>/dev/null
wait $WORKER_PID 2>/dev/null

