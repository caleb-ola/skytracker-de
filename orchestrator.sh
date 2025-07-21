#!/bin/bash

PROJECT_DIR="/Users/admin/Documents/Ma Stuffs/Savage/Practice/DE/DE_Portfolio/SkyTrack DE"
PYTHON_BIN="/opt/anaconda3/envs/skytrack-env/bin/python3"
ETL_SCRIPT_FILE="$PROJECT_DIR/scripts/etl.py"
INGEST_SCRIPT_FILE="$PROJECT_DIR/scripts/ingest.py"
LOG_FILE="$PROJECT_DIR/logs/etl.log"

cd "$PROJECT_DIR" || exit

echo "Starting flight pipeline: $(date)" >> "$LOG_FILE"

echo "Running Ingestion..."
"$PYTHON_BIN" "$INGEST_SCRIPT_FILE" >> "$LOG_FILE" 2>&1

echo "Running ETL process..."
"$PYTHON_BIN" "$ETL_SCRIPT_FILE" >> "$LOG_FILE" 2>&1


