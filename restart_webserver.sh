#!/usr/bin/env bash
set -euo pipefail

# Restart the Airflow webserver
# Requires privileges to manage the airflow user processes

REPO_DIR="/home/airflow/epwa-flight-etl"
AIRFLOW_HOME="$REPO_DIR/airflow"
AIRFLOW_USER="airflow"
AIRFLOW_CMD="$REPO_DIR/venv/bin/airflow"
LOG_FILE="$AIRFLOW_HOME/webserver.log"

stop_webserver() {
    echo "Stopping Airflow webserver if running..."
    pkill -u "$AIRFLOW_USER" -f "airflow webserver" >/dev/null 2>&1 || true
}

ensure_permissions() {
    if [ "$(id -u)" -eq 0 ]; then
        chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "$REPO_DIR"
    fi
}

start_webserver() {
    echo "Starting Airflow webserver..."
    if [ "$(id -u)" -eq 0 ]; then
        sudo -u "$AIRFLOW_USER" bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup $AIRFLOW_CMD webserver -p 8080 > $LOG_FILE 2>&1 &"
    else
        export AIRFLOW_HOME="$AIRFLOW_HOME"
        nohup "$AIRFLOW_CMD" webserver -p 8080 > "$LOG_FILE" 2>&1 &
    fi
}

ensure_permissions
stop_webserver
start_webserver

echo "Airflow webserver restarted."
