#!/usr/bin/env bash
set -euo pipefail

# Restart the Airflow webserver
# Requires privileges to manage the airflow user processes

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_HOME="$REPO_DIR/airflow"
AIRFLOW_USER="airflow"
AIRFLOW_CMD="$REPO_DIR/venv/bin/airflow"

stop_webserver() {
    echo "Stopping Airflow webserver if running..."
    pkill -u "$AIRFLOW_USER" -f "airflow webserver" >/dev/null 2>&1 || true
}

start_webserver() {
    echo "Starting Airflow webserver..."
    if [ "$(id -u)" -eq 0 ]; then
        sudo -u "$AIRFLOW_USER" bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup $AIRFLOW_CMD webserver -p 8080 > $AIRFLOW_HOME/webserver.log 2>&1 &"
    else
        export AIRFLOW_HOME="$AIRFLOW_HOME"
        nohup "$AIRFLOW_CMD" webserver -p 8080 > "$AIRFLOW_HOME/webserver.log" 2>&1 &
    fi
}

stop_webserver
start_webserver

echo "Airflow webserver restarted."
