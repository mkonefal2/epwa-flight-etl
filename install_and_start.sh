#!/usr/bin/env bash
set -euo pipefail

# Quick installer for EPWA Flight ETL on Ubuntu 22.04
# Requires sudo privileges. Running the script again offers a clean reinstall.

if [ "$(id -u)" -ne 0 ]; then
    echo "Please run this script with sudo or as root." >&2
    exit 1
fi

apt-get update
apt-get install -y python3-venv python3-pip git default-jdk

REPO_DIR="/home/airflow/epwa-flight-etl"

# handle reinstall scenario
if [ -d "$REPO_DIR" ]; then
    read -r -p "Existing installation detected in $REPO_DIR. Reinstall? [y/N]: " resp
    if [[ "$resp" =~ ^[Yy]$ ]]; then
        echo "Stopping running Airflow processes (if any)..."
        pkill -u airflow -f "airflow scheduler" >/dev/null 2>&1 || true
        pkill -u airflow -f "airflow webserver" >/dev/null 2>&1 || true
        # Ensure we're not deleting the directory we are running from
        # which would cause the script to fail midway.
        case "$PWD" in
            "$REPO_DIR"* ) cd /tmp ;;
        esac
        rm -rf "$REPO_DIR"
    else
        echo "Installation aborted."
        exit 0
    fi
fi

# create airflow user if not exists
if ! id -u airflow >/dev/null 2>&1; then
    useradd -m -d /home/airflow airflow
fi

# clone repository to airflow's home
sudo -u airflow git clone https://github.com/mkonefal2/epwa-flight-etl.git "$REPO_DIR"

chown -R airflow:airflow "$REPO_DIR"

# set up python virtual environment
sudo -u airflow python3 -m venv "$REPO_DIR/venv"
sudo -u airflow "$REPO_DIR/venv/bin/pip" install --upgrade pip
sudo -u airflow "$REPO_DIR/venv/bin/pip" install -r "$REPO_DIR/requirements.txt"

# store API key
if [ ! -f "$REPO_DIR/.env" ]; then
    read -p "Enter your AVIATIONSTACK_API_KEY: " APIKEY
    echo "AVIATIONSTACK_API_KEY=${APIKEY}" > "$REPO_DIR/.env"
    chown airflow:airflow "$REPO_DIR/.env"
fi

export AIRFLOW_HOME="$REPO_DIR/airflow"

# persist AIRFLOW_HOME for the airflow user
if [ -f /home/airflow/.bashrc ] && ! grep -q "AIRFLOW_HOME" /home/airflow/.bashrc; then
    echo "export AIRFLOW_HOME=$AIRFLOW_HOME" >> /home/airflow/.bashrc
    chown airflow:airflow /home/airflow/.bashrc
fi

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && $REPO_DIR/venv/bin/airflow db init"

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && $REPO_DIR/venv/bin/airflow users create --username admin --password StrongPassword123 --firstname Admin --lastname User --role Admin --email admin@example.com || true"

if command -v systemctl >/dev/null 2>&1; then
    cat >/etc/systemd/system/airflow-scheduler.service <<EOF
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
User=airflow
Environment=AIRFLOW_HOME=$AIRFLOW_HOME
ExecStart=$REPO_DIR/venv/bin/airflow scheduler
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    cat >/etc/systemd/system/airflow-webserver.service <<EOF
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
User=airflow
Environment=AIRFLOW_HOME=$AIRFLOW_HOME
ExecStart=$REPO_DIR/venv/bin/airflow webserver --port 8080 --debug
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable --now airflow-scheduler.service
    systemctl enable --now airflow-webserver.service
else
    sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup $REPO_DIR/venv/bin/airflow scheduler > $AIRFLOW_HOME/scheduler.log 2>&1 &"
    sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup $REPO_DIR/venv/bin/airflow webserver --port 8080 --debug > $AIRFLOW_HOME/webserver.log 2>&1 &"
fi

echo "Waiting for Airflow webserver to respond on http://localhost:8080 ..."
for i in {1..15}; do
    if curl -fs http://localhost:8080 >/dev/null 2>&1; then
        echo "Airflow webserver is up."
        break
    fi
    sleep 2
done

if ! curl -fs http://localhost:8080 >/dev/null 2>&1; then
    echo "Airflow webserver did not start correctly." >&2
    echo "Check $AIRFLOW_HOME/webserver.log for details." >&2
    exit 1
fi

cat <<EOM
Airflow is running in the background.
Access the UI at http://<your-vm-ip>:8080
Login: admin / StrongPassword123
EOM
