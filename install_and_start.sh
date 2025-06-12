#!/usr/bin/env bash
set -euo pipefail

# Quick installer for EPWA Flight ETL on Ubuntu 22.04
# Requires sudo privileges.

if [ "$(id -u)" -ne 0 ]; then
    echo "Please run this script with sudo or as root." >&2
    exit 1
fi

apt-get update
apt-get install -y python3-venv python3-pip git default-jdk

# create airflow user if not exists
if ! id -u airflow >/dev/null 2>&1; then
    useradd -m -d /home/airflow airflow
fi

# clone repository to airflow's home
if [ ! -d /home/airflow/epwa-flight-etl ]; then
    sudo -u airflow git clone https://github.com/mkonefal2/epwa-flight-etl.git /home/airflow/epwa-flight-etl
fi

chown -R airflow:airflow /home/airflow/epwa-flight-etl

# set up python virtual environment
sudo -u airflow python3 -m venv /home/airflow/epwa-flight-etl/venv
sudo -u airflow /home/airflow/epwa-flight-etl/venv/bin/pip install --upgrade pip
sudo -u airflow /home/airflow/epwa-flight-etl/venv/bin/pip install -r /home/airflow/epwa-flight-etl/requirements.txt

# store API key
if [ ! -f /home/airflow/epwa-flight-etl/.env ]; then
    read -p "Enter your AVIATIONSTACK_API_KEY: " APIKEY
    echo "AVIATIONSTACK_API_KEY=${APIKEY}" > /home/airflow/epwa-flight-etl/.env
    chown airflow:airflow /home/airflow/epwa-flight-etl/.env
fi

export AIRFLOW_HOME=/home/airflow/epwa-flight-etl/airflow

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && /home/airflow/epwa-flight-etl/venv/bin/airflow db init"

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && /home/airflow/epwa-flight-etl/venv/bin/airflow users create --username admin --password StrongPassword123 --firstname Admin --lastname User --role Admin --email admin@example.com || true"

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup /home/airflow/epwa-flight-etl/venv/bin/airflow scheduler > $AIRFLOW_HOME/scheduler.log 2>&1 &"

sudo -u airflow bash -c "export AIRFLOW_HOME=$AIRFLOW_HOME && nohup /home/airflow/epwa-flight-etl/venv/bin/airflow webserver -p 8080 > $AIRFLOW_HOME/webserver.log 2>&1 &"

cat <<EOM
Airflow is running in the background.
Access the UI at http://<your-vm-ip>:8080
Login: admin / StrongPassword123
EOM
