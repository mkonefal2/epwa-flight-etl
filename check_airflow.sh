#!/usr/bin/env bash
set -euo pipefail

# Simple health check for Airflow webserver on port 8080
# Usage: ./check_airflow.sh [host]
# If no host is provided, localhost is used.

HOST=${1:-localhost}
PORT=8080

if command -v pgrep >/dev/null; then
  echo "Checking if Airflow webserver process is running..."
  if pgrep -f "airflow webserver" >/dev/null; then
    echo "Airflow webserver process is running."
  else
    echo "Airflow webserver process not found." >&2
    exit 1
  fi
fi

echo "Checking if port $PORT is listening..."
python3 - <<PY
import socket,sys
s=socket.socket()
try:
    s.settimeout(2)
    s.connect(("localhost", $PORT))
    print("Port $PORT is open.")
except Exception as e:
    print("Port $PORT is not open.", file=sys.stderr)
    sys.exit(1)
finally:
    s.close()
PY

echo "Checking HTTP response from http://$HOST:$PORT ..."
python3 - <<PY
import sys
from urllib import request, error
url = "http://$HOST:$PORT"
try:
    with request.urlopen(url, timeout=5) as resp:
        print(f"Received {resp.status} from {url}")
except Exception as e:
    print(f"Failed to reach {url}", file=sys.stderr)
    sys.exit(1)
PY

exit 0
