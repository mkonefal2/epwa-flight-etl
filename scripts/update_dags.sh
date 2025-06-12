#!/usr/bin/env bash
set -euo pipefail

# Update Airflow DAGs from the git repository and reload the scheduler

# Determine repository root (one level up from this script)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

# Pull latest changes (fast-forward only)
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git pull --ff-only
else
    echo "Error: $REPO_DIR is not a git repository" >&2
    exit 1
fi

# Signal Airflow scheduler to reload DAGs
pkill -HUP -f "airflow scheduler" >/dev/null 2>&1 || true

echo "DAGs updated and scheduler reloaded."
