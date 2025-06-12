#!/usr/bin/env bash
set -euo pipefail

# Script to update repository when new commits appear on GitHub

# Make sure we run in repository root
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "This directory is not a git repository." >&2
    exit 1
fi

if [ -z "$(git remote)" ]; then
    echo "No git remote configured." >&2
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    echo "Uncommitted changes found. Please commit or stash them before updating." >&2
    exit 1
fi

# Fetch remote updates
git remote update

LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "Remote changes detected. Pulling latest version..."
    git pull --ff-only
else
    echo "Repository is already up-to-date."
fi
