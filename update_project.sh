#!/usr/bin/env bash
set -euo pipefail

# Script to update repository when new commits appear on GitHub

# Make sure we run in repository root
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    cat >&2 <<'EOF'
This directory is not a git repository.
Clone the project with:
    git clone https://github.com/mkonefal2/epwa-flight-etl.git
or navigate to your existing clone and re-run this script.
EOF
    exit 1
fi

if [ -z "$(git remote)" ]; then
    cat >&2 <<'EOF'
No git remote configured.
Connect your repository to a remote with:
    git remote add origin <url>
Then run this script again to pull updates.
EOF
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    cat >&2 <<'EOF'
Uncommitted changes found.
View them with:
    git status
Commit your changes:
    git add <files>
    git commit -m "Your message"
Or stash them temporarily:
    git stash
Afterward, re-run this script.
EOF
    exit 1
fi

# Fetch remote updates
git remote update

LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "Remote changes detected. Pulling latest version..."
    git pull --ff-only
    echo "Update complete."
else
    echo "Repository is already up-to-date."
fi
