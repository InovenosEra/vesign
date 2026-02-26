#!/bin/bash
# Build the React frontend and reload the running service.
# Run this whenever you want to deploy frontend changes.
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Building React frontend..."
cd "$ROOT/frontend"
npm run build

echo "Build complete. Frontend is now served by FastAPI at http://localhost:8000"

# If the launchd service is running, kick it to pick up any backend changes.
if launchctl list com.vesign &>/dev/null; then
    echo "Reloading launchd service..."
    launchctl kickstart -k gui/$(id -u)/com.vesign 2>/dev/null || true
fi

echo "Done."
