#!/bin/bash
# Fresh-install script.
# Run once after cloning the repo to wire everything up.
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

# ── 1. Python virtual environment ──────────────────────────────────────────
if [ ! -d "$ROOT/venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$ROOT/venv"
fi

echo "Installing Python dependencies..."
"$ROOT/venv/bin/pip" install --upgrade pip -q
"$ROOT/venv/bin/pip" install -r "$ROOT/requirements.txt" -q
echo "Python dependencies installed."

# ── 2. Node / frontend ─────────────────────────────────────────────────────
echo "Installing Node dependencies..."
cd "$ROOT/frontend"
npm install
echo "Building React frontend..."
npm run build
cd "$ROOT"

# ── 3. .env ────────────────────────────────────────────────────────────────
if [ ! -f "$ROOT/.env" ]; then
    echo "Creating .env from .env.example..."
    cp "$ROOT/.env.example" "$ROOT/.env"
fi

# ── 4. launchd service (macOS only) ────────────────────────────────────────
PLIST_SRC="$ROOT/deploy/com.vesign.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.vesign.plist"

if [[ "$(uname)" == "Darwin" ]]; then
    echo "Installing launchd service..."
    # Patch the plist so paths match this machine
    sed "s|/Users/inovenos/PycharmProjects/Vesign|$ROOT|g" \
        "$PLIST_SRC" > "$PLIST_DST"

    launchctl unload "$PLIST_DST" 2>/dev/null || true
    launchctl load -w "$PLIST_DST"
    echo "launchd service installed and started."
    echo "Logs: tail -f /tmp/vesign.log"

    # Daily pipeline scheduler (Mon–Fri at 17:00 local time)
    DAILY_SRC="$ROOT/deploy/com.vesign.daily.plist"
    DAILY_DST="$HOME/Library/LaunchAgents/com.vesign.daily.plist"
    sed "s|/Users/inovenos/PycharmProjects/Vesign|$ROOT|g" \
        "$DAILY_SRC" > "$DAILY_DST"
    launchctl unload "$DAILY_DST" 2>/dev/null || true
    launchctl load -w "$DAILY_DST"
    echo "Daily pipeline scheduler installed (Mon–Fri 17:00)."
    echo "Pipeline logs: tail -f /tmp/vesign-pipeline.log"
else
    echo "Non-macOS: skipping launchd. Start manually with:"
    echo "  $ROOT/venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8000"
fi

echo ""
echo "Setup complete. Open http://localhost:8000 in your browser."
