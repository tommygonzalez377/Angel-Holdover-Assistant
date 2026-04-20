#!/bin/bash
# Angel Holdover Assistant — daily launcher (macOS)
# Double-click this file to start the app.

cd "$(dirname "$0")" || exit 1
PROJ_DIR="$(pwd)"

# ── Check setup was completed ────────────────────────────────────────────────
if [ ! -f "$PROJ_DIR/venv/bin/python" ]; then
    osascript -e 'display alert "Setup Required" message "Please run setup.command before launching the app." buttons {"OK"} default button "OK"' 2>/dev/null
    echo "ERROR: Virtual environment not found."
    echo "Please double-click setup.command first."
    read -rp "Press Enter to close..."
    exit 1
fi

# ── Stop any previous server on port 8080 ────────────────────────────────────
PREV=$(lsof -ti:8080 2>/dev/null)
if [ -n "$PREV" ]; then
    echo "Stopping previous server (PID $PREV)..."
    kill -9 $PREV 2>/dev/null
    sleep 0.5
fi

# ── Start the server ─────────────────────────────────────────────────────────
echo "Starting Angel Holdover Assistant..."
"$PROJ_DIR/venv/bin/python" "$PROJ_DIR/launcher.py" &
SERVER_PID=$!

# ── Wait up to 15 seconds for the server to respond ─────────────────────────
echo "Waiting for server..."
READY=0
for i in $(seq 1 30); do
    if curl -sf http://localhost:8766 > /dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.5
done

if [ "$READY" -eq 0 ]; then
    echo ""
    echo "Server did not start in time. Check for errors above."
    wait $SERVER_PID
    read -rp "Press Enter to close..."
    exit 1
fi

# ── Open in default browser ──────────────────────────────────────────────────
open http://localhost:8766

echo ""
echo "======================================================"
echo "  Angel Holdover Assistant is running"
echo "  URL: http://localhost:8766"
echo ""
echo "  Keep this window open while you use the app."
echo "  Close this window (or press Ctrl+C) to stop."
echo "======================================================"
echo ""

# Keep running until server exits (shows live logs)
wait $SERVER_PID
echo ""
echo "Server stopped. You can close this window."
