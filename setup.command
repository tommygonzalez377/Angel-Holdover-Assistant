#!/bin/bash
# Angel Holdover Assistant — one-time setup (macOS)
# Double-click this file to install everything your Mac needs.

# Resolve the folder this script lives in (works when launched from Finder)
cd "$(dirname "$0")" || exit 1
PROJ_DIR="$(pwd)"

echo "======================================================"
echo "  Angel Holdover Assistant  —  Setup"
echo "======================================================"
echo ""

# ── 1. Find Python 3.11+ ──────────────────────────────────────────────────────
PYTHON=""
for cmd in python3.13 python3.12 python3.11 python3 python; do
    if command -v "$cmd" &>/dev/null; then
        maj=$("$cmd" -c "import sys; print(sys.version_info.major)" 2>/dev/null)
        min=$("$cmd" -c "import sys; print(sys.version_info.minor)" 2>/dev/null)
        if [ -n "$maj" ] && [ -n "$min" ] && \
           { [ "$maj" -gt 3 ] || { [ "$maj" -eq 3 ] && [ "$min" -ge 11 ]; }; }; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3.11 or newer not found."
    echo ""
    echo "Please install Python from:"
    echo "  https://www.python.org/downloads/"
    echo ""
    echo "After installing, double-click setup.command again."
    echo ""
    read -rp "Press Enter to close..."
    exit 1
fi

echo "OK  $($PYTHON --version)"
echo ""

# ── 2. Create virtual environment ────────────────────────────────────────────
if [ ! -d "$PROJ_DIR/venv" ]; then
    echo "Creating virtual environment..."
    "$PYTHON" -m venv "$PROJ_DIR/venv" || {
        echo "ERROR: Could not create virtual environment."
        read -rp "Press Enter to close..."
        exit 1
    }
    echo "OK  Virtual environment created"
else
    echo "OK  Virtual environment already exists"
fi

# ── 3. Install Python packages ────────────────────────────────────────────────
echo ""
echo "Installing Python packages (this may take a minute)..."
"$PROJ_DIR/venv/bin/pip" install --quiet --upgrade pip
"$PROJ_DIR/venv/bin/pip" install -r "$PROJ_DIR/requirements.txt" || {
    echo "ERROR: Package installation failed."
    read -rp "Press Enter to close..."
    exit 1
}
echo "OK  Python packages installed"

# ── 4. Install Playwright Chromium browser ───────────────────────────────────
echo ""
echo "Installing Playwright browser (~120 MB download, one-time only)..."
"$PROJ_DIR/venv/bin/playwright" install chromium || {
    echo "ERROR: Playwright browser install failed."
    read -rp "Press Enter to close..."
    exit 1
}
echo "OK  Playwright Chromium installed"

# ── 5. Create .env from template if not present ──────────────────────────────
if [ ! -f "$PROJ_DIR/.env" ]; then
    cp "$PROJ_DIR/.env.example" "$PROJ_DIR/.env"
    echo "OK  Created .env from template"
else
    echo "OK  .env already exists — leaving as-is"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Double-click  start.command  to launch the app"
echo "  2. When the browser opens, click your name in the"
echo "     top-right and go to Profile to save your"
echo "     Comscore and Mica credentials"
echo "======================================================"
echo ""
read -rp "Press Enter to close..."
