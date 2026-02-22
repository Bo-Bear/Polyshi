#!/usr/bin/env bash
set -euo pipefail

# ── Polyshi tmux launcher ────────────────────────────────────────
# Starts the bot inside a tmux session so you can detach and
# reattach without killing the process.
#
# Usage:
#   ./start.sh          Start (or reattach to) the bot
#   ./start.sh stop     Send "stop" to a running session
#
# Inside tmux:
#   Ctrl+B  D           Detach (bot keeps running)
#   tmux attach -t poly Reattach later
# ─────────────────────────────────────────────────────────────────

SESSION="poly"
DIR="$(cd "$(dirname "$0")" && pwd)"

# ── "stop" subcommand: send graceful stop to running session ─────
if [[ "${1:-}" == "stop" ]]; then
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        tmux send-keys -t "$SESSION" "stop" Enter
        echo "Sent 'stop' to session '$SESSION'."
    else
        echo "No tmux session '$SESSION' found."
        exit 1
    fi
    exit 0
fi

# ── Reattach if session already exists ───────────────────────────
if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running — reattaching."
    exec tmux attach -t "$SESSION"
fi

# ── Activate venv if present ─────────────────────────────────────
ACTIVATE=""
if [[ -f "$DIR/venv/bin/activate" ]]; then
    ACTIVATE="source $DIR/venv/bin/activate && "
fi

# ── Launch new tmux session with the bot ─────────────────────────
tmux new-session -d -s "$SESSION" -c "$DIR" "${ACTIVATE}python3 main.py"
tmux set-option -t "$SESSION" -g mouse on
exec tmux attach -t "$SESSION"
