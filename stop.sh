#!/usr/bin/env bash
set -euo pipefail

PID_FILE="tankbot.pid"

cd "$(dirname "$0")"

is_pid_running() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null
}

process_cmdline() {
    local pid="$1"
    ps -p "$pid" -o command= 2>/dev/null || true
}

is_expected_process() {
    local pid="$1"
    local cmd
    cmd="$(process_cmdline "$pid")"
    # If we cannot inspect cmdline (restricted env), continue with PID-based stop.
    if [[ -z "$cmd" ]]; then
        return 0
    fi
    [[ "$cmd" == *"bot.py"* ]] || [[ "$cmd" == *"tankbot/main.py"* ]] || [[ "$cmd" == *"tankbot.main"* ]]
}

if [[ ! -f "$PID_FILE" ]]; then
    echo "ℹ️  PID file not found. Bot is not running."
    exit 0
fi

PID="$(tr -d '[:space:]' < "$PID_FILE" || true)"
if [[ -z "${PID:-}" ]]; then
    echo "ℹ️  Empty PID file. Cleaning up."
    rm -f "$PID_FILE"
    exit 0
fi

if ! is_pid_running "$PID"; then
    echo "ℹ️  Process $PID is not running. Cleaning up stale PID file."
    rm -f "$PID_FILE"
    exit 0
fi

if ! is_expected_process "$PID"; then
    echo "⚠️  PID $PID is not the expected bot process. Refusing to kill."
    echo "Process command: $(process_cmdline "$PID")"
    echo "If this is the bot, run: FORCE=1 ./stop.sh"
    if [[ "${FORCE:-0}" != "1" ]]; then
        exit 1
    fi
    echo "⚠️  FORCE=1 set; proceeding to stop PID $PID."
fi

echo "Stopping bot (PID $PID)..."
kill "$PID"

for _ in {1..8}; do
    if ! is_pid_running "$PID"; then
        rm -f "$PID_FILE"
        echo "✅ Bot stopped."
        exit 0
    fi
    sleep 1
done

echo "⚠️  Bot did not stop gracefully. Sending SIGKILL..."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "✅ Bot stopped (forced)."
