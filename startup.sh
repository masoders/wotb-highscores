#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="tankbot"
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=11
PID_FILE="tankbot.pid"
LOG_FILE="tankbot.log"
BOOT_LOG_FILE="startup.out"

echo "=== ${PROJECT_NAME} startup ==="
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
BOT_ENTRY="$PROJECT_DIR/bot.py"

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
    # Some environments block `ps` for non-privileged callers.
    # If we can't inspect cmdline, trust PID liveness.
    if [[ -z "$cmd" ]]; then
        return 0
    fi
    [[ "$cmd" == *"python"* ]] && [[ "$cmd" == *"bot.py"* ]]
}

find_running_bot_pids() {
    local pids=""
    local pid=""
    local cmd=""
    # Preferred path: pgrep exact entry match.
    pids="$(pgrep -f "$BOT_ENTRY" 2>/dev/null || true)"
    if [[ -z "${pids//[[:space:]]/}" ]]; then
        # Fallback for environments where pgrep cannot enumerate processes.
        pids="$(
            ps ax -o pid= -o command= 2>/dev/null \
            | awk -v entry="$BOT_ENTRY" -v proj="$PROJECT_DIR" '
                index($0, entry) > 0 { print $1; next }
                /python/ && /bot\.py/ && index($0, proj) > 0 { print $1 }
            ' || true
        )"
    fi
    while IFS= read -r pid; do
        [[ -z "${pid:-}" ]] && continue
        pid="$(awk '{print $1}' <<<"$pid")"
        [[ -z "${pid:-}" ]] && continue
        is_pid_running "$pid" || continue
        cmd="$(process_cmdline "$pid")"
        [[ -z "${cmd:-}" ]] && continue
        if [[ "$cmd" == *"$BOT_ENTRY"* ]] || { [[ "$cmd" == *"python"* ]] && [[ "$cmd" == *"bot.py"* ]] && [[ "$cmd" == *"$PROJECT_DIR"* ]]; }; then
            echo "$pid"
        fi
    done < <(printf "%s\n" "$pids" | awk 'NF {print $1}' | sort -u)
}

running_pids="$(find_running_bot_pids | tr '\n' ' ' | xargs || true)"
if [[ -n "${running_pids:-}" ]]; then
    count="$(wc -w <<<"$running_pids" | tr -d '[:space:]')"
    if [[ "$count" -gt 1 ]]; then
        echo "❌ Multiple bot processes are already running: $running_pids"
        echo "Run ./stop.sh first to clean old processes, then start again."
        exit 1
    fi
    only_pid="$(awk '{print $1}' <<<"$running_pids")"
    echo "ℹ️  Bot already running (PID $only_pid)."
    echo "$only_pid" > "$PID_FILE"
    exit 0
fi

if [[ -f "$PID_FILE" ]]; then
    existing_pid="$(tr -d '[:space:]' < "$PID_FILE" || true)"
    if [[ -n "${existing_pid:-}" ]] && is_pid_running "$existing_pid" && is_expected_process "$existing_pid"; then
        echo "ℹ️  Bot already running (PID $existing_pid)."
        exit 0
    fi
    echo "ℹ️  Removing stale PID file."
    rm -f "$PID_FILE"
fi

# 1. Find Python
if command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
else
    echo "❌ python3 not found"
    exit 1
fi

PY_VERSION=$($PYTHON - <<EOF
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
EOF
)

read -r PY_MAJOR PY_MINOR <<<"$($PYTHON - <<EOF
import sys
print(sys.version_info.major, sys.version_info.minor)
EOF
)"

if (( PY_MAJOR < PYTHON_MIN_MAJOR )) || { (( PY_MAJOR == PYTHON_MIN_MAJOR )) && (( PY_MINOR < PYTHON_MIN_MINOR )); }; then
    echo "❌ Python ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR}+ required, found $PY_VERSION"
    exit 1
fi

echo "✅ Python $PY_VERSION detected"

# 2. Create venv if missing
if [[ ! -d "venv" ]]; then
    echo "Creating virtual environment..."
    $PYTHON -m venv venv
fi

# 3. Activate venv
source venv/bin/activate

echo "✅ Virtual environment active"

# 4. Upgrade pip
pip install --upgrade pip >/dev/null

# 5. Install dependencies
if [[ -f "requirements.txt" ]]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "❌ requirements.txt missing"
    exit 1
fi

# 6. Environment sanity checks
python - <<EOF
import sys
import discord
print("discord.py version:", discord.__version__)
EOF

echo
echo "✅ Environment ready. Launching bot in background..."
echo "Using Python rotating logs via LOG_PATH/LOG_MAX_BYTES/LOG_BACKUP_COUNT."
echo "Startup capture log: $BOOT_LOG_FILE"

: > "$BOOT_LOG_FILE"
nohup python "$BOT_ENTRY" >> "$BOOT_LOG_FILE" 2>&1 &
BOT_PID=$!
echo "$BOT_PID" > "$PID_FILE"

sleep 2
if is_pid_running "$BOT_PID"; then
    if ! is_expected_process "$BOT_PID"; then
        echo "⚠️  Bot is running (PID $BOT_PID), but command-line verification did not match expected pattern."
        echo "    Continuing because the process is alive."
    fi
    echo "✅ Bot started in background."
    echo "PID: $BOT_PID"
    echo "PID file: $PID_FILE"
    echo "Log file: $LOG_FILE"
else
    echo "❌ Bot failed to stay running."
    echo "Check: $BOOT_LOG_FILE"
    echo "Recent output:"
    tail -n 40 "$BOOT_LOG_FILE" || true
    rm -f "$PID_FILE"
    exit 1
fi
