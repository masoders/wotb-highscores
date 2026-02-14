#!/usr/bin/env bash
set -euo pipefail

PID_FILE="tankbot.pid"
FORCE_STOP="${FORCE:-${force:-0}}"

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
    # If we cannot inspect cmdline (restricted env), continue with PID-based stop.
    if [[ -z "$cmd" ]]; then
        return 0
    fi
    [[ "$cmd" == *"bot.py"* ]] || [[ "$cmd" == *"tankbot/main.py"* ]] || [[ "$cmd" == *"tankbot.main"* ]]
}

find_running_bot_pids() {
    local pids=""
    local pid=""
    local cmd=""
    pids="$(pgrep -f "$BOT_ENTRY" 2>/dev/null || true)"
    if [[ -z "${pids//[[:space:]]/}" ]]; then
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

kill_pid_gracefully() {
    local pid="$1"
    if ! is_pid_running "$pid"; then
        return 0
    fi
    echo "Stopping bot (PID $pid)..."
    kill "$pid" 2>/dev/null || true
    for _ in {1..8}; do
        if ! is_pid_running "$pid"; then
            echo "✅ Stopped PID $pid."
            return 0
        fi
        sleep 1
    done
    echo "⚠️  PID $pid did not stop gracefully. Sending SIGKILL..."
    kill -9 "$pid" 2>/dev/null || true
    if ! is_pid_running "$pid"; then
        echo "✅ Stopped PID $pid (forced)."
    fi
}

declare -a targets=()
stopped_count=0
skipped_count=0

if [[ -f "$PID_FILE" ]]; then
    PID="$(tr -d '[:space:]' < "$PID_FILE" || true)"
    if [[ -n "${PID:-}" ]]; then
        targets+=("$PID")
    fi
fi

while IFS= read -r pid; do
    [[ -n "${pid:-}" ]] && targets+=("$pid")
done < <(find_running_bot_pids)

if [[ "${#targets[@]}" -eq 0 ]]; then
    echo "ℹ️  No running bot processes found."
    rm -f "$PID_FILE"
    exit 0
fi

# Deduplicate target PIDs
unique_targets="$(printf "%s\n" "${targets[@]}" | sort -u)"
echo "Found bot PID(s): $(echo "$unique_targets" | tr '\n' ' ' | xargs)"
while IFS= read -r pid; do
    [[ -z "${pid:-}" ]] && continue
    if ! is_expected_process "$pid" && [[ "$FORCE_STOP" != "1" ]]; then
        echo "⚠️  PID $pid is not recognized as expected bot process: $(process_cmdline "$pid")"
        echo "Set FORCE=1 (or force=1) to stop it anyway."
        skipped_count=$((skipped_count + 1))
        continue
    fi
    if is_pid_running "$pid"; then
        kill_pid_gracefully "$pid"
        if ! is_pid_running "$pid"; then
            stopped_count=$((stopped_count + 1))
        fi
    fi
done <<< "$unique_targets"

rm -f "$PID_FILE"
if [[ "$stopped_count" -eq 0 && "$skipped_count" -gt 0 ]]; then
    echo "⚠️  No processes were stopped."
    exit 1
fi
echo "✅ Stop routine completed."
