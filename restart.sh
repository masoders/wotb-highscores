#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== tankbot restart ==="
echo "1/2 Stopping bot..."
bash ./stop.sh

echo "2/2 Starting bot..."
bash ./startup.sh

echo "✅ Restart complete."
