#!/bin/bash
# Scraper auto-restart wrapper with Telegram alerting
# Usage: ./run_forever.sh [--clean]
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

source "$SCRIPT_DIR/venv/bin/activate"

RESTART_COUNT=0
MAX_RESTARTS=20
EXTRA_ARGS="$@"

# Exponential backoff: 30s, 60s, 120s, 300s, 600s (cap)
get_wait_time() {
    local count=$1
    local wait=30
    for ((i=1; i<count && i<5; i++)); do
        wait=$((wait * 2))
    done
    if [ $wait -gt 600 ]; then
        wait=600
    fi
    echo $wait
}

send_telegram() {
    local TOKEN=$(grep TELEGRAM_BOT_TOKEN .env | cut -d= -f2)
    local CHAT=$(grep TELEGRAM_CHAT_ID .env | cut -d= -f2)
    curl -s -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
        -d chat_id="${CHAT}" \
        -d text="$1" \
        -d parse_mode="HTML" > /dev/null 2>&1
}

cleanup_chrome() {
    # Kill orphaned Chrome processes from this scraper
    local SCRAPER_NAME=$(basename "$SCRIPT_DIR")
    pkill -f "chrome.*$SCRAPER_NAME" 2>/dev/null
    pkill -f "chromedriver.*$SCRAPER_NAME" 2>/dev/null
    # Also kill any zombie chrome processes owned by this user
    pkill -f "chrome.*--headless" 2>/dev/null
    sleep 2
}

while [ $RESTART_COUNT -lt $MAX_RESTARTS ]; do
    echo "$(date) — Starting pipeline (restart #$RESTART_COUNT)..."

    python3 run_all.py $EXTRA_ARGS
    EXIT_CODE=$?

    # Only pass --clean on first run (if specified)
    EXTRA_ARGS=""

    if [ $EXIT_CODE -eq 0 ]; then
        echo "$(date) — Pipeline finished successfully."
        date +%s > "$SCRIPT_DIR/pipeline_complete.marker"
        send_telegram "🏁 <b>Pipeline finished successfully</b> (after $RESTART_COUNT restarts). Syncing to Supabase..."

        # Sync to benchmark_businesses immediately (before backup gets overwritten)
        echo "$(date) — Running Supabase sync..."
        sudo -u claude-bot /home/claude-bot/tools/scraper2-to-supabase.sh >> "$SCRIPT_DIR/run_all_log.txt" 2>&1
        if [ $? -eq 0 ]; then
            echo "$(date) — Supabase sync OK"
            send_telegram "📊 <b>Scraper 2 synced to Supabase</b>"
        else
            echo "$(date) — Supabase sync FAILED"
            send_telegram "⚠️ <b>Scraper 2 sync failed</b> — will retry on nightly cron"
        fi
        break
    fi

    RESTART_COUNT=$((RESTART_COUNT + 1))
    WAIT=$(get_wait_time $RESTART_COUNT)

    # Clean up orphaned Chrome processes before restart
    cleanup_chrome

    send_telegram "⚠️ <b>Pipeline crashed</b> (exit code $EXIT_CODE)
Restart #$RESTART_COUNT/$MAX_RESTARTS in ${WAIT}s..."

    echo "$(date) — Pipeline crashed (exit $EXIT_CODE). Restart #$RESTART_COUNT in ${WAIT}s..."
    sleep $WAIT
done

if [ $RESTART_COUNT -ge $MAX_RESTARTS ]; then
    send_telegram "🔴 <b>Pipeline gave up</b> after $MAX_RESTARTS restarts!"
fi
