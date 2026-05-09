#!/bin/bash
# Scraper 2 pipeline watchdog
# Checks if the pipeline is stuck (no progress for STALE_MINUTES)
# Run via cron every 30 min: */30 * * * * /home/hello/scraper/scraper2/watchdog.sh
#
# What it checks:
# 1. Is run_forever.sh running? If not → alert (pipeline died silently)
# 2. Is places_data.csv being updated? If stale → alert + kill + restart

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

INSTANCE_NAME="scraper2"
STALE_MINUTES=60
DATA_FILE="20251105 GMaps Scraper/places_data.csv"
LOG_FILE="watchdog.log"
LOCK_FILE="/tmp/scraper2_watchdog.lock"
PID_FILE="$SCRIPT_DIR/pipeline.pid"

# Prevent overlapping runs
if [ -f "$LOCK_FILE" ]; then
    LOCK_AGE=$(( $(date +%s) - $(stat -c %Y "$LOCK_FILE") ))
    if [ $LOCK_AGE -lt 300 ]; then
        exit 0
    fi
fi
touch "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

send_telegram() {
    local TOKEN=$(sudo -u hello grep TELEGRAM_BOT_TOKEN "$SCRIPT_DIR/.env" | cut -d= -f2)
    local CHAT=$(sudo -u hello grep TELEGRAM_CHAT_ID "$SCRIPT_DIR/.env" | cut -d= -f2)
    curl -s -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
        -d chat_id="${CHAT}" \
        -d text="$1" \
        -d parse_mode="HTML" > /dev/null 2>&1
}

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') — $1" >> "$LOG_FILE"
}

# Check if pipeline is running via PID file
pipeline_running() {
    if [ -f "$PID_FILE" ]; then
        local PID=$(cat "$PID_FILE")
        # Check if process exists (works cross-user via /proc)
        if [ -d "/proc/$PID" ]; then
            echo "$PID"
            return 0
        fi
        sudo -u hello rm -f "$PID_FILE"
    fi
    # Fallback: check for run_forever.sh with scraper2 cwd
    for PID in $(pgrep -u hello -f "run_forever"); do
        local CWD=$(sudo -u hello readlink /proc/$PID/cwd 2>/dev/null)
        if [[ "$CWD" == *"scraper2"* ]]; then
            echo "$PID" > "$PID_FILE"
            echo "$PID"
            return 0
        fi
    done
    return 1
}

kill_pipeline() {
    # Kill by PID file
    if [ -f "$PID_FILE" ]; then
        local PID=$(cat "$PID_FILE")
        sudo -u hello kill -- -$(ps -o pgid= -p $PID 2>/dev/null | tr -d ' ') 2>/dev/null
        sudo -u hello rm -f "$PID_FILE"
    fi
    # Kill any remaining run_forever/run_all with scraper2 cwd
    for PID in $(pgrep -u hello -f "run_forever|run_all"); do
        local CWD=$(readlink /proc/$PID/cwd 2>/dev/null)
        if [[ "$CWD" == *"scraper2"* ]]; then
            sudo -u hello kill "$PID" 2>/dev/null
        fi
    done
    sleep 3
}

start_pipeline() {
    cd "$SCRIPT_DIR"
    sudo -u hello bash -c "cd $SCRIPT_DIR && nohup ./run_forever.sh >> run_all_log.txt 2>&1 & echo \$! > $PID_FILE"
}

# Check 1: Is pipeline running?
# Check completion marker — do not restart if pipeline finished successfully <24h ago
COMPLETE_MARKER="$SCRIPT_DIR/pipeline_complete.marker"
if [ -f "$COMPLETE_MARKER" ]; then
    MARKER_AGE=$(( $(date +%s) - $(stat -c %Y "$COMPLETE_MARKER") ))
    MARKER_HOURS=$(( MARKER_AGE / 3600 ))
    if [ $MARKER_AGE -lt 86400 ]; then
        log "OK: Pipeline completed ${MARKER_HOURS}h ago, not restarting"
        exit 0
    else
        log "INFO: Completion marker is ${MARKER_HOURS}h old (>24h), allowing restart"
        rm -f "$COMPLETE_MARKER"
    fi
fi

PIPELINE_PID=$(pipeline_running)
if [ -z "$PIPELINE_PID" ]; then
    log "ALERT: Pipeline not running, restarting"
    send_telegram "🔴 <b>Scraper 2 watchdog:</b> Pipeline not running! Restarting..."

    start_pipeline
    log "Restarted pipeline as hello user (PID $(cat $PID_FILE 2>/dev/null))"
    send_telegram "🟢 <b>Scraper 2:</b> Pipeline restarted as hello user"
    exit 0
fi

# Check 2: Is any data file being updated? (multi-stage awareness)
LINKS_FILE="20251105 GMaps Scraper/links.txt"
SOCIAL_FILE="20251105 Socials Scraper/output.csv"
SOCIALS_LOG="20251105 Socials Scraper/scraper.log"

# Find the most recently modified data file across all stages
LATEST_FILE=""
LATEST_AGE=999999
for CHECK_FILE in "$DATA_FILE" "$LINKS_FILE" "$SOCIAL_FILE" "$SOCIALS_LOG"; do
    if [ -f "$CHECK_FILE" ]; then
        AGE=$(( $(date +%s) - $(stat -c %Y "$CHECK_FILE") ))
        if [ $AGE -lt $LATEST_AGE ]; then
            LATEST_AGE=$AGE
            LATEST_FILE="$CHECK_FILE"
        fi
    fi
done

if [ -n "$LATEST_FILE" ]; then
    FILE_AGE_MIN=$(( LATEST_AGE / 60 ))
    if [ -f "$DATA_FILE" ]; then
        ROWS=$(wc -l < "$DATA_FILE")
    else
        ROWS="n/a"
    fi

    if [ $FILE_AGE_MIN -gt $STALE_MINUTES ]; then
        log "ALERT: Pipeline stale for ${FILE_AGE_MIN}m (latest: $(basename "$LATEST_FILE"), rows: ${ROWS}), killing pipeline"
        send_telegram "⚠️ <b>Scraper 2 watchdog:</b> Pipeline stuck!
No progress for ${FILE_AGE_MIN} min (latest: $(basename "$LATEST_FILE"), rows: ${ROWS}).
Killing and restarting..."

        kill_pipeline

        start_pipeline
        log "Restarted pipeline after stale detection (PID $(cat $PID_FILE 2>/dev/null))"
        send_telegram "🟢 <b>Scraper 2:</b> Pipeline restarted in resume mode"
    else
        log "OK: Pipeline active, $(basename "$LATEST_FILE") updated ${FILE_AGE_MIN}m ago (rows: ${ROWS})"
    fi
else
    log "OK: Pipeline running (PID $PIPELINE_PID), no data files yet"
fi
