#!/bin/bash
cd "/home/hello/scraper/Scraper/20251105 Socials Scraper"
source /home/hello/scraper/Scraper/venv/bin/activate

while true; do
    echo "$(date) - Starting scraper..."
    python3 social_media_scraper.py
    EXIT_CODE=$?
    echo "$(date) - Scraper exited with code $EXIT_CODE"

    # Ha nincs progress file, kész vagyunk
    if [ ! -f scraper_progress.txt ]; then
        echo "$(date) - No progress file, scraping complete!"
        break
    fi

    echo "$(date) - Restarting in 10 seconds..."
    pkill -9 -f chromium 2>/dev/null
    sleep 10
done
