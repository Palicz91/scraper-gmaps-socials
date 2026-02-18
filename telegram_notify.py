"""
Telegram notification helper for scraper pipeline.
Usage: from telegram_notify import notify
"""

import requests
import os
from datetime import datetime

# --- CONFIG ---
TELEGRAM_BOT_TOKEN = "8260583365:AAHAyLxwuuoWHa8XmDwchahsQNrxfZuiGaM"
TELEGRAM_CHAT_ID = "1825555416"

def notify(message: str, silent: bool = False) -> bool:
    """Send a Telegram message. Returns True if successful."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_notification": silent,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"⚠️ Telegram notify failed: {e}")
        return False

def stage_done(stage: str, details: str = ""):
    """Notify that a pipeline stage completed."""
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"✅ <b>{stage}</b> kész ({ts})"
    if details:
        msg += f"\n{details}"
    notify(msg)

def stage_failed(stage: str, error: str = ""):
    """Notify that a pipeline stage failed."""
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"🔴 <b>{stage}</b> HIBA ({ts})"
    if error:
        msg += f"\n<code>{error[:500]}</code>"
    notify(msg)

def pipeline_summary(total_places: int = 0, social_found: int = 0, duration_sec: float = 0):
    """Send end-of-pipeline summary."""
    mins = duration_sec / 60
    msg = (
        f"🏁 <b>Pipeline kész</b>\n"
        f"📍 Helyek: {total_places}\n"
        f"📱 Social talált: {social_found}\n"
        f"⏱ Idő: {mins:.1f} perc"
    )
    notify(msg)

def send_file(filepath: str, caption: str = "") -> bool:
    """Send a file via Telegram. Returns True if successful."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(filepath, "rb") as f:
            payload = {"chat_id": TELEGRAM_CHAT_ID}
            if caption:
                payload["caption"] = caption
                payload["parse_mode"] = "HTML"
            r = requests.post(url, data=payload, files={"document": f}, timeout=30)
            return r.status_code == 200
    except Exception as e:
        print(f"⚠️ Telegram file send failed: {e}")
        return False
