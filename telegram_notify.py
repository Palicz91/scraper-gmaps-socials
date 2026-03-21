"""
Telegram notification helper for scraper pipeline.
Usage: from telegram_notify import notify
"""

import requests
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "scraper")


def notify(message: str, silent: bool = False) -> bool:
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
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"✅ [{INSTANCE_NAME}] <b>{stage}</b> done ({ts})"
    if details:
        msg += f"\n{details}"
    notify(msg)


def stage_failed(stage: str, error: str = ""):
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"🔴 [{INSTANCE_NAME}] <b>{stage}</b> FAILED ({ts})"
    if error:
        msg += f"\n<code>{error[:500]}</code>"
    notify(msg)


def pipeline_summary(total_places: int = 0, social_found: int = 0, duration_sec: float = 0):
    mins = duration_sec / 60
    msg = (
        f"🏁 [{INSTANCE_NAME}] <b>Pipeline complete</b>\n"
        f"📍 Places: {total_places}\n"
        f"📱 Social found: {social_found}\n"
        f"⏱ Duration: {mins:.1f} min"
    )
    notify(msg)


def send_file(filepath: str, caption: str = "") -> bool:
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
