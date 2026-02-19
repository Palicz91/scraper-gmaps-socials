"""
Telegram bot for controlling the scraper pipeline.
Commands:
  /locations Miami, Tampa       - set locations.txt (or send .txt file with /locations caption)
  /categories pizza, sushi      - set categories.txt (or send .txt file with /categories caption)
  /run                          - start pipeline in tmux
  /status                       - check if pipeline is running
  /show_locations               - show current locations.txt
  /show_categories              - show current categories.txt
"""

import subprocess
from pathlib import Path
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

TELEGRAM_BOT_TOKEN = "8260583365:AAHAyLxwuuoWHa8XmDwchahsQNrxfZuiGaM"
ALLOWED_CHAT_ID = 1825555416

SCRAPER_DIR = Path.home() / "scraper" / "Scraper"
GMAPS_DIR = SCRAPER_DIR / "20251105 GMaps Scraper"


def is_allowed(update: Update) -> bool:
    return update.effective_chat.id == ALLOWED_CHAT_ID


async def cmd_locations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text:
        await update.message.reply_text("Használat:\n• /locations Miami, Tampa, Orlando\n• Vagy küldj .txt fájlt /locations caption-nel")
        return
    items = [item.strip() for item in text.split(",") if item.strip()]
    filepath = GMAPS_DIR / "locations.txt"
    filepath.write_text("\n".join(items) + "\n", encoding="utf-8")
    await update.message.reply_text(f"✅ locations.txt frissítve ({len(items)} lokáció):\n" + "\n".join(items))


async def cmd_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text:
        await update.message.reply_text("Használat:\n• /categories pizza, sushi\n• Vagy küldj .txt fájlt /categories caption-nel")
        return
    items = [item.strip() for item in text.split(",") if item.strip()]
    filepath = GMAPS_DIR / "categories.txt"
    filepath.write_text("\n".join(items) + "\n", encoding="utf-8")
    await update.message.reply_text(f"✅ categories.txt frissítve ({len(items)} kategória):\n" + "\n".join(items))


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    doc = update.message.document
    caption = (update.message.caption or "").strip().lower()

    if not doc or not doc.file_name.endswith(".txt"):
        await update.message.reply_text("⚠️ Csak .txt fájlt fogadok /locations vagy /categories caption-nel.")
        return

    if caption not in ("/locations", "/categories"):
        await update.message.reply_text("⚠️ Caption legyen /locations vagy /categories")
        return

    file = await doc.get_file()
    content = (await file.download_as_bytearray()).decode("utf-8").strip()
    lines = [line.strip() for line in content.splitlines() if line.strip()]

    if caption == "/locations":
        target = GMAPS_DIR / "locations.txt"
        label = "lokáció"
    else:
        target = GMAPS_DIR / "categories.txt"
        label = "kategória"

    target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    preview = "\n".join(lines[:10])
    suffix = f"\n... és még {len(lines) - 10}" if len(lines) > 10 else ""
    await update.message.reply_text(f"✅ {target.name} frissítve ({len(lines)} {label}):\n{preview}{suffix}")


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    result = subprocess.run(["tmux", "has-session", "-t", "scraper"], capture_output=True)
    if result.returncode == 0:
        await update.message.reply_text("⚠️ Pipeline már fut! /status -szal ellenőrizheted.")
        return
    cmd = f"cd {SCRAPER_DIR} && git pull && source venv/bin/activate && python3 run_all.py"
    subprocess.run(["tmux", "new-session", "-d", "-s", "scraper", "bash", "-c", cmd])
    await update.message.reply_text("🚀 Pipeline elindítva.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    result = subprocess.run(["tmux", "has-session", "-t", "scraper"], capture_output=True)
    if result.returncode == 0:
        # Grab last 10 lines from tmux pane
        log = subprocess.run(
            ["tmux", "capture-pane", "-t", "scraper", "-p", "-S", "-10"],
            capture_output=True, text=True
        )
        lines = log.stdout.strip()
        msg = "🟢 Pipeline fut.\n\n<pre>" + (lines[-3000:] if lines else "Nincs log") + "</pre>"
        await update.message.reply_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_text("⚪ Nincs futó pipeline.")


async def cmd_show_locations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    filepath = GMAPS_DIR / "locations.txt"
    if filepath.exists():
        content = filepath.read_text(encoding="utf-8").strip()
        if len(content) > 4000:
            await update.message.reply_document(document=open(filepath, "rb"), caption="📍 locations.txt")
        else:
            await update.message.reply_text(f"📍 locations.txt:\n{content}")
    else:
        await update.message.reply_text("⚠️ locations.txt nem található.")


async def cmd_show_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    filepath = GMAPS_DIR / "categories.txt"
    if filepath.exists():
        content = filepath.read_text(encoding="utf-8").strip()
        if len(content) > 4000:
            await update.message.reply_document(document=open(filepath, "rb"), caption="📂 categories.txt")
        else:
            await update.message.reply_text(f"📂 categories.txt:\n{content}")
    else:
        await update.message.reply_text("⚠️ categories.txt nem található.")


if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("locations", cmd_locations))
    app.add_handler(CommandHandler("categories", cmd_categories))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("show_locations", cmd_show_locations))
    app.add_handler(CommandHandler("show_categories", cmd_show_categories))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    print("🤖 Bot elindult.")
    app.run_polling()
