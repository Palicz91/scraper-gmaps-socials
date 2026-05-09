"""
Telegram bot for controlling the scraper pipeline.
Commands:
  /locations Miami, Tampa       - set locations.txt (or send .txt file with /locations caption)
  /categories pizza, sushi      - set categories.txt (or send .txt file with /categories caption)
  /run                          - start pipeline (asks about review scraping first)
  /status                       - check if pipeline is running
  /show_locations               - show current locations.txt
  /show_categories              - show current categories.txt
"""

import subprocess
import json
import os
import html
from pathlib import Path
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

# Load .env from the same directory as this script
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "scraper")
TMUX_SESSION = os.environ.get("TMUX_SESSION", INSTANCE_NAME)

SCRAPER_DIR = Path(__file__).resolve().parent
GMAPS_DIR = SCRAPER_DIR / "20251105 GMaps Scraper"
RUN_CONFIG_FILE = SCRAPER_DIR / "run_config.json"


def is_allowed(update: Update) -> bool:
    return update.effective_chat.id == ALLOWED_CHAT_ID


async def cmd_locations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text:
        await update.message.reply_text("Usage:\n• /locations Miami, Tampa, Orlando\n• Or send a .txt file with /locations caption")
        return
    items = [item.strip() for item in text.split(",") if item.strip()]
    filepath = GMAPS_DIR / "locations.txt"
    filepath.write_text("\n".join(items) + "\n", encoding="utf-8")
    await update.message.reply_text(f"✅ locations.txt updated ({len(items)} locations):\n" + "\n".join(items))


async def cmd_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text:
        await update.message.reply_text("Usage:\n• /categories pizza, sushi\n• Or send a .txt file with /categories caption")
        return
    items = [item.strip() for item in text.split(",") if item.strip()]
    filepath = GMAPS_DIR / "categories.txt"
    filepath.write_text("\n".join(items) + "\n", encoding="utf-8")
    await update.message.reply_text(f"✅ categories.txt updated ({len(items)} categories):\n" + "\n".join(items))


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    doc = update.message.document
    caption = (update.message.caption or "").strip().lower()

    if not doc or not doc.file_name.endswith(".txt"):
        await update.message.reply_text("Only .txt files accepted with /locations or /categories caption.")
        return

    if caption not in ("/locations", "/categories"):
        await update.message.reply_text("Caption must be /locations or /categories")
        return

    file = await doc.get_file()
    content = (await file.download_as_bytearray()).decode("utf-8").strip()
    lines = [line.strip() for line in content.splitlines() if line.strip()]

    if caption == "/locations":
        target = GMAPS_DIR / "locations.txt"
        label = "locations"
    else:
        target = GMAPS_DIR / "categories.txt"
        label = "categories"

    target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    preview = "\n".join(lines[:10])
    suffix = f"\n... and {len(lines) - 10} more" if len(lines) > 10 else ""
    await update.message.reply_text(f"✅ {target.name} updated ({len(lines)} {label}):\n{preview}{suffix}")


# Default pipeline settings
DEFAULT_PIPELINE_CONFIG = {
    "scrape_reviews": True,
    "email_validation": True,
    "classification": True,
    "email_generation": True,
}

PIPELINE_LABELS = {
    "scrape_reviews": "📊 Review scraping",
    "email_validation": "📧 Email validation",
    "classification": "🎯 Classification",
    "email_generation": "✉️ Email generation",
}


def build_config_message(config: dict) -> str:
    lines = ["🔧 <b>Pipeline beállítások:</b>\n"]
    for key, label in PIPELINE_LABELS.items():
        status = "✅" if config.get(key, True) else "❌"
        lines.append(f"{label}: {status}")
    lines.append("\nKattints egy gombra a ki/bekapcsoláshoz, majd ▶️ START.")
    return "\n".join(lines)


def build_config_keyboard(config: dict) -> InlineKeyboardMarkup:
    buttons = []
    for key, label in PIPELINE_LABELS.items():
        is_on = config.get(key, True)
        toggle_text = f"{'🟢' if is_on else '🔴'} {label.split(' ', 1)[1]}"
        buttons.append([InlineKeyboardButton(toggle_text, callback_data=f"cfg:{key}")])
    buttons.append([
        InlineKeyboardButton("✅ Mind BE", callback_data="cfg:all_on"),
        InlineKeyboardButton("❌ Mind KI", callback_data="cfg:all_off"),
    ])
    buttons.append([InlineKeyboardButton("▶️ START", callback_data="cfg:start")])
    return InlineKeyboardMarkup(buttons)


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    result = subprocess.run(["tmux", "has-session", "-t", TMUX_SESSION], capture_output=True)
    if result.returncode == 0:
        await update.message.reply_text("⚠️ Pipeline already running! Check with /status.")
        return

    config = dict(DEFAULT_PIPELINE_CONFIG)
    context.user_data["pipeline_config"] = config
    await update.message.reply_text(
        build_config_message(config),
        reply_markup=build_config_keyboard(config),
        parse_mode="HTML",
    )


async def handle_run_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or query.from_user.id != ALLOWED_CHAT_ID:
        return
    await query.answer()

    data = query.data
    config = context.user_data.get("pipeline_config", dict(DEFAULT_PIPELINE_CONFIG))

    if data == "cfg:start":
        # Write config and launch pipeline
        RUN_CONFIG_FILE.write_text(json.dumps(config), encoding="utf-8")

        enabled = [PIPELINE_LABELS[k].split(" ", 1)[1] for k, v in config.items() if v]
        disabled = [PIPELINE_LABELS[k].split(" ", 1)[1] for k, v in config.items() if not v]
        summary = "✅ " + ", ".join(enabled) if enabled else ""
        if disabled:
            summary += "\n❌ " + ", ".join(disabled)
        await query.edit_message_text(f"🚀 [{INSTANCE_NAME}] Pipeline started!\n{summary}")

        cmd = f"cd {SCRAPER_DIR} && git pull && source venv/bin/activate && python3 run_all.py"
        subprocess.run(["tmux", "new-session", "-d", "-s", TMUX_SESSION, "bash", "-c", cmd])
        return

    if data == "cfg:all_on":
        for key in config:
            config[key] = True
    elif data == "cfg:all_off":
        for key in config:
            config[key] = False
    elif data.startswith("cfg:"):
        key = data[4:]
        if key in config:
            config[key] = not config[key]

    context.user_data["pipeline_config"] = config
    await query.edit_message_text(
        build_config_message(config),
        reply_markup=build_config_keyboard(config),
        parse_mode="HTML",
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    result = subprocess.run(["tmux", "has-session", "-t", TMUX_SESSION], capture_output=True)
    if result.returncode != 0:
        await update.message.reply_text("⚪ No pipeline running.")
        return
    subprocess.run(["tmux", "send-keys", "-t", TMUX_SESSION, "C-c", ""])
    await update.message.reply_text(f"🛑 [{INSTANCE_NAME}] Stop signal sent.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    here = Path(__file__).resolve().parent

    # 1) nohup launch path: pipeline.pid + run_all_log.txt
    pid_file = here / "pipeline.pid"
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)  # alive check
            log_file = here / "run_all_log.txt"
            tail_text = ""
            if log_file.exists():
                with log_file.open("rb") as f:
                    f.seek(0, 2)
                    size = f.tell()
                    f.seek(max(0, size - 6000))
                    tail_text = f.read().decode("utf-8", errors="replace")
                tail_text = "\n".join(tail_text.splitlines()[-20:])
            safe_tail = html.escape(tail_text) if tail_text else "No log output yet."
            msg = (
                f"🟢 [{INSTANCE_NAME}] Pipeline running (PID {pid}, nohup mode).\n\n"
                f"<pre>{safe_tail}</pre>"
            )
            await update.message.reply_text(msg, parse_mode="HTML")
            return
        except (ValueError, ProcessLookupError, PermissionError, OSError):
            pass  # stale or unreadable PID file → fall through

    # 2) Legacy tmux launch path: tmux session created by /run command
    result = subprocess.run(["tmux", "has-session", "-t", TMUX_SESSION], capture_output=True)
    if result.returncode == 0:
        log = subprocess.run(
            ["tmux", "capture-pane", "-t", TMUX_SESSION, "-p", "-S", "-15"],
            capture_output=True, text=True
        )
        lines = log.stdout.strip()
        msg = f"🟢 [{INSTANCE_NAME}] Pipeline running (tmux mode).\n\n<pre>" + (lines[-3000:] if lines else "No log") + "</pre>"
        await update.message.reply_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_text(f"⚪ [{INSTANCE_NAME}] No pipeline running.")


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
        await update.message.reply_text("⚠️ locations.txt not found.")


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
        await update.message.reply_text("⚠️ categories.txt not found.")


if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("locations", cmd_locations))
    app.add_handler(CommandHandler("categories", cmd_categories))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("show_locations", cmd_show_locations))
    app.add_handler(CommandHandler("show_categories", cmd_show_categories))
    app.add_handler(CallbackQueryHandler(handle_run_callback, pattern="^cfg:"))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    print(f"🤖 [{INSTANCE_NAME}] Bot started.")
    app.run_polling()
