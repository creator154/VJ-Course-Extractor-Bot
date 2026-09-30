import logging
import os
import threading

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from pyromod import listen

from config import api_id, api_hash, bot_token
from one import register_pwwp_handlers
from two import register_cpwp_handlers
from three import register_appxwp_handlers


# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# --- WEB HEALTH CHECK SERVER ---
def run_web():
    port = int(os.environ.get("PORT", 8080))

    try:
        from flask import Flask

        app = Flask(__name__)

        @app.route("/")
        def health():
            return "Bot running", 200

        app.run(
            host="0.0.0.0",
            port=port
        )

    except Exception:
        import http.server
        import socketserver

        handler = http.server.SimpleHTTPRequestHandler

        with socketserver.TCPServer(
            ("", port),
            handler
        ) as httpd:
            httpd.serve_forever()


# Start health-check server
threading.Thread(
    target=run_web,
    daemon=True
).start()


# --- BOT CLIENT ---
bot = Client(
    "techvjbot",
    api_id=api_id,
    api_hash=api_hash,
    bot_token=bot_token
)


# =========================================================
# START MENU
# =========================================================

START_IMAGE = "https://files.catbox.moe/vg3vae.jpg"

START_CAPTION = (
    "✦ <b>Zx Extractor</b>\n\n"
    "⚡ Fast • Simple • Powerful\n"
    "📚 PW • Classplus • Appx\n\n"
    "<i>Choose a platform to continue.</i>"
)


START_KEYBOARD = InlineKeyboardMarkup([
    [
        InlineKeyboardButton(
            "👨‍💻 Developer 🇮🇳",
            url="https://t.me/SumitTripathi"
        )
    ],
    [
        InlineKeyboardButton(
            "🚀 Physics Wallah 🚀",
            callback_data="pwwp"
        )
    ],
    [
        InlineKeyboardButton(
            "📘 Classplus 📘",
            callback_data="cpwp"
        )
    ],
    [
        InlineKeyboardButton(
            "📒 Appx 📒",
            callback_data="appxwp"
        )
    ]
])


# =========================================================
# /START
# =========================================================

@bot.on_message(filters.command(["start"]))
async def start(bot: Client, message: Message):

    await message.reply_photo(
        photo=START_IMAGE,
        caption=START_CAPTION,
        reply_markup=START_KEYBOARD
    )


# =========================================================
# /HELP
# =========================================================

@bot.on_message(filters.command(["help"]))
async def help(bot: Client, message: Message):

    await message.reply_text(
        "✦ <b>Zx Extractor</b>\n\n"
        "Use /start to open the extractor menu.",
        parse_mode="html"
    )


# =========================================================
# REGISTER EXTRACTOR HANDLERS
# =========================================================

register_pwwp_handlers(bot)
register_cpwp_handlers(bot)
register_appxwp_handlers(bot)


# =========================================================
# RUN BOT
# =========================================================

if __name__ == "__main__":
    bot.run()
