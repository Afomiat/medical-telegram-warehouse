import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

load_dotenv()

# ── credentials from .env ──────────────────────────────────────────────────
API_ID   = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE    = os.getenv("TELEGRAM_PHONE")

# ── channels to scrape ────────────────────────────────────────────────────
CHANNELS = [
    "CheMed123",
    "lobelia4cosmetics",
    "tikvahpharma",
]

# ── output paths ──────────────────────────────────────────────────────────
DATA_DIR = Path("data/raw/telegram_messages")
IMG_DIR  = Path("data/raw/images")
LOG_DIR  = Path("logs")

# ── logging setup ─────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "scraper.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


async def scrape_channel(client: TelegramClient, channel: str) -> None:
    """Scrape all messages from one channel and save them."""
    log.info(f"Starting channel: {channel}")
    messages_by_date: dict[str, list] = {}

    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        log.error(f"Could not find channel '{channel}': {e}")
        return

    async for message in client.iter_messages(entity, limit=500):
        # ── build the record ──────────────────────────────────────────
        date_str = message.date.strftime("%Y-%m-%d")

        has_photo = isinstance(message.media, MessageMediaPhoto)
        image_path = None

        # ── download image if present ─────────────────────────────────
        if has_photo:
            img_folder = IMG_DIR / channel
            img_folder.mkdir(parents=True, exist_ok=True)
            img_file = img_folder / f"{message.id}.jpg"
            try:
                await client.download_media(message.media, file=str(img_file))
                image_path = str(img_file)
                log.info(f"  Downloaded image: {img_file}")
            except Exception as e:
                log.warning(f"  Failed to download image for msg {message.id}: {e}")

        record = {
            "message_id":   message.id,
            "channel_name": channel,
            "message_date": message.date.isoformat(),
            "message_text": message.text or "",
            "has_media":    has_photo,
            "image_path":   image_path,
            "views":        message.views or 0,
            "forwards":     message.forwards or 0,
        }

        # ── group by date ─────────────────────────────────────────────
        if date_str not in messages_by_date:
            messages_by_date[date_str] = []
        messages_by_date[date_str].append(record)

    # ── save JSON files partitioned by date ───────────────────────────────
    for date_str, records in messages_by_date.items():
        out_dir = DATA_DIR / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{channel}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
        log.info(f"  Saved {len(records)} messages → {out_file}")


async def main() -> None:
    client = TelegramClient("telegram_session", API_ID, API_HASH)
    await client.start(phone=PHONE)
    log.info("Logged in to Telegram")

    for channel in CHANNELS:
        await scrape_channel(client, channel)

    await client.disconnect()
    log.info("Done — all channels scraped")


if __name__ == "__main__":
    asyncio.run(main())