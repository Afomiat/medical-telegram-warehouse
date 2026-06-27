import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ── logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("logs/loader.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── database connection ───────────────────────────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
engine = create_engine(DB_URL)

# ── create raw schema and table ───────────────────────────────────────────
CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id    BIGINT,
    channel_name  TEXT,
    message_date  TIMESTAMPTZ,
    message_text  TEXT,
    has_media     BOOLEAN,
    image_path    TEXT,
    views         INTEGER,
    forwards      INTEGER,
    scraped_at    TIMESTAMPTZ DEFAULT NOW()
);
"""

# ── insert query ──────────────────────────────────────────────────────────
INSERT_MSG = """
INSERT INTO raw.telegram_messages (
    message_id, channel_name, message_date,
    message_text, has_media, image_path, views, forwards
) VALUES (
    :message_id, :channel_name, :message_date,
    :message_text, :has_media, :image_path, :views, :forwards
)
ON CONFLICT DO NOTHING;
"""


def load_json_files() -> None:
    data_dir = Path("data/raw/telegram_messages")
    json_files = list(data_dir.glob("**/*.json"))

    if not json_files:
        log.error("No JSON files found in data/raw/telegram_messages")
        return

    log.info(f"Found {len(json_files)} JSON files to load")

    with engine.connect() as conn:
        # create schema and table if they don't exist
        conn.execute(text(CREATE_SCHEMA))
        conn.execute(text(CREATE_TABLE))
        conn.commit()
        log.info("Schema and table ready")

        total_inserted = 0

        for json_file in sorted(json_files):
            with open(json_file, encoding="utf-8") as f:
                records = json.load(f)

            inserted = 0
            for record in records:
                conn.execute(text(INSERT_MSG), record)
                inserted += 1

            conn.commit()
            log.info(f"  Loaded {inserted:>4} records from {json_file}")
            total_inserted += inserted

    log.info(f"Done — {total_inserted} total records loaded into raw.telegram_messages")


if __name__ == "__main__":
    load_json_files()