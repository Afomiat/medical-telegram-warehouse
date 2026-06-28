import csv
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("logs/loader.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
engine = create_engine(DB_URL)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.yolo_detections (
    message_id        TEXT,
    channel_name      TEXT,
    image_path        TEXT,
    detected_objects  TEXT,
    image_category    TEXT,
    object_count      INTEGER
);
"""

INSERT_ROW = """
INSERT INTO raw.yolo_detections (
    message_id, channel_name, image_path,
    detected_objects, image_category, object_count
) VALUES (
    :message_id, :channel_name, :image_path,
    :detected_objects, :image_category, :object_count
)
ON CONFLICT DO NOTHING;
"""

def load_yolo_results():
    csv_path = Path("data/yolo_detections.csv")
    if not csv_path.exists():
        log.error("data/yolo_detections.csv not found — run yolo_detect.py first")
        return

    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    log.info(f"Found {len(rows)} YOLO detection rows to load")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text(CREATE_TABLE))
        conn.commit()

        for row in rows:
            row["object_count"] = int(row["object_count"])
            conn.execute(text(INSERT_ROW), row)

        conn.commit()

    log.info(f"Done — {len(rows)} rows loaded into raw.yolo_detections")

if __name__ == "__main__":
    load_yolo_results()