# Ethiopian Medical Data Pipeline
> End-to-end ELT pipeline: Telegram scraping → PostgreSQL → dbt star schema → YOLOv8 enrichment → FastAPI → Dagster orchestration

Built for **Kara Solutions** as part of the **10 Academy AI Engineering Track — Week 8**.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Project Structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Quick Start](#6-quick-start)
7. [Task 1 — Data Scraping](#7-task-1--data-scraping)
8. [Task 2 — Data Modeling with dbt](#8-task-2--data-modeling-with-dbt)
9. [Task 3 — Image Enrichment with YOLOv8](#9-task-3--image-enrichment-with-yolov8)
10. [Task 4 — Analytical API](#10-task-4--analytical-api)
11. [Task 5 — Pipeline Orchestration](#11-task-5--pipeline-orchestration)
12. [Environment Variables](#12-environment-variables)
13. [Data Schema](#13-data-schema)
14. [API Endpoints](#14-api-endpoints)
15. [Running Tests](#15-running-tests)
16. [Key Findings](#16-key-findings)
17. [Known Limitations](#17-known-limitations)

---

## 1. Project Overview

This project builds a production-grade data platform that generates actionable insights about Ethiopian medical businesses using data scraped from public Telegram channels.

**Business questions answered:**
- What are the top 10 most frequently mentioned medical products across all channels?
- How does posting activity vary across channels over time?
- Which channels use more visual content?
- What types of images do medical channels post (product displays, promotional, lifestyle)?

**Data sources — public Telegram channels:**
| Channel | Category |
|---|---|
| CheMed123 | Medical Products |
| lobelia4cosmetics | Cosmetics & Health |
| tikvahpharma | Pharmaceuticals |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE FLOW                          │
│                                                                 │
│  Telegram API                                                   │
│      │                                                          │
│      ▼                                                          │
│  [EXTRACT]  src/scraper.py (Telethon)                          │
│      │      - Messages, metadata, images                        │
│      │                                                          │
│      ▼                                                          │
│  [DATA LAKE]  data/raw/telegram_messages/YYYY-MM-DD/           │
│               data/raw/images/{channel}/{message_id}.jpg        │
│      │                                                          │
│      ▼                                                          │
│  [LOAD]  scripts/load_raw_to_postgres.py                       │
│      │   raw.telegram_messages (PostgreSQL)                     │
│      │                                                          │
│      ▼                                                          │
│  [TRANSFORM]  dbt (medical_warehouse/)                         │
│      │        staging.stg_telegram_messages                     │
│      │        marts.dim_channels                                │
│      │        marts.dim_dates                                   │
│      │        marts.fct_messages                                │
│      │                                                          │
│      ▼                                                          │
│  [ENRICH]  src/yolo_detect.py (YOLOv8 nano)                   │
│      │     marts.fct_image_detections                           │
│      │                                                          │
│      ▼                                                          │
│  [SERVE]  api/main.py (FastAPI)                                │
│           http://localhost:8000/docs                             │
│                                                                 │
│  [ORCHESTRATE]  pipeline.py (Dagster)                          │
│                 http://localhost:3000                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Scraping | Telethon | 1.36.0 |
| Database | PostgreSQL | 15 |
| Container | Docker + Docker Compose | latest |
| Transformation | dbt-postgres | 1.7.0 |
| dbt utilities | dbt-utils | 1.1.1 |
| Object Detection | Ultralytics YOLOv8 | latest |
| API Framework | FastAPI | 0.109.0 |
| API Server | Uvicorn | 0.27.0 |
| ORM | SQLAlchemy | 2.0.23 |
| Orchestration | Dagster + dagster-webserver | 1.5.14 |
| Data Validation | Pydantic | v2 |
| Environment | python-dotenv | 1.0.0 |

---

## 4. Project Structure

```
medical-telegram-warehouse/
│
├── .env                          # Secrets — DO NOT COMMIT
├── .gitignore
├── docker-compose.yml            # PostgreSQL container
├── requirements.txt
├── pipeline.py                   # Dagster orchestration
├── README.md
│
├── src/
│   ├── __init__.py
│   ├── scraper.py                # Telegram scraper (Task 1)
│   └── yolo_detect.py           # YOLOv8 object detection (Task 3)
│
├── scripts/
│   ├── load_raw_to_postgres.py   # Data lake → PostgreSQL (Task 2)
│   └── load_yolo_to_postgres.py  # YOLO results → PostgreSQL (Task 3)
│
├── medical_warehouse/            # dbt project (Task 2)
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_telegram_messages.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── dim_channels.sql
│   │       ├── dim_dates.sql
│   │       ├── fct_messages.sql
│   │       └── fct_image_detections.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── tests/
│       ├── assert_no_future_messages.sql
│       └── assert_positive_views.sql
│
├── api/
│   ├── __init__.py
│   ├── main.py                   # FastAPI app (Task 4)
│   ├── database.py               # SQLAlchemy connection
│   └── schemas.py                # Pydantic models
│
├── data/
│   ├── raw/
│   │   ├── telegram_messages/    # JSON data lake (partitioned by date)
│   │   │   └── YYYY-MM-DD/
│   │   │       └── channel_name.json
│   │   └── images/               # Downloaded images
│   │       └── {channel_name}/
│   │           └── {message_id}.jpg
│   └── yolo_detections.csv       # YOLO results
│
├── logs/
│   ├── scraper.log
│   ├── loader.log
│   └── yolo.log
│
├── notebooks/
│   └── __init__.py
└── tests/
    └── __init__.py
```

---

## 5. Prerequisites

- Python 3.11+
- Docker Desktop (running)
- Git
- A Telegram account
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)

---

## 6. Quick Start

### Step 1 — Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/medical-telegram-warehouse.git
cd medical-telegram-warehouse
```

### Step 2 — Create virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Set up environment variables

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values (see [Environment Variables](#12-environment-variables)).

### Step 5 — Start PostgreSQL

```bash
docker compose up -d
```

### Step 6 — Run the full pipeline manually

```bash
# Task 1 — Scrape Telegram
python src/scraper.py

# Task 2 — Load to PostgreSQL
python scripts/load_raw_to_postgres.py

# Task 2 — Run dbt transformations
cd medical_warehouse
dbt deps
dbt run
dbt test
cd ..

# Task 3 — Run YOLO detection
python src/yolo_detect.py
python scripts/load_yolo_to_postgres.py

# Task 4 — Start API
uvicorn api.main:app --reload --port 8000

# Task 5 — Run via Dagster (optional — runs everything above automatically)
dagster dev -f pipeline.py
```

### Step 7 — Or run everything with Dagster

```bash
dagster dev -f pipeline.py
# Open http://localhost:3000
# Click Launchpad → Launch Run
```

---

## 7. Task 1 — Data Scraping

**Script:** `src/scraper.py`

Scrapes public Ethiopian medical Telegram channels using the Telethon Python library.

**What it does:**
- Connects to Telegram using API credentials from `.env`
- Loops through all messages in each channel (up to 500 per run)
- Downloads images when a message contains a photo
- Saves messages as date-partitioned JSON files
- Logs all activity to `logs/scraper.log`

**Run:**
```bash
python src/scraper.py
```

**Output:**
```
data/raw/telegram_messages/
    2026-06-27/
        CheMed123.json
        lobelia4cosmetics.json
        tikvahpharma.json
    2026-06-26/
        ...

data/raw/images/
    CheMed123/
        76.jpg
        75.jpg
    lobelia4cosmetics/
        ...
```

**Data collected per message:**
| Field | Description |
|---|---|
| message_id | Unique message identifier |
| channel_name | Telegram channel name |
| message_date | ISO 8601 timestamp |
| message_text | Full text content |
| has_media | Whether message has a photo |
| image_path | Local path to downloaded image |
| views | View count |
| forwards | Forward count |

**Results:** 1,076 messages, 732 images across 3 channels.

---

## 8. Task 2 — Data Modeling with dbt

### Load raw data

**Script:** `scripts/load_raw_to_postgres.py`

Reads all JSON files from the data lake and loads them into `raw.telegram_messages` in PostgreSQL.

```bash
python scripts/load_raw_to_postgres.py
```

### dbt transformations

```bash
cd medical_warehouse
dbt deps          # install dbt-utils package
dbt run           # build all models
dbt test          # run all 15 tests
dbt docs generate # generate documentation
dbt docs serve    # view docs at http://localhost:8080
```

### Star Schema

```
                    ┌──────────────────┐
                    │   dim_channels   │
                    │  (3 rows)        │
                    │  PK: channel_key │
                    └────────┬─────────┘
                             │
┌──────────────┐   ┌────────▼─────────┐
│  dim_dates   │   │   fct_messages   │
│  (1,392 rows)│◄──│  (1,076 rows)    │
│  PK: date_key│   │  FK: channel_key │
└──────────────┘   │  FK: date_key    │
                   └──────────────────┘
                             │
                   ┌─────────▼────────────┐
                   │  fct_image_detections│
                   │  (732 rows)          │
                   │  FK: message_id      │
                   └──────────────────────┘
```

### dbt Models

| Model | Schema | Type | Rows | Description |
|---|---|---|---|---|
| stg_telegram_messages | staging | View | 1,076 | Cleaned, typed staging layer |
| dim_channels | marts | Table | 3 | One row per channel with stats |
| dim_dates | marts | Table | 1,392 | Full date spine |
| fct_messages | marts | Table | 1,076 | Central fact table |
| fct_image_detections | marts | Table | 732 | YOLO results joined to messages |

### Tests — 15/15 Passing

```bash
dbt test
# Done. PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=15
```

Tests include: `unique`, `not_null`, `relationships` (foreign keys), and 2 custom business rule tests:
- `assert_no_future_messages` — no message dates in the future
- `assert_positive_views` — no negative view counts

---

## 9. Task 3 — Image Enrichment with YOLOv8

**Script:** `src/yolo_detect.py`

Runs YOLOv8 nano object detection on all downloaded images and classifies each image.

```bash
python src/yolo_detect.py
python scripts/load_yolo_to_postgres.py
```

### Classification Scheme

| Category | Logic | Count | % |
|---|---|---|---|
| other | No recognizable objects | 398 | 54.4% |
| product_display | Bottle/container, no person | 252 | 34.4% |
| lifestyle | Person, no product | 74 | 10.1% |
| promotional | Person + product | 8 | 1.1% |

**Output:** `data/yolo_detections.csv`

Results are loaded into `raw.yolo_detections` and exposed via the `fct_image_detections` dbt mart model.

---

## 10. Task 4 — Analytical API

**Script:** `api/main.py`

```bash
uvicorn api.main:app --reload --port 8000
```

Interactive docs available at: **http://localhost:8000/docs**

### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | API root — lists all endpoints |
| GET | `/api/channels` | All channels with summary stats |
| GET | `/api/reports/top-products?limit=10` | Most mentioned terms across all messages |
| GET | `/api/channels/{channel_name}/activity` | Daily posting activity for one channel |
| GET | `/api/search/messages?query=paracetamol` | Full-text keyword search |
| GET | `/api/reports/visual-content` | Image category stats per channel |

### Example Response — Top Products

```json
[
  { "term": "cream", "mention_count": 187 },
  { "term": "price", "mention_count": 143 },
  { "term": "birr", "mention_count": 121 }
]
```

### Example Response — Visual Content

```json
[
  {
    "channel_name": "tikvahpharma",
    "image_category": "product_display",
    "image_count": 180,
    "avg_views": 1243.5
  }
]
```

---

## 11. Task 5 — Pipeline Orchestration

**Script:** `pipeline.py`

```bash
dagster dev -f pipeline.py
# Open http://localhost:3000
```

### Pipeline Graph

```
scrape_telegram_data
        │
        ▼
load_raw_to_postgres
        │
        ▼
run_dbt_transformations
        │
        ▼
run_yolo_enrichment
```

### Schedule

The pipeline runs automatically every day at **6:00 AM UTC**.

```python
cron_schedule = "0 6 * * *"
```

### Running manually via Dagster UI

1. Open http://localhost:3000
2. Click `medical_data_pipeline`
3. Click **Launchpad** tab
4. Click **Launch Run**
5. Monitor progress in the **Runs** tab

---

## 12. Environment Variables

Create a `.env` file in the project root:

```env
# Telegram API credentials (from my.telegram.org)
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=+251xxxxxxxxx

# PostgreSQL (Docker)
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5433
POSTGRES_DB=medical_warehouse
POSTGRES_USER=warehouse_user
POSTGRES_PASSWORD=warehouse_pass
```

> **Never commit `.env` to Git.** It is already in `.gitignore`.

---

## 13. Data Schema

### raw.telegram_messages

```sql
CREATE TABLE raw.telegram_messages (
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
```

### raw.yolo_detections

```sql
CREATE TABLE raw.yolo_detections (
    message_id        TEXT,
    channel_name      TEXT,
    image_path        TEXT,
    detected_objects  TEXT,
    image_category    TEXT,
    object_count      INTEGER
);
```

### marts.fct_messages (key columns)

```sql
message_id    BIGINT      -- message identifier
channel_key   TEXT        -- FK to dim_channels
date_key      INTEGER     -- FK to dim_dates (YYYYMMDD)
message_text  TEXT
message_length INTEGER
views         INTEGER
forwards      INTEGER
has_image     BOOLEAN
```

---

## 14. API Endpoints

Full interactive documentation: **http://localhost:8000/docs**

```bash
# Get all channels
curl http://localhost:8000/api/channels

# Top 10 products
curl http://localhost:8000/api/reports/top-products?limit=10

# Channel activity
curl http://localhost:8000/api/channels/tikvahpharma/activity

# Search messages
curl "http://localhost:8000/api/search/messages?query=paracetamol&limit=20"

# Visual content stats
curl http://localhost:8000/api/reports/visual-content
```

---

## 15. Running Tests

### dbt tests

```bash
cd medical_warehouse
dbt test
```

### Unit tests

```bash
pytest tests/
```

### GitHub Actions

Tests run automatically on every push via `.github/workflows/unittests.yml`.

---

## 16. Key Findings

- **1,076 messages** collected across 3 channels over the last 4 years
- **tikvahpharma** and **lobelia4cosmetics** are the most active channels (500 messages each)
- **34.4%** of images show product displays — channels focus heavily on product photography
- Only **1.1%** of images are promotional (person + product) — local medical channels prefer product-only content
- Most common detected objects: bottles, cups, and containers
- **54.4%** of images could not be classified by the general YOLOv8 model — likely Amharic text graphics and locally-specific packaging

---

## 17. Known Limitations

| Limitation | Impact | Potential Fix |
|---|---|---|
| Scraper limited to 500 messages per channel | Older messages may be missed | Implement incremental scraping with last message ID tracking |
| YOLOv8 trained on general COCO objects | 54% of images unclassified | Fine-tune YOLOv8 on labeled Ethiopian medical images |
| No API authentication | Not production-ready | Add API key or OAuth2 authentication |
| Docker unstable on Windows | Setup friction | Deploy on Linux server or cloud VM |
| PostgreSQL runs locally | Not accessible externally | Deploy to managed PostgreSQL (Supabase, RDS, Neon) |

---

## Author

**Afomia Tadesse**
10 Academy — AI Engineering Track, Week 8
June 2026

---

## License

This project is for educational purposes as part of the 10 Academy AI Engineering program.