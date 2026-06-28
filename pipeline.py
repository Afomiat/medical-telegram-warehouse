import subprocess
import os
from dagster import op, job, schedule, ScheduleDefinition, Definitions
from dotenv import load_dotenv

load_dotenv()


@op
def scrape_telegram_data(context):
    """Op 1 — Scrape messages and images from Telegram channels."""
    context.log.info("Starting Telegram scraper...")
    result = subprocess.run(
        ["python", "src/scraper.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Scraper failed:\n{result.stderr}")
    context.log.info(result.stdout)
    context.log.info("Telegram scraping complete.")


@op
def load_raw_to_postgres(context, after_scrape):
    """Op 2 — Load raw JSON files into PostgreSQL raw schema."""
    context.log.info("Loading raw data to PostgreSQL...")
    result = subprocess.run(
        ["python", "scripts/load_raw_to_postgres.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Loader failed:\n{result.stderr}")
    context.log.info(result.stdout)
    context.log.info("Raw data loaded successfully.")


@op
def run_dbt_transformations(context, after_load):
    """Op 3 — Run dbt models to transform raw data into star schema."""
    context.log.info("Running dbt transformations...")
    result = subprocess.run(
        ["dbt", "run"],
        capture_output=True,
        text=True,
        cwd=os.path.join(os.getcwd(), "medical_warehouse")
    )
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")
    context.log.info(result.stdout)

    # also run dbt tests
    context.log.info("Running dbt tests...")
    test_result = subprocess.run(
        ["dbt", "test"],
        capture_output=True,
        text=True,
        cwd=os.path.join(os.getcwd(), "medical_warehouse")
    )
    if test_result.returncode != 0:
        raise Exception(f"dbt test failed:\n{test_result.stderr}")
    context.log.info(test_result.stdout)
    context.log.info("dbt transformations and tests complete.")


@op
def run_yolo_enrichment(context, after_dbt):
    """Op 4 — Run YOLOv8 object detection on downloaded images."""
    context.log.info("Running YOLO object detection...")
    result = subprocess.run(
        ["python", "src/yolo_detect.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"YOLO detection failed:\n{result.stderr}")
    context.log.info(result.stdout)

    # load results into postgres
    context.log.info("Loading YOLO results to PostgreSQL...")
    load_result = subprocess.run(
        ["python", "scripts/load_yolo_to_postgres.py"],
        capture_output=True,
        text=True
    )
    if load_result.returncode != 0:
        raise Exception(f"YOLO loader failed:\n{load_result.stderr}")
    context.log.info(load_result.stdout)
    context.log.info("YOLO enrichment complete.")


@job
def medical_data_pipeline():
    """Full ELT pipeline: scrape -> load -> transform -> enrich."""
    scrape   = scrape_telegram_data()
    load     = load_raw_to_postgres(after_scrape=scrape)
    dbt      = run_dbt_transformations(after_load=load)
    run_yolo_enrichment(after_dbt=dbt)


# ── daily schedule at 6AM UTC ─────────────────────────────────────────────
daily_schedule = ScheduleDefinition(
    job=medical_data_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)

# ── Dagster definitions — entry point ─────────────────────────────────────
defs = Definitions(
    jobs=[medical_data_pipeline],
    schedules=[daily_schedule],
)