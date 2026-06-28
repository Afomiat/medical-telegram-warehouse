import csv
import logging
from pathlib import Path

from ultralytics import YOLO

# ── logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("logs/yolo.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── paths ─────────────────────────────────────────────────────────────────
IMG_DIR     = Path("data/raw/images")
OUTPUT_CSV  = Path("data/yolo_detections.csv")
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ── YOLO product-related class IDs (COCO dataset labels) ─────────────────
# 0=person, 39=bottle, 41=cup, 43=knife, 44=spoon, 74=clock, 76=scissors
PERSON_CLASSES  = {0}
PRODUCT_CLASSES = {39, 41, 74, 76, 25, 26, 28}  # bottle, cup, and container-like objects

def classify_image(detected_classes: set) -> str:
    """Classify image based on detected object classes."""
    has_person  = bool(detected_classes & PERSON_CLASSES)
    has_product = bool(detected_classes & PRODUCT_CLASSES)

    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


def run_detection() -> None:
    # load YOLOv8 nano — downloads automatically on first run (~6MB)
    model = YOLO("yolov8n.pt")
    log.info("YOLOv8 nano model loaded")

    # find all images
    image_files = list(IMG_DIR.glob("**/*.jpg"))
    if not image_files:
        log.error(f"No images found in {IMG_DIR}")
        return

    log.info(f"Found {len(image_files)} images to process")

    results_rows = []

    for img_path in sorted(image_files):
        # extract channel name and message_id from path
        # path structure: data/raw/images/{channel_name}/{message_id}.jpg
        channel_name = img_path.parent.name
        message_id   = img_path.stem  # filename without extension

        try:
            results = model(str(img_path), verbose=False)
            result  = results[0]

            detected_classes = set()
            detections = []

            for box in result.boxes:
                class_id   = int(box.cls[0])
                confidence = float(box.conf[0])
                class_name = model.names[class_id]

                detected_classes.add(class_id)
                detections.append(f"{class_name}:{confidence:.2f}")

            image_category = classify_image(detected_classes)
            detected_str   = ", ".join(detections) if detections else "none"

            log.info(f"  {img_path.name} → {image_category} | {detected_str}")

            results_rows.append({
                "message_id":      message_id,
                "channel_name":    channel_name,
                "image_path":      str(img_path),
                "detected_objects": detected_str,
                "image_category":  image_category,
                "object_count":    len(detections),
            })

        except Exception as e:
            log.warning(f"  Failed to process {img_path}: {e}")
            results_rows.append({
                "message_id":      message_id,
                "channel_name":    channel_name,
                "image_path":      str(img_path),
                "detected_objects": "error",
                "image_category":  "other",
                "object_count":    0,
            })

    # save results to CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "message_id", "channel_name", "image_path",
            "detected_objects", "image_category", "object_count"
        ])
        writer.writeheader()
        writer.writerows(results_rows)

    # print summary
    categories = {}
    for row in results_rows:
        cat = row["image_category"]
        categories[cat] = categories.get(cat, 0) + 1

    log.info(f"\nDone — processed {len(results_rows)} images")
    log.info(f"Results saved to {OUTPUT_CSV}")
    log.info("Category breakdown:")
    for cat, count in sorted(categories.items()):
        log.info(f"  {cat:20s} {count:>4} images")


if __name__ == "__main__":
    run_detection()