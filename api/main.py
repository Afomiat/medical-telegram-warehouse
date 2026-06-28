from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from api.database import get_db
from api import schemas

app = FastAPI(
    title="Ethiopian Medical Data API",
    description="Analytical API exposing insights from Ethiopian medical Telegram channels",
    version="1.0.0"
)


@app.get("/")
def root():
    return {
        "message": "Ethiopian Medical Data API",
        "docs": "/docs",
        "endpoints": [
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/reports/visual-content",
            "/api/channels"
        ]
    }


@app.get("/api/channels", response_model=List[schemas.ChannelSummary])
def get_channels(db: Session = Depends(get_db)):
    """Returns a summary of all channels in the warehouse."""
    rows = db.execute(text("""
        SELECT
            channel_name,
            channel_type,
            total_posts,
            COALESCE(avg_views, 0)   AS avg_views,
            COALESCE(total_images, 0) AS total_images
        FROM marts.dim_channels
        ORDER BY total_posts DESC
    """)).fetchall()

    return [schemas.ChannelSummary(
        channel_name=r.channel_name,
        channel_type=r.channel_type,
        total_posts=r.total_posts,
        avg_views=float(r.avg_views),
        total_images=r.total_images
    ) for r in rows]


@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def top_products(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Returns the most frequently mentioned words across all messages."""
    rows = db.execute(text("""
        SELECT
            word AS term,
            COUNT(*) AS mention_count
        FROM (
            SELECT regexp_split_to_table(
                lower(message_text), E'\\\\s+'
            ) AS word
            FROM marts.fct_messages
            WHERE message_length > 0
        ) words
        WHERE
            length(word) > 3
            AND word NOT IN (
                'this','that','with','from','have','will',
                'your','been','they','were','also','what',
                'when','than','then','into','more','over',
                'after','before','about','which','there',
                'their','these','those','would','could',
                'should','other','some','such','only',
                'very','just','like','each','much','both',
                'here','make','well','take','come','time',
                'year','know','need','want','even','back',
                'good','great','best','high','free','sale',
                'price','order','contact','call','send',
                'birr','100','200','500','1000','2000'
            )
        GROUP BY word
        ORDER BY mention_count DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return [schemas.TopProduct(
        term=r.term,
        mention_count=r.mention_count
    ) for r in rows]


@app.get("/api/channels/{channel_name}/activity",
         response_model=List[schemas.ChannelActivity])
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """Returns daily posting activity and engagement for a specific channel."""
    rows = db.execute(text("""
        SELECT
            f.channel_name,
            d.full_date::text           AS message_date,
            COUNT(*)                    AS message_count,
            COALESCE(SUM(f.views), 0)   AS total_views,
            COALESCE(SUM(f.forwards), 0) AS total_forwards
        FROM marts.fct_messages f
        JOIN marts.dim_dates d ON f.date_key = d.date_key
        WHERE lower(f.channel_name) = lower(:channel_name)
        GROUP BY f.channel_name, d.full_date
        ORDER BY d.full_date DESC
    """), {"channel_name": channel_name}).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Channel '{channel_name}' not found"
        )

    return [schemas.ChannelActivity(
        channel_name=r.channel_name,
        message_date=r.message_date,
        message_count=r.message_count,
        total_views=r.total_views,
        total_forwards=r.total_forwards
    ) for r in rows]


@app.get("/api/search/messages", response_model=List[schemas.MessageResult])
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Search messages containing a specific keyword."""
    rows = db.execute(text("""
        SELECT
            message_id,
            channel_name,
            message_date::text  AS message_date,
            message_text,
            views
        FROM marts.fct_messages
        WHERE lower(message_text) LIKE lower(:query)
        ORDER BY views DESC
        LIMIT :limit
    """), {"query": f"%{query}%", "limit": limit}).fetchall()

    return [schemas.MessageResult(
        message_id=r.message_id,
        channel_name=r.channel_name,
        message_date=r.message_date,
        message_text=r.message_text,
        views=r.views
    ) for r in rows]


@app.get("/api/reports/visual-content",
         response_model=List[schemas.VisualContentStat])
def visual_content_stats(db: Session = Depends(get_db)):
    """Returns image category statistics per channel."""
    rows = db.execute(text("""
        SELECT
            channel_name,
            image_category,
            COUNT(*)            AS image_count,
            AVG(views)          AS avg_views
        FROM marts.fct_image_detections
        GROUP BY channel_name, image_category
        ORDER BY channel_name, image_count DESC
    """)).fetchall()

    return [schemas.VisualContentStat(
        channel_name=r.channel_name,
        image_category=r.image_category,
        image_count=r.image_count,
        avg_views=float(r.avg_views) if r.avg_views else None
    ) for r in rows]