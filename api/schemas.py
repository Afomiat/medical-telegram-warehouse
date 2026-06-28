from pydantic import BaseModel
from typing import Optional

class TopProduct(BaseModel):
    term: str
    mention_count: int

class ChannelActivity(BaseModel):
    channel_name: str
    message_date: str
    message_count: int
    total_views: int
    total_forwards: int

class MessageResult(BaseModel):
    message_id: int
    channel_name: str
    message_date: str
    message_text: str
    views: int

class VisualContentStat(BaseModel):
    channel_name: str
    image_category: str
    image_count: int
    avg_views: Optional[float]

class ChannelSummary(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float
    total_images: int