from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class WooOrderRequest(BaseModel):
    base_url: str
    consumer_key: str
    consumer_secret: str
    connected_id: str
    fill_type: str
    CreatedAfter: Optional[datetime] = None
    LastUpdatedAfter: Optional[datetime] = None
    PerPage: Optional[int] = 100