from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field



class AmazonOrderRequest(BaseModel):
    refresh_token: str
    client_secret:str
    region:str
    fill_type:str
    CreatedAfter:Optional[datetime] = None
    LastUpdatedAfter:Optional[datetime] = None
    MaxResultsPerPage:Optional[int] = 100
