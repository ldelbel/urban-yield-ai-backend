from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ServiceRequestRecord(BaseModel):
    case_type: Optional[str] = None
    status: Optional[str] = None
    address: Optional[str] = None
    case_date: Optional[datetime] = None
    latitude: float
    longitude: float
    h3_index: Optional[str] = None
