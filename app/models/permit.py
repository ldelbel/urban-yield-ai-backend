from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PermitRecord(BaseModel):
    permit_num: str
    status: Optional[str] = None
    address: Optional[str] = None
    issue_date: Optional[datetime] = None
    added_date: Optional[datetime] = None
    work_type: Optional[str] = None
    description: Optional[str] = None          # ArcGIS field: 'descriptio'
    building_area_sqft: Optional[float] = None  # ArcGIS field: 'bldgareanu'
    declared_value: Optional[float] = None      # ArcGIS field: 'declvalnu'
    latitude: float
    longitude: float
    h3_index: Optional[str] = None              # computed post-ingestion
