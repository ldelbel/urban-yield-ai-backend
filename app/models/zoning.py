from typing import Optional

from pydantic import BaseModel


class ZoningRecord(BaseModel):
    zoning_code: str
    zoning_desc: Optional[str] = None
    # polygon rings as list of [lon, lat] coordinate pairs (GeoJSON order)
    rings: list[list[list[float]]]
