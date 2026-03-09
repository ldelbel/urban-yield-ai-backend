from typing import Optional

from pydantic import BaseModel


class VacancyRecord(BaseModel):
    address: Optional[str] = None
    district: Optional[str] = None
    latitude: float
    longitude: float
    h3_index: Optional[str] = None
