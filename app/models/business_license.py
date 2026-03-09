from typing import Optional

from pydantic import BaseModel


class BusinessLicenseRecord(BaseModel):
    company_name: Optional[str] = None
    industry: Optional[str] = None
    license_year: Optional[int] = None
    address: Optional[str] = None
    latitude: float
    longitude: float
    h3_index: Optional[str] = None
