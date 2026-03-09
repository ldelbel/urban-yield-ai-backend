from abc import ABC, abstractmethod
from typing import Any


class BaseIngestor(ABC):
    @abstractmethod
    async def fetch(self) -> list[Any]:
        """Fetch and return parsed records from the data source."""
        ...
