from abc import ABC, abstractmethod
from typing import Any

from src.core.http_client import HttpClient


class Connector(ABC):
    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http_client = http_client or HttpClient()

    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        raise NotImplementedError
