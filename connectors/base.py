from abc import ABC, abstractmethod
from typing import Dict

class SchemaCrawler(ABC):
    """Abstract base class for schema crawlers that every interface must implement."""
    @abstractmethod
    def discover(self, connection_info: Dict) -> Dict:
        """
        Return canonical schema:
          {
            "entity": "customer",
            "attributes": [
              {"name":"id", "type":"string", "nullable": False},
              ...
            ],
            "source": {...}
          }
        """
        raise NotImplementedError("Subclasses must implement this method.")