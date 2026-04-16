from typing import List, Dict, Any, Optional
from lib.api_connect import SimpleAPIClient
import logging

log = logging.getLogger(__name__)


class ExternalAPIReader:
    """Чтение данных из внешнего API через SimpleAPIClient"""

    def __init__(self, api_client: SimpleAPIClient):
        self.client = api_client

    def fetch_all_restaurants(self, limit: int = 50) -> List[Dict]:
        """Получает список всех ресторанов."""
        return self.client.fetch_all_restaurants()

    def fetch_all_couriers(self, limit: int = 50) -> List[Dict]:
        """Получает список всех курьеров."""
        return self.client.fetch_all_couriers()

    def fetch_all_deliveries(
        self,
        restaurant_id: str,
        days_back: int = 7,
        limit: int = 50
    ) -> List[Dict]:
        """Получает список всех доставок для ресторана за период."""
        return self.client.fetch_all_deliveries(
            restaurant_id=restaurant_id,
            days_back=days_back
        )
