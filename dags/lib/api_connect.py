import requests
from datetime import datetime, timedelta
from typing import List, Dict


class SimpleAPIClient:

    NICKNAME = "Baidakova"
    COHORT = "12"
    API_KEY = "25c27781-8fde-4b30-a22e-524044a7580f"

    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
    PAGE_LIMIT = 50  # максимум записей за запрос

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        """Инициализация клиента с заголовками аутентификации"""
        self.headers = {
            "X-Nickname": self.NICKNAME,
            "X-Cohort": self.COHORT,
            "X-API-KEY": self.API_KEY,
            "Content-Type": "application/json"
        }

    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Выполняет GET-запрос к указанному эндпоинту."""
        url = f"{self.BASE_URL}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_restaurants(self, offset: int = 0) -> List[Dict]:
        """Получает список ресторанов с пагинацией."""
        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": self.PAGE_LIMIT,
            "offset": offset
        }
        return self._make_request("/restaurants", params)

    def get_couriers(self, offset: int = 0) -> List[Dict]:
        """Получает список курьеров с пагинацией."""
        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": self.PAGE_LIMIT,
            "offset": offset
        }
        return self._make_request("/couriers", params)

    def get_deliveries(
        self,
        restaurant_id: str,
        from_date: str,
        to_date: str,
        offset: int = 0
    ) -> List[Dict]:
        """Получает список доставок для ресторана за период с пагинацией."""
        params = {
            "restaurant_id": restaurant_id,
            "from": from_date,
            "to": to_date,
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": self.PAGE_LIMIT,
            "offset": offset
        }
        response = self._make_request("/deliveries", params)
        # API может возвращать список напрямую или в ключе "data"
        if isinstance(response, list):
            return response
        return response.get("data", response.get("result", []))

    def fetch_all_restaurants(self) -> List[Dict]:
        """Загружает все рестораны с постраничным чтением. Возвращает список."""
        all_items = []
        offset = 0

        while True:
            items = self.get_restaurants(offset=offset)
            if not items:
                break
            all_items.extend(items)
            offset += self.PAGE_LIMIT
            print(f"Loaded {len(all_items)} restaurants...")

        return all_items

    def fetch_all_couriers(self) -> List[Dict]:
        """Загружает все курьеры с постраничным чтением. Возвращает список."""
        all_items = []
        offset = 0

        while True:
            items = self.get_couriers(offset=offset)
            if not items:
                break
            all_items.extend(items)
            offset += self.PAGE_LIMIT
            print(f"Loaded {len(all_items)} couriers...")

        return all_items

    def fetch_all_deliveries(
        self,
        restaurant_id: str,
        days_back: int = 7
    ) -> List[Dict]:
        """Загружает все доставки для ресторана за последние 7 дней."""
        # Формируем период в нужном формате
        to_dt = datetime.now()
        from_dt = to_dt - timedelta(days=days_back)

        # Конвертируем в строки формата "%Y-%m-%d %H:%M:%S"
        to_date = to_dt.strftime(self.DATE_FORMAT)
        from_date = from_dt.strftime(self.DATE_FORMAT)

        all_items = []
        offset = 0

        while True:
            items = self.get_deliveries(
                restaurant_id=restaurant_id,
                from_date=from_date,
                to_date=to_date,
                offset=offset
            )
            if not items:
                break
            all_items.extend(items)
            offset += self.PAGE_LIMIT
            print(f"Loaded {len(all_items)} deliveries for restaurant {restaurant_id}...")

        return all_items
