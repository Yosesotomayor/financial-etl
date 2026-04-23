import httpx
import os
from dotenv import load_dotenv

load_dotenv()

class FinnhubClient:
      def __init__(self, token: str) -> None:
            self.client = httpx.Client(base_url="https://finnhub.io/api/v1")
            self.params = {
                  "token": token
            }

      def get_stock_price(self, symbol: str) -> float:
            response = self.client.get("/quote", params={**self.params, "symbol": symbol})
            response.raise_for_status()
            return response.json()["c"]
