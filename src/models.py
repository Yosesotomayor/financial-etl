from pydantic import BaseModel, Field

class StockQuote(BaseModel):
    current_price: float = Field(alias="c")
    high_price: float = Field(alias="h")
    low_price: float = Field(alias="l")
    timestamp: int = Field(alias="t")