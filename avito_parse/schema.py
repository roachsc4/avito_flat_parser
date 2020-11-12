from dataclasses import dataclass
from datetime import datetime


@dataclass
class Advertisement:
    id: int
    url: str
    price: int
    title: str
    address: str
    approximate_date_string: str

    date: datetime = None
    description: str = None

    @property
    def full_url(self):
        return f'https://avito.ru{self.url}'

    @property
    def formatted_price(self) -> str:
        return f'{self.price:_}'.replace('_', ' ')


@dataclass
class User:
    id: int
    username: str
    first_name: str
    last_name: str
    chat_id: int
