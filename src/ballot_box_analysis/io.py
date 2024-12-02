import re
from typing import Literal

from pydantic import BaseModel

TRAVEL_TYPES = Literal["driving", "public_transport", "walking"]
WEEK_DAYS = Literal["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


class Location(BaseModel):
    name_or_id: str
    lat: float
    lng: float

    @classmethod
    def sanitize_name_or_id(cls, value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9-]", "", value)

    def __init__(self, **data):
        if "name_or_id" in data:
            data["name_or_id"] = self.sanitize_name_or_id(data["name_or_id"])
        super().__init__(**data)
