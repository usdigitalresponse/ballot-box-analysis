import re
from typing import Literal

from pydantic import BaseModel

TRAVEL_TYPES = Literal["driving", "public_transport", "walking"]
WEEK_DAYS = Literal["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


class Location(BaseModel):
    """
    A class to represent a geographical location with a name or ID, latitude, and
    longitude.

    Attributes:
        name_or_id (str): The name or ID of the location. Non-alphanumeric characters are removed.
        lat (float): The latitude of the location.
        lng (float): The longitude of the location.

    """

    name_or_id: str
    lat: float
    lng: float

    @classmethod
    def sanitize_name_or_id(cls, value: str) -> str:
        """
        Sanitize a given name or ID by removing any characters that are not alphanumeric
        or hyphens.

        Args:
            value (str): The name or ID to be sanitized.

        Returns:
            str: The sanitized name or ID containing only alphanumeric characters
                and hyphens.

        """
        return re.sub(r"[^a-zA-Z0-9-]", "", value)

    def __init__(self, **data):
        if "name_or_id" in data:
            data["name_or_id"] = self.sanitize_name_or_id(data["name_or_id"])
        super().__init__(**data)
