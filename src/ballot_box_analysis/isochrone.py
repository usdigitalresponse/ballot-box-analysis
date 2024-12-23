import datetime
import json
import os
import time
from pathlib import Path, PosixPath

import geopandas as gpd
import pandas as pd
import requests
from dateutil import parser, tz
from loguru import logger
from shapely.geometry import shape

from ballot_box_analysis.io import TRAVEL_TYPES, WEEK_DAYS, Location


class IsochroneGenerator:
    _ISOCHRONES_API = "https://api.traveltimeapp.com/v4/time-map"

    def __init__(
        self,
        locations: gpd.GeoDataFrame,
        name_or_id_col: str = "name",
        travel_type: TRAVEL_TYPES = "driving",
        arrival_weekday: WEEK_DAYS = "Tuesday",
        arrival_time: str = "18:00",
        timezone: str = "America/Los_Angeles",
        cache_dir: PosixPath = Path(".traveltime_cache"),
    ):
        self.locations = locations
        self.name_or_id_col = name_or_id_col
        self.travel_type = travel_type
        self.arrival_time_iso = self._calc_arrival_time(
            arrival_weekday=arrival_weekday, arrival_time=arrival_time, timezone=timezone
        )
        self.cache_dir = cache_dir

    @classmethod
    def from_pandas(
        cls, locations: pd.DataFrame, lat_col: str = "lat", lng_col: str = "lng", name_or_id_col: str = "name"
    ):
        if lat_col not in locations.columns:
            raise ValueError(  # noqa: TRY003
                f"Latitude column name '{lat_col}' does not exist in the provided data frame. Please enter a different value."
            )

        if lng_col not in locations.columns:
            raise ValueError(  # noqa: TRY003
                f"Longitude column name '{lng_col}' does not exist in the provided data frame. Please enter a different value."
            )

        locations_gpd = gpd.GeoDataFrame(
            locations,
            geometry=gpd.points_from_xy(locations[lng_col], locations[lat_col]),
            crs="EPSG:4326",
        )
        return cls.from_geopandas(locations=locations_gpd, name_or_id_col=name_or_id_col)

    @classmethod
    def from_geopandas(cls, locations: gpd.GeoDataFrame, name_or_id_col: str = "name"):
        return cls(locations=locations, name_or_id_col=name_or_id_col)

    @staticmethod
    def _calc_arrival_time(
        arrival_weekday: WEEK_DAYS,
        arrival_time: str,
        timezone: str,
    ) -> str:
        tz_info = tz.gettz(timezone)
        today = datetime.date.today()
        weekday_int = time.strptime(arrival_weekday, "%A").tm_wday
        next_weekday = today + datetime.timedelta((1 - today.weekday() + weekday_int - 1) % 7)
        arrival_time = datetime.datetime(
            year=next_weekday.year,
            month=next_weekday.month,
            day=next_weekday.day,
            hour=int(arrival_time[0:2]),
            minute=int(arrival_time[3:5]),
            tzinfo=tz_info,
        ).isoformat()

        return arrival_time

    def set_travel_type(self, travel_type: TRAVEL_TYPES):
        self.travel_type = travel_type

    def set_arrival_time(
        self,
        arrival_weekday: WEEK_DAYS,
        arrival_time: str,
        timezone: str,
    ):
        self.arrival_time_iso = self._calc_arrival_time(
            arrival_weekday=arrival_weekday, arrival_time=arrival_time, timezone=timezone
        )

    def set_cache_dir(self, cache_dir: PosixPath | None):
        self.cache_dir = cache_dir

    @staticmethod
    def _get_api_keys() -> tuple[str, str]:
        traveltime_id = os.environ.get("TRAVELTIME_ID")
        traveltime_key = os.environ.get("TRAVELTIME_KEY")
        if not traveltime_id or not traveltime_key:
            raise ValueError("Please set the environment variables TRAVELTIME_ID and TRAVELTIME_KEY.")  # noqa: TRY003
        return traveltime_id, traveltime_key

    def _parse_arrival_time(self) -> datetime.datetime:
        parsed_arrival_time = parser.parse(self.arrival_time_iso)
        return parsed_arrival_time

    def _construct_filename(self, location: Location, travel_minutes: int) -> str:
        parsed_arrival_time = self._parse_arrival_time()
        arrival_weekday = parsed_arrival_time.strftime("%A")
        arrival_hhmm = parsed_arrival_time.strftime("%H%M")

        stem = "_-_".join([location.name_or_id, self.travel_type, str(travel_minutes), arrival_weekday, arrival_hhmm])
        return f"{stem}.json"

    @staticmethod
    def _isochrone_response_to_shape(shapes: list[dict]) -> shape:
        coordinates = []
        for _shape in shapes:
            shell = [[c["lng"], c["lat"]] for c in _shape["shell"]]
            holes = [[[c["lng"], c["lat"]] for c in h] for h in _shape["holes"]]
            rings = [shell]
            rings.extend(holes)
            coordinates.append(rings)

        multipolygon = {"type": "MultiPolygon", "coordinates": coordinates}
        return shape(multipolygon)

    def _generate_isochrone(self, travel_minutes: int, max_retries: int, location: Location, headers: dict) -> dict:
        time.sleep(5)

        for i in range(max_retries):
            try:
                r = requests.post(
                    self._ISOCHRONES_API,
                    headers=headers,
                    json={
                        "arrival_searches": [
                            {
                                "id": location.name_or_id,
                                "coords": {"lat": location.lat, "lng": location.lng},
                                "arrival_time": self.arrival_time_iso,
                                "travel_time": travel_minutes * 60,
                                "transportation": {
                                    "type": self.travel_type,
                                },
                                "level_of_detail": {"scale_type": "simple", "level": "medium"},
                            }
                        ]
                    },
                    timeout=60,
                )
                r.raise_for_status()
                break

            except requests.exceptions.RequestException as e:
                error_str = f"Error: {e}"

                if r.status_code == 429 and i < max_retries:
                    retry_seconds = 10 * (i + 2)
                    logger.warning(f"[{location.name_or_id}] Retrying in {retry_seconds} seconds. {error_str}")
                    time.sleep(retry_seconds)

                else:
                    logger.error(f"[{location.name_or_id}] {error_str}")
                    raise

        isochrone = r.json()
        return isochrone

    def generate_isochrones(self, travel_minutes: int, max_retries: int = 4) -> gpd.GeoDataFrame:
        traveltime_id, traveltime_key = self._get_api_keys()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Application-Id": traveltime_id,
            "X-Api-Key": traveltime_key,
        }

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        parsed_arrival_time = self._parse_arrival_time()
        arrival_weekday = parsed_arrival_time.strftime("%A")
        arrival_time_of_day = parsed_arrival_time.strftime("%-I:%M %p")

        isochrones: gpd.GeoDataFrame = self.locations.copy()
        isochrones["TravelType"] = self.travel_type
        isochrones["TravelMinutes"] = travel_minutes
        isochrones["ArrivalTime"] = f"{arrival_weekday} at {arrival_time_of_day}"
        isochrones["isochrone"] = None

        for idx, row in self.locations.iterrows():
            location = Location(name_or_id=row[self.name_or_id_col], lat=row.geometry.y, lng=row.geometry.x)
            isochrone = None

            if self.cache_dir:
                filename = self.cache_dir / self._construct_filename(location, travel_minutes)

                if filename.exists():
                    logger.info(f"[{location.name_or_id}] Loading from cache: {filename}")
                    with open(filename) as f:
                        isochrone = json.load(f)

            if isochrone is None:
                logger.info(f"[{location.name_or_id}] Generating isochrone...")
                isochrone = self._generate_isochrone(travel_minutes, max_retries, location, headers)
                if self.cache_dir:
                    with open(filename, "w") as f:
                        json.dump(isochrone, f, indent=4)

            results: list[dict] = isochrone.get("results")
            if results:
                shapes = results[0].get("shapes")
                isochrones.at[idx, "isochrone"] = self._isochrone_response_to_shape(shapes)

        isochrones.set_geometry("isochrone", inplace=True)
        isochrones.drop(columns=["geometry"], inplace=True)
        isochrones.rename_geometry("geometry", inplace=True)

        return isochrones
