import contextlib
import os
import re
from typing import Literal

import geopandas as gpd
import pygris
from keplergl import KeplerGl
from pydantic import BaseModel, Field


class KeplerFilter(BaseModel):
    dataId: str
    id: list[str]
    name: list[str]
    type: Literal["multiSelect"]  # TODO: Add other recognized types
    value: list[str]


class KeplerPointColumns(BaseModel):
    lat: str
    lng: str
    altitude: str | None = None


class KeplerGeojsonColumns(BaseModel):
    geojson: str


class KeplerPointVisConfig(BaseModel):
    radius: int
    fixedRadius: bool = False
    opacity: float = Field(ge=0, le=1)
    outline: bool
    thickness: float
    strokeColor: list[int, int, int] | None
    filled: bool = True


class KeplerGeojsonVisConfig(BaseModel):
    opacity: float = Field(ge=0, le=1)
    strokeOpacity: float = Field(ge=0, le=1)
    thickness: float
    strokeColor: list[int, int, int]
    stroked: bool
    filled: bool = True


class KeplerLayerConfig(BaseModel):
    dataId: str
    label: str
    color: list[int, int, int, int]
    highlightColor: list[int, int, int, int] = [252, 242, 26, 255]
    columns: KeplerPointColumns | KeplerGeojsonColumns
    isVisible: bool
    visConfig: KeplerPointVisConfig | KeplerGeojsonVisConfig


class KeplerLayer(BaseModel):
    id: str
    type: Literal["point", "geojson"]
    config: KeplerLayerConfig


class KeplerField(BaseModel):
    name: str
    format: None = None  # TODO: Add Literal with recognized formats


class KeplerTooltip(BaseModel):
    fieldsToShow: dict[str, list[KeplerField]]
    enabled: bool = True


class KeplerInteractionConfig(BaseModel):
    tooltip: KeplerTooltip


class KeplerVisState(BaseModel):
    filters: list[KeplerFilter]
    layers: list[KeplerLayer]
    interactionConfig: KeplerInteractionConfig


class KeplerMapState(BaseModel):
    latitude: float
    longitude: float
    zoom: int = 9


class KeplerVisibleLayerGroups(BaseModel):
    label: bool = True
    road: bool = True
    border: bool = False
    building: bool = True
    water: bool = True
    land: bool = True


class KeplerMapStyle(BaseModel):
    styleType: Literal["dark", "light", "satellite"] = "dark"
    visibleLayerGroups: KeplerVisibleLayerGroups = KeplerVisibleLayerGroups()


class KeplerConfig(BaseModel):
    visState: KeplerVisState
    mapState: KeplerMapState
    mapStyle: KeplerMapStyle


class InvalidCountyError(Exception):
    def __init__(self):
        self.message = "Invalid county format provided. Please provide the county and state separated by a comma (e.g., 'Monmouth, NJ')."
        super().__init__(self.message)


class KeplerMap:
    def __init__(self, county: str) -> None:
        county_boundary = self._get_county_boundary(county)
        county_centroid = county_boundary["geometry"].iloc[0].centroid

        self.config = KeplerConfig(
            visState=KeplerVisState(
                filters=[],
                layers=[
                    KeplerLayer(
                        id="County Boundary",
                        type="geojson",
                        config=KeplerLayerConfig(
                            dataId="County Boundary",
                            label="County Boundary",
                            color=[255, 255, 255],
                            columns=KeplerGeojsonColumns(geojson="geometry"),
                            isVisible=True,
                            visConfig=KeplerGeojsonVisConfig(
                                opacity=0.01,
                                strokeOpacity=0.15,
                                thickness=0.5,
                                strokeColor=[255, 255, 255],
                                stroked=True,
                                filled=False,
                            ),
                        ),
                    )
                ],
                interactionConfig=KeplerInteractionConfig(
                    tooltip=KeplerTooltip(
                        fieldsToShow={
                            "County Boundary": [
                                KeplerField(name="GEOID"),
                                KeplerField(name="NAME"),
                            ]
                        }
                    )
                ),
            ),
            mapState=KeplerMapState(
                latitude=float(county_centroid.y),
                longitude=float(county_centroid.x),
                zoom=9,
            ),
            mapStyle=KeplerMapStyle(styleType="dark"),
        )

        self.map = KeplerGl()
        self.map.config = {"version": "v1", "config": self.config.model_dump()}
        self.map.add_data(data=county_boundary, name="County Boundary")

    @staticmethod
    def _get_county_boundary(county: str) -> gpd.GeoDataFrame:
        try:
            county, state = re.split(r",\s*", county)
        except ValueError:
            raise InvalidCountyError() from None

        with contextlib.redirect_stdout(open(os.devnull, "w")):
            fips_county = pygris.validate_county(county=county, state=state)
            county_boundaries = pygris.counties(state=state, cb=True, cache=True)
            county_boundary: gpd.GeoDataFrame = county_boundaries[county_boundaries["COUNTYFP"] == fips_county]

        return county_boundary.to_crs(epsg=4326)

    def show(self) -> KeplerGl:
        return self.map
