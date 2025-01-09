import contextlib
import os
import re
from pathlib import PosixPath
from typing import Literal

import geopandas as gpd
import pygris
import shapely
from aenum import StrEnum
from keplergl import KeplerGl
from pydantic import BaseModel, ConfigDict, Field


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
    geojson: str = "geometry"


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
    color: list[int, int, int]
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


class VoterAddressLayer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    voter_addresses: gpd.GeoDataFrame
    tooltip_cols: list[str]
    geojson_col: str = "geometry"
    color: list[int, int, int] = [207, 216, 244]
    vis_config: KeplerPointVisConfig = KeplerPointVisConfig(
        radius=2,
        opacity=0.2,
        outline=False,
        thickness=2,
        strokeColor=None,
    )


class KeplerMapLayerTitles(StrEnum):
    COUNTY_BOUNDARY = "County Boundary"
    VOTER_ADDRESS = "Voter Address"


class InvalidCountyError(Exception):
    def __init__(self):
        self.message = "Invalid county format provided. Please provide the county and state separated by a comma (e.g., 'Monmouth, NJ')."
        super().__init__(self.message)


class KeplerMap:
    def __init__(self, county: str) -> None:
        county_boundary_gdf = self._get_county_boundary(county)
        county_boundary: shapely.Polygon | shapely.MultiPolygon = county_boundary_gdf["geometry"].iloc[0]
        county_centroid = county_boundary.centroid

        self.config = self._init_config(county_centroid)

        self.map = KeplerGl()
        self._update_map_config()
        self.map.add_data(data=county_boundary_gdf, name=KeplerMapLayerTitles.COUNTY_BOUNDARY)

    @staticmethod
    def _init_config(county_centroid: shapely.Point) -> KeplerConfig:
        return KeplerConfig(
            visState=KeplerVisState(
                filters=[],
                layers=[
                    KeplerLayer(
                        id=KeplerMapLayerTitles.COUNTY_BOUNDARY,
                        type="geojson",
                        config=KeplerLayerConfig(
                            dataId=KeplerMapLayerTitles.COUNTY_BOUNDARY,
                            label=KeplerMapLayerTitles.COUNTY_BOUNDARY,
                            color=[255, 255, 255],
                            columns=KeplerGeojsonColumns(),
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
                            KeplerMapLayerTitles.COUNTY_BOUNDARY: [
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

    def _update_map_config(self) -> None:
        self.map.config = {"version": "v1", "config": self.config.model_dump()}

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

    def add_voter_address_layer(self, voter_address_layer: VoterAddressLayer) -> None:
        self.config.visState.layers.append(
            KeplerLayer(
                id=KeplerMapLayerTitles.VOTER_ADDRESS,
                type="geojson",
                config=KeplerLayerConfig(
                    dataId=KeplerMapLayerTitles.VOTER_ADDRESS,
                    label=KeplerMapLayerTitles.VOTER_ADDRESS,
                    color=voter_address_layer.color,
                    columns=KeplerGeojsonColumns(),
                    isVisible=True,
                    visConfig=voter_address_layer.vis_config,
                ),
            )
        )
        self.config.visState.interactionConfig.tooltip.fieldsToShow[KeplerMapLayerTitles.VOTER_ADDRESS] = [
            KeplerField(name=col) for col in voter_address_layer.tooltip_cols
        ]

        self._update_map_config()
        self.map.add_data(data=voter_address_layer.voter_addresses, name=KeplerMapLayerTitles.VOTER_ADDRESS)

    def show(self) -> KeplerGl:
        return self.map

    def export(self, file_name: PosixPath | str) -> None:
        self.map.save_to_html(file_name=file_name)
