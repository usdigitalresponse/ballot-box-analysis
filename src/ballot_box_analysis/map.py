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
    dataId: list[str]
    id: str
    name: list[str]
    type: Literal["multiSelect"] = "multiSelect"  # TODO: Add other recognized types
    value: list[str]


class KeplerPointColumns(BaseModel):
    lat: str
    lng: str
    altitude: str | None = None


class KeplerGeojsonColumns(BaseModel):
    geojson: str = "geometry"


class KeplerVisConfig(BaseModel):
    radius: int | None = None
    fixedRadius: bool = False
    opacity: float = Field(default=1.0, ge=0, le=1)
    strokeOpacity: float = Field(default=0.0, ge=0, le=1)
    thickness: float = 0.0
    strokeColor: list[int, int, int] | None = None
    stroked: bool = (
        False  # TODO: Figure out whether this is synonymous with `outline`, and if not, what distinguishes them
    )
    filled: bool = True


class KeplerLayerConfig(BaseModel):
    dataId: str
    label: str
    color: list[int, int, int]
    highlightColor: list[int, int, int, int] = [252, 242, 26, 255]
    columns: KeplerPointColumns | KeplerGeojsonColumns
    isVisible: bool
    visConfig: KeplerVisConfig


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
    border: bool = True
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


class MapFilter(BaseModel):
    col_name: str
    default_value: list[str]


class VoterAddressLayer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    voter_addresses: gpd.GeoDataFrame
    tooltip_cols: list[str]
    geojson_col: str = "geometry"
    color: list[int, int, int] = [207, 216, 244]
    is_visible: bool = True
    vis_config: KeplerVisConfig = KeplerVisConfig(radius=2, opacity=0.2)


class TravelTimeRadiusLayer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ballot_box_isochrones: gpd.GeoDataFrame
    tooltip_cols: list[str]
    geojson_col: str = "geometry"
    color: list[int, int, int] = [227, 151, 10]
    is_visible: bool = True
    vis_config: KeplerVisConfig = KeplerVisConfig(
        opacity=0.5,
        strokeOpacity=0.8,
        thickness=0.5,
        strokeColor=[50, 33, 19],
    )
    filters: list[MapFilter] | None = None


class BallotBoxLayer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ballot_boxes: gpd.GeoDataFrame
    tooltip_cols: list[str]
    geojson_col: str = "geometry"
    color: list[int, int, int] = [255, 255, 255]
    is_visible: bool = True
    vis_config: KeplerVisConfig = KeplerVisConfig(
        radius=18,
        opacity=0.8,
        strokeOpacity=0.8,
        thickness=0.8,
        strokeColor=[0, 0, 0],
        stroked=True,
    )


class KeplerMapLayerTitles(StrEnum):
    COUNTY_BOUNDARY = "County Boundary"
    VOTER_ADDRESS = "Voter Address"
    TRAVEL_TIME_RADIUS = "Travel Time Radius"
    BALLOT_BOX = "Ballot Box"


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
                            visConfig=KeplerVisConfig(
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

    def show(self) -> KeplerGl:
        return self.map

    def export(self, file_name: PosixPath | str) -> None:
        self.map.save_to_html(file_name=file_name)


class IsochroneMap(KeplerMap):
    def __init__(
        self,
        county: str,
        voter_address_layer: VoterAddressLayer,
        travel_time_radius_layer: TravelTimeRadiusLayer,
        ballot_box_layer: BallotBoxLayer,
    ) -> None:
        super().__init__(county)
        self._add_voter_address_layer(voter_address_layer)
        self._add_travel_time_radius_layer(travel_time_radius_layer)
        self._add_ballot_box_layer(ballot_box_layer)

    def _add_voter_address_layer(self, voter_address_layer: VoterAddressLayer) -> None:
        self.config.visState.layers.insert(
            0,
            KeplerLayer(
                id=KeplerMapLayerTitles.VOTER_ADDRESS,
                type="geojson",
                config=KeplerLayerConfig(
                    dataId=KeplerMapLayerTitles.VOTER_ADDRESS,
                    label=KeplerMapLayerTitles.VOTER_ADDRESS,
                    color=voter_address_layer.color,
                    columns=KeplerGeojsonColumns(),
                    isVisible=voter_address_layer.is_visible,
                    visConfig=voter_address_layer.vis_config,
                ),
            ),
        )
        self.config.visState.interactionConfig.tooltip.fieldsToShow[KeplerMapLayerTitles.VOTER_ADDRESS] = [
            KeplerField(name=col) for col in voter_address_layer.tooltip_cols
        ]

        self._update_map_config()
        self.map.add_data(data=voter_address_layer.voter_addresses.fillna(""), name=KeplerMapLayerTitles.VOTER_ADDRESS)

    def _add_travel_time_radius_layer(self, travel_time_radius_layer: TravelTimeRadiusLayer) -> None:
        self.config.visState.layers.insert(
            0,
            KeplerLayer(
                id=KeplerMapLayerTitles.TRAVEL_TIME_RADIUS,
                type="geojson",
                config=KeplerLayerConfig(
                    dataId=KeplerMapLayerTitles.TRAVEL_TIME_RADIUS,
                    label=KeplerMapLayerTitles.TRAVEL_TIME_RADIUS,
                    color=travel_time_radius_layer.color,
                    columns=KeplerGeojsonColumns(),
                    isVisible=travel_time_radius_layer.is_visible,
                    visConfig=travel_time_radius_layer.vis_config,
                ),
            ),
        )
        self.config.visState.interactionConfig.tooltip.fieldsToShow[KeplerMapLayerTitles.TRAVEL_TIME_RADIUS] = [
            KeplerField(name=col) for col in travel_time_radius_layer.tooltip_cols
        ]

        if travel_time_radius_layer.filters:
            for _filter in travel_time_radius_layer.filters:
                self.config.visState.filters.append(
                    KeplerFilter(
                        dataId=[KeplerMapLayerTitles.TRAVEL_TIME_RADIUS],
                        id=KeplerMapLayerTitles.TRAVEL_TIME_RADIUS,
                        name=[_filter.col_name],
                        value=_filter.default_value,
                    )
                )

        self._update_map_config()
        self.map.add_data(
            data=travel_time_radius_layer.ballot_box_isochrones.fillna(""), name=KeplerMapLayerTitles.TRAVEL_TIME_RADIUS
        )

    def _add_ballot_box_layer(self, ballot_box_layer: BallotBoxLayer) -> None:
        self.config.visState.layers.insert(
            0,
            KeplerLayer(
                id=KeplerMapLayerTitles.BALLOT_BOX,
                type="geojson",
                config=KeplerLayerConfig(
                    dataId=KeplerMapLayerTitles.BALLOT_BOX,
                    label=KeplerMapLayerTitles.BALLOT_BOX,
                    color=ballot_box_layer.color,
                    columns=KeplerGeojsonColumns(),
                    isVisible=ballot_box_layer.is_visible,
                    visConfig=ballot_box_layer.vis_config,
                ),
            ),
        )
        self.config.visState.interactionConfig.tooltip.fieldsToShow[KeplerMapLayerTitles.BALLOT_BOX] = [
            KeplerField(name=col) for col in ballot_box_layer.tooltip_cols
        ]

        self._update_map_config()
        self.map.add_data(data=ballot_box_layer.ballot_boxes.fillna(""), name=KeplerMapLayerTitles.BALLOT_BOX)
