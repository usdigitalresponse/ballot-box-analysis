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
    """
    KeplerFilter is a model representing a filter configuration for Kepler.gl.

    Attributes:
        dataId (list[str]): A list of data identifiers that the filter applies to.
        id (str): A unique identifier for the filter.
        name (list[str]): A list of names associated with the filter.
        type (Literal["multiSelect"]): The type of the filter, which is currently limited to "multiSelect".
        value (list[str]): A list of values that the filter will use for selection.

    """

    dataId: list[str]
    id: str
    name: list[str]
    type: Literal["multiSelect"] = "multiSelect"  # TODO: Add other recognized types
    value: list[str]


class KeplerPointColumns(BaseModel):
    """
    KeplerPointColumns is a data model representing the columns for a point in
    Kepler.gl.

    Attributes:
        lat (str): The name of the column representing the latitude.
        lng (str): The name of the column representing the longitude.
        altitude (str | None, optional): The name of the column representing the altitude. Defaults to None.

    """

    lat: str
    lng: str
    altitude: str | None = None


class KeplerGeojsonColumns(BaseModel):
    """
    KeplerGeojsonColumns is a model that defines the structure for geojson columns used
    in Kepler.gl.

    Attributes:
        geojson (str): The name of the geojson column, default is "geometry".

    """

    geojson: str = "geometry"


class KeplerVisConfig(BaseModel):
    """
    KeplerVisConfig is a configuration model for visualizing data using Kepler.gl.

    Attributes:
        radius (int | None): The radius of the visual elements. Default is None.
        fixedRadius (bool): Whether the radius is fixed. Default is False.
        opacity (float): The opacity of the visual elements, ranging from 0 to 1. Default is 1.0.
        strokeOpacity (float): The opacity of the stroke, ranging from 0 to 1. Default is 0.0.
        thickness (float): The thickness of the stroke. Default is 0.0.
        strokeColor (list[int, int, int] | None): The color of the stroke as an RGB list. Default is None.
        stroked (bool): Whether the visual elements are stroked. Default is False.
        filled (bool): Whether the visual elements are filled. Default is True.

    """

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
    """
    Configuration for a Kepler.gl layer.

    Attributes:
        dataId (str): Identifier for the data source.
        label (str): Label for the layer.
        color (list[int, int, int]): RGB color for the layer.
        highlightColor (list[int, int, int, int]): RGBA color for highlighting, default is [252, 242, 26, 255].
        columns (KeplerPointColumns | KeplerGeojsonColumns): Column configuration for the layer.
        isVisible (bool): Visibility status of the layer.
        visConfig (KeplerVisConfig): Visualization configuration for the layer.

    """

    dataId: str
    label: str
    color: list[int, int, int]
    highlightColor: list[int, int, int, int] = [252, 242, 26, 255]
    columns: KeplerPointColumns | KeplerGeojsonColumns
    isVisible: bool
    visConfig: KeplerVisConfig


class KeplerLayer(BaseModel):
    """
    KeplerLayer represents a layer configuration for Kepler.gl visualization.

    Attributes:
        id (str): Unique identifier for the layer.
        type (Literal["point", "geojson"]): Type of the layer, either "point" or "geojson".
        config (KeplerLayerConfig): Configuration settings for the layer.

    """

    id: str
    type: Literal["point", "geojson"]
    config: KeplerLayerConfig


class KeplerField(BaseModel):
    """
    Represents a field in a Kepler.gl map configuration.

    Attributes:
        name (str): The name of the field.
        format (None): The format of the field.

    """

    name: str
    format: None = None  # TODO: Add Literal with recognized formats


class KeplerTooltip(BaseModel):
    """
    A model representing the configuration for tooltips in Kepler.gl.

    Attributes:
        fieldsToShow (dict[str, list[KeplerField]]): A dictionary where the keys are dataset names and the values are lists of fields to show in the tooltip.
        enabled (bool): A flag indicating whether the tooltip is enabled. Defaults to True.

    """

    fieldsToShow: dict[str, list[KeplerField]]
    enabled: bool = True


class KeplerInteractionConfig(BaseModel):
    """
    KeplerInteractionConfig is a configuration class for Kepler.gl interactions.

    Attributes:
        tooltip (KeplerTooltip): An instance of KeplerTooltip that defines the tooltip configuration for Kepler.gl interactions.

    """

    tooltip: KeplerTooltip


class KeplerVisState(BaseModel):
    """
    Represents the visualization state for Kepler.gl.

    Attributes:
        filters (list[KeplerFilter]): A list of filters applied to the visualization.
        layers (list[KeplerLayer]): A list of layers used in the visualization.
        interactionConfig (KeplerInteractionConfig): Configuration for user interactions within the visualization.

    """

    filters: list[KeplerFilter]
    layers: list[KeplerLayer]
    interactionConfig: KeplerInteractionConfig


class KeplerMapState(BaseModel):
    """
    Represents the state of a Kepler.gl map.

    Attributes:
        latitude (float): The latitude coordinate of the map's center.
        longitude (float): The longitude coordinate of the map's center.
        zoom (int): The zoom level of the map, default is 9.

    """

    latitude: float
    longitude: float
    zoom: int = 9


class KeplerVisibleLayerGroups(BaseModel):
    """
    KeplerVisibleLayerGroups is a model that defines the visibility of various layer
    groups in a Kepler.gl map.

    Attributes:
        label (bool): Visibility of the label layer group. Default is True.
        road (bool): Visibility of the road layer group. Default is True.
        border (bool): Visibility of the border layer group. Default is True.
        building (bool): Visibility of the building layer group. Default is True.
        water (bool): Visibility of the water layer group. Default is True.
        land (bool): Visibility of the land layer group. Default is True.

    """

    label: bool = True
    road: bool = True
    border: bool = True
    building: bool = True
    water: bool = True
    land: bool = True


class KeplerMapStyle(BaseModel):
    """
    KeplerMapStyle is a model that defines the style settings for a Kepler.gl map.

    Attributes:
        styleType (Literal["dark", "light", "satellite"]): The type of map style to be used. Defaults to "dark".
        visibleLayerGroups (KeplerVisibleLayerGroups): An instance of KeplerVisibleLayerGroups that defines which layer groups are visible on the map.

    """

    styleType: Literal["dark", "light", "satellite"] = "dark"
    visibleLayerGroups: KeplerVisibleLayerGroups = KeplerVisibleLayerGroups()


class KeplerConfig(BaseModel):
    """
    KeplerConfig is a configuration model for Kepler.gl visualization.

    Attributes:
        visState (KeplerVisState): The visual state configuration for Kepler.gl.
        mapState (KeplerMapState): The map state configuration for Kepler.gl.
        mapStyle (KeplerMapStyle): The map style configuration for Kepler.gl.

    """

    visState: KeplerVisState
    mapState: KeplerMapState
    mapStyle: KeplerMapStyle


class MapFilter(BaseModel):
    """
    Public-facing class used to represent a filter for a Kepler.gl map.

    Attributes:
        col_name (str): The name of the column to be filtered.
        default_value (list[str]): The default values for the filter.

    """

    col_name: str
    default_value: list[str]


class VoterAddressLayer(BaseModel):
    """
    VoterAddressLayer is a model that represents a layer of voter addresses on a
    Kepler.gl map.

    Attributes:
        voter_addresses (gpd.GeoDataFrame): A GeoDataFrame containing voter addresses.
        tooltip_cols (list[str]): A list of column names to be used for tooltips.
        geojson_col (str): The name of the column containing GeoJSON geometry data. Default is "geometry".
        color (list[int, int, int]): The RGB color for the layer. Default is [207, 216, 244].
        is_visible (bool): A flag indicating whether the layer is visible. Default is True.
        vis_config (KeplerVisConfig): Visualization configuration for the layer, including radius and opacity. Default is KeplerVisConfig(radius=2, opacity=0.2).

    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    voter_addresses: gpd.GeoDataFrame
    tooltip_cols: list[str]
    geojson_col: str = "geometry"
    color: list[int, int, int] = [207, 216, 244]
    is_visible: bool = True
    vis_config: KeplerVisConfig = KeplerVisConfig(radius=2, opacity=0.2)


class TravelTimeRadiusLayer(BaseModel):
    """
    TravelTimeRadiusLayer represents a layer on a Kepler.gl map that visualizes travel
    time radii around ballot boxes.

    Attributes:
        ballot_box_isochrones (gpd.GeoDataFrame): GeoDataFrame containing isochrones for ballot boxes.
        tooltip_cols (list[str]): List of column names to be used for tooltips.
        geojson_col (str): Name of the column containing GeoJSON geometries. Defaults to "geometry".
        color (list[int, int, int]): RGB color for the layer. Defaults to [227, 151, 10].
        is_visible (bool): Flag indicating whether the layer is visible. Defaults to True.
        vis_config (KeplerVisConfig): Visualization configuration for the layer.
        filters (list[MapFilter] | None): Optional list of filters to apply to the layer. Defaults to None.

    """

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
    """
    BallotBoxLayer represents a layer of ballot box locations on a Kepler.gl map.

    Attributes:
        ballot_boxes (gpd.GeoDataFrame): A GeoDataFrame containing the ballot box data.
        tooltip_cols (list[str]): A list of column names to be shown in the tooltip.
        geojson_col (str): The name of the column containing the geometry data. Default is "geometry".
        color (list[int, int, int]): The RGB color of the ballot boxes. Default is [255, 255, 255].
        is_visible (bool): A flag indicating whether the layer is visible. Default is True.
        vis_config (KeplerVisConfig): Configuration for the visualization of the ballot boxes.

    """

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
    """
    KeplerMapLayerTitles is an enumeration of titles for different map layers used in
    the Kepler.gl map visualization.

    Attributes:
        COUNTY_BOUNDARY (str): Represents the county boundary layer.
        VOTER_ADDRESS (str): Represents the voter address layer.
        TRAVEL_TIME_RADIUS (str): Represents the travel time radius layer.
        BALLOT_BOX (str): Represents the ballot box layer.

    """

    COUNTY_BOUNDARY = "County Boundary"
    VOTER_ADDRESS = "Voter Address"
    TRAVEL_TIME_RADIUS = "Travel Time Radius"
    BALLOT_BOX = "Ballot Box"


class InvalidCountyError(Exception):
    """
    Exception raised for errors in the input county format.

    Attributes:
        message (str): Explanation of the error.

    """

    def __init__(self):
        self.message = "Invalid county format provided. Please provide the county and state separated by a comma (e.g., 'Monmouth, NJ')."
        super().__init__(self.message)


class KeplerMap:
    """
    A class to create and manage a Kepler.gl map for a specified county.

    Attributes:
        config (KeplerConfig): The configuration for the Kepler.gl map.
        map (KeplerGl): The Kepler.gl map instance.

    """

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
        """
        Initialize the Kepler.gl configuration for the map visualization.

        Args:
            county_centroid (shapely.Point): The centroid point of the county used to set the initial map state.

        Returns:
            KeplerConfig: The configuration object for Kepler.gl map visualization.

        """
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
        """
        Updates the map configuration with the current model configuration.

        Returns:
            None

        """
        self.map.config = {"version": "v1", "config": self.config.model_dump()}

    @staticmethod
    def _get_county_boundary(county: str) -> gpd.GeoDataFrame:
        """
        Retrieve the geographical boundary of a specified county.

        Args:
            county (str): The name of the county followed by the state abbreviation, separated by a comma (e.g., "Monmouth, NJ").

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the boundary of the specified county in EPSG:4326 coordinate reference system.

        Raises:
            InvalidCountyError: If the input county string is not in the expected format.

        """
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
        """
        Displays the current Kepler.gl map instance.

        Returns:
            KeplerGl: The current map instance.

        """
        return self.map

    def export(self, file_name: PosixPath | str) -> None:
        """
        Exports the map to an HTML file.

        Args:
            file_name (PosixPath | str): The path or name of the file where the map will be saved.

        """
        self.map.save_to_html(file_name=file_name)


class IsochroneMap(KeplerMap):
    """
    IsochroneMap is a specialized KeplerMap that visualizes voter addresses, travel time
    radii, and ballot box locations on a map for a given county.

    Attributes:
        county (str): The name of the county for which the map is generated.
        voter_address_layer (VoterAddressLayer): Layer containing voter address data.
        travel_time_radius_layer (TravelTimeRadiusLayer): Layer containing travel time radius data.
        ballot_box_layer (BallotBoxLayer): Layer containing ballot box location data.

    """

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
        """
        Adds a voter address layer to the map configuration and updates the map.

        Args:
            voter_address_layer (VoterAddressLayer): The voter address layer to be added, containing configuration details
                such as color, visibility, and tooltip columns.

        Returns:
            None

        """
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
        """
        Adds a travel time radius layer to the map configuration and updates the map.

        Args:
            travel_time_radius_layer (TravelTimeRadiusLayer): The travel time radius layer to be added, containing configuration details
                such as color, visibility, and tooltip columns.

        Returns:
            None

        """
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
        """
        Adds a ballot box location layer to the map configuration and updates the map.

        Args:
            ballot_box_layer (BallotBoxLayer): The ballot box location layer to be added, containing configuration details
                such as color, visibility, and tooltip columns.

        Returns:
            None

        """
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
