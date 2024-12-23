import geopandas as gpd
import pandas as pd
from tqdm import tqdm

# TODO: Parallelize `within` calls
# TODO: Cache results using DuckDB


class SpatialJoiner:
    def __init__(self, ballot_box_isochrones: gpd.GeoDataFrame, voter_addresses: gpd.GeoDataFrame) -> None:
        if not isinstance(ballot_box_isochrones, gpd.GeoDataFrame) or not isinstance(voter_addresses, gpd.GeoDataFrame):
            raise ValueError("Both input dataframes must be GeoPandas geodataframes.")  # noqa: TRY003, TRY004

        self.ballot_box_isochrones = ballot_box_isochrones
        self.voter_addresses = voter_addresses

    def summary(self, count_voters_col: str) -> pd.DataFrame:
        within_any_true = 0
        total_count_voters = self.voter_addresses[count_voters_col].sum()

        building_addresses = (
            self.voter_addresses.groupby(["building_id", "geometry"])[count_voters_col]
            .sum()
            .reset_index(name=count_voters_col)
        )

        for _, voter_row in tqdm(building_addresses.iterrows(), total=building_addresses.shape[0]):
            voter_geometry = voter_row["geometry"]
            count_voters = voter_row[count_voters_col]

            for _, ballot_box_row in self.ballot_box_isochrones.iterrows():
                ballot_box_geometry = ballot_box_row["geometry"]

                if voter_geometry.within(ballot_box_geometry):
                    within_any_true += count_voters
                    break

        within_any_false = total_count_voters - within_any_true

        rollup = pd.DataFrame({
            "within_any": [True, False],
            "count_voters": [within_any_true, within_any_false],
            "share_voters": [(within_any_true / total_count_voters), (within_any_false / total_count_voters)],
        })

        return rollup
