import hashlib
import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path, PosixPath

import censusgeocode as cg
import duckdb
import geopandas as gpd
import pandas as pd
import requests
from loguru import logger

# TODO: Add documentation to make it clear that this only accepts deconstructed addresses; open issue for alternative


class Geocoder:
    def __init__(
        self,
        addresses_df: pd.DataFrame,
        address_col: str,
        city_col: str,
        state_col: str,
        zip_col: str,
        unit_col: str | None = None,
        cache_dir: PosixPath = Path(".geocoding_cache"),
        duckdb_path: PosixPath = Path("scratch.db"),
        duckdb_table: str = "voters",
    ):
        self.addresses_df = addresses_df
        self.address_col = address_col
        self.city_col = city_col
        self.state_col = state_col
        self.zip_col = zip_col
        self.unit_col = unit_col

        self.cache_dir = cache_dir

        self.census_success = cache_dir / "census" / "success"
        self.census_fail = cache_dir / "census" / "fail"

        self.google_success = cache_dir / "google" / "success"
        self.google_fail = cache_dir / "google" / "fail"

        for directory in [self.cache_dir, self.census_success, self.census_fail, self.google_success, self.google_fail]:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"Geocoding cache created at: {self.cache_dir}")

        self.duckdb_path = duckdb_path
        self.duckdb_table = duckdb_table
        self._init_duckdb()

        logger.info(f"DuckDB database created at: {self.duckdb_path}. Table name: {self.duckdb_table}")

    def _init_duckdb(self) -> None:
        self.conn = duckdb.connect(str(self.duckdb_path))

        # Create tables if they don't exist
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.duckdb_table} (
                building_id VARCHAR PRIMARY KEY,
                street_address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip_code VARCHAR,
                lat DOUBLE,
                lng DOUBLE,
                geocoding_source VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _generate_id(self, row: pd.Series, include_unit: bool = False) -> str:
        components = [
            row[self.address_col],
            row[self.city_col],
            row[self.state_col],
            row[self.zip_col],
        ]

        if include_unit and self.unit_col:
            components.append(str(row[self.unit_col]))

        return hashlib.sha256("".join(components).lower().encode()).hexdigest()

    def _get_existing(self) -> pd.DataFrame:
        return self.conn.execute(
            f"SELECT DISTINCT building_id, lat, lng, geocoding_source FROM {self.duckdb_table}"  # noqa: S608
        ).fetchdf()

    def _join_existing(self) -> gpd.GeoDataFrame:
        geocoded = self._get_existing()
        self.addresses_df = self.addresses_df.merge(geocoded, on="building_id", how="left")
        return gpd.GeoDataFrame(
            self.addresses_df,
            geometry=gpd.points_from_xy(self.addresses_df["lng"], self.addresses_df["lat"]),
            crs="EPSG:4326",
        )

    @staticmethod
    def _geocode_single_google(
        building_id: str,
        addr: str,
        google_success: PosixPath,
        google_fail: PosixPath,
    ) -> dict | None:
        google_api_key = os.environ.get("GOOGLE_API_KEY")
        if not google_api_key:
            raise ValueError("Please set the environment variable GOOGLE_API_KEY.")  # noqa: TRY003

        cache_file = google_success / f"{building_id}.json"
        fail_file = google_fail / f"{building_id}.json"

        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        if fail_file.exists():
            return None

        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={
                "address": addr,
                "key": google_api_key,
            },
            timeout=10,
        )

        r.raise_for_status()
        r_json: dict = r.json()
        r_status = r_json.get("status")

        if r_status == "OK":
            with open(cache_file, "w") as f:
                json.dump(r_json, f, indent=4)
            return r_json
        else:
            with open(fail_file, "w") as f:
                json.dump(None, f)
            return None

    @staticmethod
    def _geocode_single(
        row: pd.Series,
        census_success: PosixPath,
        census_fail: PosixPath,
        google_success: PosixPath,
        google_fail: PosixPath,
        address_col: str,
        city_col: str,
        state_col: str,
        zip_col: str,
    ) -> dict | None:
        building_id = row["building_id"]
        cache_file = census_success / f"{building_id}.json"
        fail_file = census_fail / f"{building_id}.json"

        addr = " ".join([
            row[address_col],
            row[city_col],
            row[state_col],
            row[zip_col],
        ])

        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        if fail_file.exists():
            return Geocoder._geocode_single_google(
                building_id=building_id, addr=addr, google_success=google_success, google_fail=google_fail
            )

        try:
            result = cg.onelineaddress(addr)

            if result:
                with open(cache_file, "w") as f:
                    json.dump(result, f, indent=4)
                return result
            else:
                with open(fail_file, "w") as f:
                    json.dump(None, f)
                return Geocoder._geocode_single_google(
                    building_id=building_id, addr=addr, google_success=google_success, google_fail=google_fail
                )

        except Exception as e:
            logger.error(f"[{building_id}] {e}")
            return None

    def geocode(self, batch_size: int = 500, processes: int = 50) -> gpd.GeoDataFrame:
        # Add IDs if not already present
        self.addresses_df.loc[:, "address_id"] = self.addresses_df.apply(self._generate_id, include_unit=True, axis=1)
        self.addresses_df.loc[:, "building_id"] = self.addresses_df.apply(self._generate_id, axis=1)

        # Reorder columns
        self.addresses_df = self.addresses_df[
            ["address_id", "building_id"]
            + [col for col in self.addresses_df.columns if col not in ["address_id", "building_id"]]
        ]

        # Skip already processed addresses
        geocoded_buildings = self._get_existing()
        remaining_to_process = self.addresses_df.copy()
        remaining_to_process = remaining_to_process[
            ~remaining_to_process["building_id"].isin(geocoded_buildings["building_id"])
        ]

        # Process in batches
        remaining_to_process = remaining_to_process[
            ["building_id", self.address_col, self.city_col, self.state_col, self.zip_col]
        ].drop_duplicates()

        for batch_idx, batch_start in enumerate(range(0, len(remaining_to_process), batch_size)):
            batch = remaining_to_process.iloc[batch_start : batch_start + batch_size]
            _batch_size = len(batch)

            logger.info(f"[Batch {batch_idx}] Geocoding process started...")

            with ProcessPoolExecutor(processes) as executor:
                batch_results = list(
                    executor.map(
                        self._geocode_single,
                        [row for _, row in batch.iterrows()],
                        [self.census_success] * _batch_size,
                        [self.census_fail] * _batch_size,
                        [self.google_success] * _batch_size,
                        [self.google_fail] * _batch_size,
                        [self.address_col] * _batch_size,
                        [self.city_col] * _batch_size,
                        [self.state_col] * _batch_size,
                        [self.zip_col] * _batch_size,
                    )
                )

            geocoded_batch = pd.DataFrame([
                {
                    "building_id": row["building_id"],
                    "street_address": row[self.address_col],
                    "city": row[self.city_col],
                    "state": row[self.state_col],
                    "zip_code": row[self.zip_col],
                    "lat": r["results"][0]["geometry"]["location"]["lat"]
                    if isinstance(r, dict)
                    else r[0]["coordinates"]["y"],
                    "lng": r["results"][0]["geometry"]["location"]["lng"]
                    if isinstance(r, dict)
                    else r[0]["coordinates"]["x"],
                    "geocoding_source": "google" if isinstance(r, dict) else "census",
                    "created_at": datetime.now(),
                }
                for r, (_, row) in zip(batch_results, batch.iterrows(), strict=False)
                if r
            ])

            self.conn.append(self.duckdb_table, geocoded_batch)
            self.conn.commit()

            logger.info(
                f"[Batch {batch_idx}] Geocoding process completed. {len(geocoded_batch)} of {_batch_size} addresses successfully geocoded."
            )

        return self._join_existing()
