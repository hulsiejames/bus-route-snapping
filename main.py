# # # # IMPORTS # # # #

# system
import pathlib
import logging
import time

# 3rd party
import pandas as pd
import gpxpy
import gpxpy.gpx
from tqdm import tqdm

# # # # CONSTANTS # # # #
ROOT_DIR = pathlib.Path().resolve()
ASSETS_DIR = ROOT_DIR / "assets"
OUTPUTS_DIR = ROOT_DIR / "outputs"

GTFS_DIR = ASSETS_DIR / rf"gtfs/itm_all"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOG = logging.getLogger(__name__)

# # # # FUNCTIONS & CLASSES # # # #


def read_gtfs_files(gtfs_dir: pathlib.Path):
    """Read GTFS files and return as DataFrames"""
    LOG.info("Readings trips.txt")
    trips = pd.read_csv(gtfs_dir / "trips.txt", low_memory=False)
    LOG.info("Reading stop_times.txt")
    stop_times = pd.read_csv(gtfs_dir / "stop_times.txt", low_memory=False)
    LOG.info("Reading stops.txt")
    stops = pd.read_csv(gtfs_dir / "stops.txt", low_memory=False)
    return trips, stop_times, stops


def find_unique_routes(trips: pd.DataFrame) -> pd.Series:
    """Find all unique routes in the GTFS feed"""
    return trips["route_id"].unique()


def build_route_sequences(trips: pd.DataFrame, stop_times: pd.DataFrame):
    """Build a list of sequential stops for each route"""
    route_sequences = {}
    LOG.info("Identifying unqique route IDs")
    unique_routes = find_unique_routes(trips)

    for route_id in tqdm(
        unique_routes, desc="Processing unique routes", total=len(unique_routes)
    ):
        route_trips = trips[trips["route_id"] == route_id]
        route_sequences[route_id] = []

        # Implement efficiecnies here - currently we are searching multiple, different trips of a single route
        # we want to make it such that we only keep one unique instance of trip_ids for a route.
        # Seperately, we want to take note of the frequency of each route per day (dependant on the number of
        #  unique trip_ids that occur for a single route!)

        unique_trip_ids = route_trips["trip_id"].unique()
        trip_id_counts = route_trips["trip_id"].value_counts()

        LOG.info(
            "Route id: %s has %s unique associated trip_ids",
            route_id,
            len(unique_trip_ids),
        )
        LOG.info("Unique trip_id counts per trip_id: \n%s", trip_id_counts)

        for trip_id in route_trips["trip_id"]:
            trip_stop_times = stop_times[stop_times["trip_id"] == trip_id]
            trip_stop_times = trip_stop_times.sort_values(by="stop_sequence")
            route_sequences[route_id].append(trip_stop_times["stop_id"].tolist())
    return route_sequences


def convert_to_gpx(route_sequences, stops):
    """Convert route sequences to GPX format"""
    for route_id, sequences in tqdm(
        route_sequences.items(), desc="converting to gpx", total=len(route_sequences)
    ):
        gpx = gpxpy.gpx.GPX()
        for sequence in sequences:
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx.tracks.append(gpx_track)
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(gpx_segment)
            for stop_id in sequence:
                stop = stops[stops["stop_id"] == stop_id].iloc[0]
                gpx_segment.points.append(
                    gpxpy.gpx.GPXTrackPoint(stop["stop_lat"], stop["stop_lon"])
                )
        with open(OUTPUTS_DIR / f"route_{route_id}.gpx", "w", encoding="utf-8") as f:
            f.write(gpx.to_xml())


def main():
    """Main execution function"""

    start_time = time.time()
    LOG.info("Starting the main function.")

    # Step 1: Read GTFS files
    step_start_time = time.time()
    trips, stop_times, stops = read_gtfs_files(GTFS_DIR)
    LOG.info("Read GTFS files. Duration: %.2f seconds.", time.time() - step_start_time)

    # Step 2: Build route sequences
    step_start_time = time.time()
    route_sequences = build_route_sequences(trips, stop_times)
    LOG.info(
        "Built route sequences. Duration: %.2f seconds.", time.time() - step_start_time
    )

    # Step 3: Convert to GPX
    step_start_time = time.time()
    convert_to_gpx(route_sequences, stops)
    LOG.info(
        "Converted routes to GPX. Duration: %.2f seconds.",
        time.time() - step_start_time,
    )

    LOG.info(
        "Main function completed. Total duration: %.2f seconds.",
        time.time() - start_time,
    )


if __name__ == "__main__":
    main()
