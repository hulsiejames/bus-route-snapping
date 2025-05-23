# -*- coding: utf-8 -*-
"""
Created on Thu May 22 17:29:27 2025

@author: Signalis
"""

import pandas as pd
import pathlib
from datetime import datetime, timedelta

import gpxpy
import gpxpy.gpx
from tqdm import tqdm


def ct():
    return datetime.now().strftime("%H:%M:%S")


GTFS_DIR = pathlib.Path(r"E:\Repos\bus-route-snapping\assets\gtfs\north_west")
GPX_TRACES_DIR = pathlib.Path(r"E:\Repos\bus-route-snapping\assets\gpx_traces")

DEPARTURE_TIME = "08:15:00"
TRIP_TIME_MINS = 90


import requests


def send_gpx_to_graphhopper_local(
    gpx_file_path, profile="foot", host="http://localhost:8989"
):
    """
    Sends a GPX file to a local GraphHopper Map Matching API instance.

    Parameters:
    - gpx_file_path (str): Path to the GPX file.
    - profile (str): The profile to use for map matching (e.g., 'car').
    - host (str): Base URL of the local GraphHopper instance.

    Returns:
    - response (requests.Response): The response from the API.
    """
    url = f"{host}/match?profile={profile}"
    headers = {"Content-Type": "application/gpx+xml"}

    with open(gpx_file_path, "r") as gpx_file:
        gpx_data = gpx_file.read()

    response = requests.post(url, headers=headers, data=gpx_data)

    return response


def create_gpx_files_from_csv(csv_path: pathlib.Path, output_dir: pathlib.Path):
    # Read the CSV file
    print(f"Reading: {csv_path}")
    df = pd.read_csv(csv_path)

    # Ensure the output directory exists
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Iterate over each row in the DataFrame
    for index, row in tqdm(df.iterrows(), desc="Creating GPX traces", total=len(df)):
        trip_id = row["trip_id"]
        stop_coordinates = eval(
            row["stop_coordinate_array"]
        )  # Convert string representation of list to actual list

        # Create a new GPX object
        gpx = gpxpy.gpx.GPX()

        # Create a new track in the GPX object
        gpx_track = gpxpy.gpx.GPXTrack()
        gpx.tracks.append(gpx_track)

        # Create a new segment in the track
        gpx_segment = gpxpy.gpx.GPXTrackSegment()
        gpx_track.segments.append(gpx_segment)

        # Add points to the segment
        for lat, lon in stop_coordinates:
            gpx_segment.points.append(
                gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon)
            )

        # Save the GPX file
        gpx_file_path = output_dir / f"{trip_id}_stop_trace.gpx"
        with open(gpx_file_path, "w") as f:
            f.write(gpx.to_xml())


print("reading stop times")
stop_times = pd.read_csv(GTFS_DIR / "stop_times.txt", low_memory=False)
print("reading trips")
trips = pd.read_csv(GTFS_DIR / "trips.txt", low_memory=False)
print("reading routes")
routes = pd.read_csv(GTFS_DIR / "routes.txt", low_memory=False)
print("reading stops")
stops = pd.read_csv(GTFS_DIR / "stops.txt", low_memory=False)
print("data read")

# st_test = stop_times.groupby("trip_id")

# Convert departure_time column to datetime.time for comparison
stop_times["departure_time"] = pd.to_datetime(
    stop_times["departure_time"],
    format="%H:%M:%S",
    errors="coerce",
).dt.time

if DEPARTURE_TIME:

    if not TRIP_TIME_MINS:
        raise ValueError(
            "`TRIP_TIME_MINS must be specified if `DEPARTURE_TIME` is not None"
        )

    departure_time = pd.to_datetime(DEPARTURE_TIME).time()
    departure_time = datetime.strptime(DEPARTURE_TIME, "%H:%M:%S")
    trip_end_time = departure_time + timedelta(minutes=TRIP_TIME_MINS)

    print(f"Filtering trips for period: {departure_time} -> {trip_end_time}")

    i_len = len(stop_times)
    # Filter based on time window
    stop_times = stop_times[
        (stop_times["departure_time"] >= departure_time.time())
        & (stop_times["departure_time"] <= trip_end_time.time())
    ]
    print(
        f"Removed {i_len - len(stop_times):,} rows for being outside of the time window: {departure_time.time()} - {trip_end_time.time()}"
    )

# departure_time = pd.to_datetime(DEPARTURE_TIME).time()
# departure_time = datetime.strptime(DEPARTURE_TIME, "%H:%M:%S")
# trip_end_time = departure_time + timedelta(minutes=TRIP_TIME_MINS)


# THings to investigate
top_check = stop_times.loc[
    stop_times["trip_id"] == "VJd52ffba9e123b1a8d51fd4e3844e51e0d506f99f"
].copy()
top_check_pls_be_the_same = stop_times.loc[
    stop_times["trip_id"] == "VJf470303a637ffc221bcc1c814e0e788f2c883b44"
].copy()

st_grp_test = stop_times.loc[stop_times.groupby("trip_id")["stop_sequence"].idxmax()]
# No count in here should be above 1 if this has worked
st_grp_test["trip_id"].value_counts()


# Max stop sequence SHOULD be 3 here.
assure_st_grp_test = stop_times.loc[
    stop_times["trip_id"] == "VJffffe5f8fd900a3e8e9e64e4691e556ddaeed637"
].copy()
# NB stop_sequence starts from 0 indexed stop - so above is acutally 4 stop route


print(f"commencing merge {ct()}")
trip_stop_times = pd.merge(
    left=trips,
    right=stop_times,
    left_on="trip_id",
    right_on="trip_id",
)
print(f"merge complete {ct()}")
print(
    f"With {len(trip_stop_times):,} rows and {len(trip_stop_times['trip_id'].unique()):,} unique trip ids"
)

# Then apply groupby on route_id trip_id & idxmax on stop_sequence!
print(f"commencing groupby {ct()}")
trip_stop_times_grped = trip_stop_times.loc[
    trip_stop_times.groupby(["route_id", "trip_id"])["stop_sequence"].idxmax()
].copy()
print(f"completed {ct()}")
print(
    f"leaving {len(trip_stop_times_grped):,} rows and {trip_stop_times_grped['trip_id'].nunique():,} unique trip ids"
)


i_len = len(trip_stop_times)
# bprint(
#    f"Rmoving trips with a stop departure time before departure_time {departure_time}"
# )
# Now, iterate over every identified trip_id and identify it's stop sequence
valid_trip_id = trip_stop_times.copy()
# Filter rows where 'departure_time' is greater than or equal to DEPARTURE_TIME
# valid_trip_id = valid_trip_id[valid_trip_id['departure_time'] >= DEPARTURE_TIME]
# print(f"Removed {i_len - len(valid_trip_id):,} rows before {departure_time} ({len(valid_trip_id):,} remain)")

# Join on stops
valid_trip_id = pd.merge(
    left=valid_trip_id,
    right=stops,
    left_on="stop_id",
    right_on="stop_id",
    indicator=True,
    validate="m:1",
)
print(valid_trip_id["_merge"].value_counts())
valid_trip_id = valid_trip_id.drop(columns=["_merge"])


trip_ids = list(valid_trip_id["trip_id"].unique())


# Dictionary to store coordinates for each trip_id
#
#
# =============================================================================
# # Iterate over each trip_id
# for trip_id in tqdm(trip_ids, desc="building coordinate arrays", total=len(trip_ids)):
#     # Filter rows matching the current trip_id
#     trip_data = valid_trip_id[valid_trip_id["trip_id"] == trip_id]
#
#     # Sort by stop_sequence
#     trip_data = trip_data.sort_values(by="stop_sequence")
#
#     # Extract coordinates (stop_lat, stop_lon) in the order of stop_sequence
#     coordinates = list(zip(trip_data["stop_lat"], trip_data["stop_lon"]))
#
#     # Store the coordinates in the dictionary
#     trip_coordinates[trip_id] = coordinates
# =============================================================================

# Attempt 2
grouped = valid_trip_id.groupby("trip_id")


trip_coordinates = {}
trip_data_dict = {}

for trip_id, trip_data in tqdm(
    grouped, desc="building coordinate arrays", total=len(grouped)
):
    trip_data = trip_data.sort_values("stop_sequence")
    stop_coordinates = list(zip(trip_data["stop_lat"], trip_data["stop_lon"]))
    trip_coordinates[trip_id] = stop_coordinates
    stop_ids = list(trip_data["stop_id"])
    # stop_times = list(trip_data["departure_time"].dt.time())
    # stop_times = [t.strftime("%H:%M:%S") for t in trip_data["departure_time"].dt.time]
    stop_times = [
        pd.to_datetime(t, format="%H:%M:%S").strftime("%H:%M:%S")
        for t in trip_data["departure_time"]
    ]

    trip_data_dict[trip_id] = {
        "stop_coordinate_array": stop_coordinates,
        "stop_ids": stop_ids,
        "stop_times": stop_times,
    }


df_trip_coordinates = pd.DataFrame(
    list(trip_coordinates.items()), columns=["trip_id", "stop_coordinate_array"]
)
df_trip_data = pd.DataFrame(
    [{"trip_id": trip_id, **data} for trip_id, data in trip_data_dict.items()]
)

if departure_time:
    time_period = f"{str(departure_time.time()).replace(':','-')}_{str(trip_end_time.time()).replace(':','-')}"
else:
    time_period = "all_services"

df_trip_coordinates.to_csv(f"{time_period}_trip_stop_times.csv", index=False)
df_trip_data.to_csv(f"{time_period}_trip_data_stop_times.csv", index=False)

print(f"Exported dataset: {time_period}_trip_stop_times.csv")

test = trip_stop_times_grped.loc[trip_stop_times_grped["route_id"] == 2].copy()

print(f"Creating GPX traces for {len(df_trip_data):,} trips")
create_gpx_files_from_csv(
    csv_path=f"{time_period}_trip_data_stop_times.csv",
    output_dir=GPX_TRACES_DIR,
)

print("Process completed!")

test = send_gpx_to_graphhopper_local(
    gpx_file_path=r"E:\Repos\bus-route-snapping\assets\gpx_traces\VJ00a68be87a2da89846fe3076fcc2c68a980838ce_stop_trace.gpx",
    profile="foot",
)
print(test)

print("debug test")

# =============================================================================
# 025-05-22 17:23:02,579 - INFO - Route id: 29 has 27 unique associated trip_ids
# 2025-05-22 17:23:02,580 - INFO - Unique trip_id counts per trip_id:
# trip_id
# VJd52ffba9e123b1a8d51fd4e3844e51e0d506f99f    1
# VJf470303a637ffc221bcc1c814e0e788f2c883b44    1

# VJ513afd2d10bec32cd72e380e03d10635141d9540    1
# VJ8db3e71bc090b36f9cf933ef2519720096c8f58a    1
# VJb5a70765e3eb60c5717a37298d09ba2bb5e1d8a7    1
# VJba62bb4cbdbb57a4580b31042bc1d2bd020394e8    1
# VJc06448d3003a79ab7961b7337d476645cc6fe27f    1
# VJ05c0cd9c4814ee1b8bdc6316fe0c0bfcb946d241    1
# VJ2b2b593ccb062562b597c1d793a32d325a8160f8    1
# VJ307b063c4958ee3ed206a577933db2cf7f982eb7    1
# VJf5b4b14853ca2ccd3ff6eb8d09700e270e0219a4    1
# VJ12f1edfd40627024919b2ed1237aa94516d5e1a1    1
# VJ6af69036c81638391e9dd3f1ddf79f8af7652959    1
# VJbad51b6b560881bfa12f27bbb0aae07b3f38e6c0    1
# VJab36f35ad42a9795d2b86abe20a039fd0c19629e    1
# VJ2911ba224f432a78f14ae76a7cb88708a0444d2f    1
# VJ4e0d1761fc6a27aca270c12cc9d14f9135fd0dd6    1
# VJ7b71913802b0944ad2b75052f6abf04df3cc5b6f    1
# VJ66a419dffb09ecaad12a7d2a52d1916a4d38c9f3    1
# VJ9c1c9537d8de9b25468e065ecc168f68468081e3    1
# VJaeb6d92b88b246eaa4cde9a0e9ba03e7b5ca0824    1
# VJ4f59f60ea46520ff783e769e8fa6425789d62cb8    1
# VJb75e07756df70e3104de3c4c9a77bcd310218844    1
# VJd5556a9f5549eff9f0f4add42d30384b8b8aaf27    1
# VJ866b63d05acb44a74a45db51a22a91134bf147b1    1
# VJ47991f9612b6da9863bdd28e1069be180048f313    1
# VJ8b656cfc8f2d8c90091676a8ebd6ffb18d1ab607    1
# =============================================================================


# =============================================================================
# 025-05-22 17:17:01,205 - INFO - Route id: 103798 has 18 unique associated trip_ids
# 2025-05-22 17:17:01,213 - INFO - Unique trip_id counts per trip_id:
# trip_id
# VJ3632947e4e409ed14b9db24d52c8915db23b767e    1
# VJba552053579a8b08e780d2b6e1059b94c7c77ccd    1
# VJ837d34a68cbf9f05d546884202cb779d7392e503    1
# VJead2e7dde16f5afde72eeca73fa3490201d74775    1
# VJ4939a7b932fb90fe977a54f6da5f299d610d556e    1
# VJca775f7a2a656c499c2a1564251a2a478494c770    1
# VJ78ee606fc30d2bebce9438a7e27eca701aaa9e11    1
# VJ9e06aca107068e20769ad4f35cf23e6ec6d844ae    1
# VJ97ffaee9f8f868dc6dedf29ad0afdc3f38e3e80e    1
# VJ432498c1b301f3aed8719a1744851b2fd63d077d    1
# VJ515e9be3651e0a1cdfec10d5222ea5ddc1947ce1    1
# VJ3dc650ed15c78bc846cbf171bcdcd43586fb1d05    1
# VJ9d1c87e22166edf33eaf5a86db0712c3f20d4c3f    1
# VJa86e94958f66783d429e424471c59771ef0d25ed    1
# VJ4d865d06534ced79daed5ad0b5043325af415000    1
# VJ0eebc3308eec7d0a3afd8f15d56a0dbd1d8e9e32    1
# VJ4a1cc970b9c4045a2e61e05a08b39219f9b18250    1
# VJce10856055b8e91a401341ca19f1e527cf33a10d    1
#
# =============================================================================
