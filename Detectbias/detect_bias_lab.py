



#!/usr/bin/env python3

import pandas as pd
from bs4 import BeautifulSoup
from scipy.stats import binomtest, ttest_ind

# =============================================================
# 1. LOAD STOP EVENTS (MULTI-TABLE HTML)
# =============================================================

STOP_EVENTS_FILE = "trimet_stopevents.html"

print("\nLoading & parsing STOP EVENTS HTML...\n")

with open(STOP_EVENTS_FILE, "r", encoding="utf-8", errors="ignore") as f:
    soup = BeautifulSoup(f.read(), "html.parser")

tables = soup.find_all("table")
print(f"Found {len(tables)} tables in stop-events HTML.")

df_list = []
for tbl in tables:
    try:
        df = pd.read_html(str(tbl))[0]
        df_list.append(df)
    except:
        pass

print(f"Parsed {len(df_list)} tables.")
stops_df = pd.concat(df_list, ignore_index=True)
print(f"Combined STOP EVENTS rows: {len(stops_df)}")

# =============================================================
# 2. CLEAN & TRANSFORM STOP EVENTS
# =============================================================

print("\nCleaning STOP EVENTS data...")

stops_df = stops_df[[
    "trip_number",
    "vehicle_number",
    "arrive_time",
    "location_id",
    "ons",
    "offs"
]].copy()

# Rename columns to lab-required names
stops_df.columns = ["trip_id", "vehicle_number", "arrive_time",
                    "location_id", "ons", "offs"]

# Convert types
stops_df["trip_id"] = stops_df["trip_id"].astype(int)
stops_df["vehicle_number"] = stops_df["vehicle_number"].astype(int)
stops_df["location_id"] = stops_df["location_id"].astype(int)
stops_df["ons"] = stops_df["ons"].astype(int)
stops_df["offs"] = stops_df["offs"].astype(int)
stops_df["arrive_time"] = stops_df["arrive_time"].astype(int)

# Convert arrive_time (sec since midnight) → timestamp
stops_df["tstamp"] = pd.to_datetime(
    stops_df["arrive_time"], unit="s", origin="2022-12-07"
)

print("STOP EVENTS DataFrame ready.\n")


# =============================================================
# 2 — LAB ANSWERS
# =============================================================

print("\n==================")
print("LAB PART 2 ANSWERS")
print("==================\n")

A = stops_df["vehicle_number"].nunique()
B = stops_df["location_id"].nunique()
C_min = stops_df["tstamp"].min()
C_max = stops_df["tstamp"].max()
D = (stops_df["ons"] >= 1).sum()
E = 100 * D / len(stops_df)

print("A. Number of vehicles:", A)
print("B. Number of unique stop locations:", B)
print("C. Timestamp range:", C_min, "to", C_max)
print("D. Stops with >=1 boarding:", D)
print(f"E. % stops with boarding: {E:.2f}%")

overall_board_rate = D / len(stops_df)


# =============================================================
# 3 — VALIDATION
# =============================================================

print("\n==================")
print("LAB PART 3 ANSWERS")
print("==================\n")

# Location 6913
loc_df = stops_df[stops_df["location_id"] == 6913]
print("A.i Stops at location 6913:", len(loc_df))
print("A.ii # of buses at location:", loc_df["vehicle_number"].nunique())
if len(loc_df) > 0:
    print("A.iii % with boarding:",
          100 * (loc_df["ons"] >= 1).mean())
else:
    print("A.iii % with boarding: N/A")

# Vehicle 4062
bus_df = stops_df[stops_df["vehicle_number"] == 4062]
print("\nB.i Stops by vehicle 4062:", len(bus_df))
print("B.ii Total boarded:", bus_df["ons"].sum())
print("B.iii Total deboarded:", bus_df["offs"].sum())
if len(bus_df) > 0:
    print("B.iv % with boarding:",
          100 * (bus_df["ons"] >= 1).mean())
else:
    print("B.iv % with boarding: N/A")


# =============================================================
# 4 — BINOMIAL TEST FOR BOARDING BIAS
# =============================================================

print("\n==================")
print("LAB PART 4 ANSWERS")
print("==================\n")

biased_ons = []

for bus, g in stops_df.groupby("vehicle_number"):
    n = len(g)
    k = (g["ons"] >= 1).sum()
    p = binomtest(k, n, overall_board_rate).pvalue

    if p < 0.05:
        biased_ons.append((bus, p))

print("Vehicles with p < 0.05:")
for b, p in biased_ons:
    print(f"  Vehicle {b}: p={p:.6f}")

print("\nPossible reasons for bias:")
print("1. APC sensor malfunction")
print("2. Door sensor not recording events")
print("3. Data transmission gaps")


# =============================================================
# 5 — GPS RELPOS BIAS (t-test)
# =============================================================

print("\n==================")
print("LAB PART 5 ANSWERS")
print("==================\n")

RELPOS_FILE = "trimet_relpos.csv"
relpos_df = pd.read_csv(RELPOS_FILE)

all_relpos = relpos_df["RELPOS"].values
biased_gps = []

for bus, g in relpos_df.groupby("VEHICLE_NUMBER"):
    _, p = ttest_ind(g["RELPOS"].values, all_relpos, equal_var=False)
    if p < 0.005:
        biased_gps.append((bus, p))

print("Vehicles with GPS bias (p < 0.005):")
for b, p in biased_gps:
    print(f"  Vehicle {b}: p={p:.6e}")

print("\nPossible reasons:")
print("1. GPS antenna issues")
print("2. Route geometry effects")
print("3. Map-matching errors")








# =============================================================
# 6 — X2 TEST FOR OFFS VS ONS BIAS
# =============================================================

print("\n==================")
print("LAB PART 6 ANSWERS (GRAD STUDENTS)")
print("==================\n")

from scipy.stats import chi2_contingency

# Total system-level offs and ons
total_offs = stops_df["offs"].sum()
total_ons = stops_df["ons"].sum()

print("Total OFFS across system:", total_offs)
print("Total ONS across system:", total_ons)

biased_offs = []

for bus, g in stops_df.groupby("vehicle_number"):

    bus_offs = g["offs"].sum()
    bus_ons = g["ons"].sum()

    # Construct contingency table:
    #      ONS     OFFS
    # bus   a       b
    # system_rest (ONS-a) (OFFS-b)

    a = bus_ons
    b = bus_offs
    c = total_ons - a
    d = total_offs - b

    table = [[a, b],
             [c, d]]

    chi2, p, dof, expected = chi2_contingency(table)

    if p < 0.05:
        biased_offs.append((bus, p))

print("\nVehicles with OFFS/ONS bias (p < 0.05):")
for bus, p in biased_offs:
    print(f"  Vehicle {bus}: p={p:.6f}")

print("\n==============================")
print("ALL LAB ANSWERS GENERATED.")
print("==============================\n")

