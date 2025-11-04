import pandas as pd

# New dataset (multi-trip)
url = "https://drive.google.com/uc?export=download&id=1IhbdnFL5tX3e1JhMUuD1vS5lbKEGYeM2"

print("=== Loading vehicle 4223 breadcrumb data ===")
df = pd.read_csv(url)
print(f"Total records: {len(df)}")

# Drop unused columns
cols_to_drop = ["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"]
df = df.drop(columns=cols_to_drop, errors="ignore")

# Keep relevant columns
use_columns = [
    "EVENT_NO_TRIP", "VEHICLE_ID", "OPD_DATE", "ACT_TIME",
    "METERS", "GPS_LATITUDE", "GPS_LONGITUDE"
]
df = df[use_columns]

# Convert OPD_DATE format
df["OPD_DATE"] = pd.to_datetime(
    df["OPD_DATE"], format="%d%b%Y:%H:%M:%S", errors="coerce"
)

# Convert ACT_TIME to timedelta
df["ACT_TIME"] = pd.to_timedelta(df["ACT_TIME"], unit="s")

# Create final timestamp
df["TIMESTAMP"] = df["OPD_DATE"] + df["ACT_TIME"]

# ----- Compute SPEED per trip (important!) -----
df = df.sort_values(["EVENT_NO_TRIP","TIMESTAMP"])

df["dMETERS"] = df.groupby("EVENT_NO_TRIP")["METERS"].diff()
df["dTIME"] = df.groupby("EVENT_NO_TRIP")["TIMESTAMP"].diff().dt.total_seconds()

df["SPEED"] = df.apply(
    lambda r: r["dMETERS"] / r["dTIME"] if r["dTIME"] and r["dTIME"] > 0 else 0,
    axis=1
)

# Drop intermediate columns
df = df.drop(columns=["OPD_DATE", "ACT_TIME", "dMETERS", "dTIME"])

print("\n--- Data Preview ---")
print(df.head())

# ----- Speed Statistics -----
max_speed = df["SPEED"].max()
median_speed = df["SPEED"].median()

max_row = df.loc[df["SPEED"].idxmax()]

print("\n--- SPEED RESULTS for Vehicle 4223 on Feb 15, 2023 ---")
print(f"Max speed: {max_speed:.2f} m/s ({max_speed*2.23694:.2f} mph)")
print(f"Occurred at: {max_row['TIMESTAMP']}")
print(f"Location: ({max_row['GPS_LATITUDE']}, {max_row['GPS_LONGITUDE']})")
print(f"Median speed: {median_speed:.2f} m/s ({median_speed*2.23694:.2f} mph)")

