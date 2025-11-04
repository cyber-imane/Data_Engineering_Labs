import pandas as pd

# Google Drive direct download
url = "https://drive.google.com/uc?export=download&id=1mminKe9fwqpJ59POiveHv_UrRYVSPjjY"

# --------------------------
# 1. Load full dataset
# --------------------------
df = pd.read_csv(url)

print("=== Loading full dataset ===")
print(f"Total breadcrumb records: {len(df)}\n")
print("Preview of first 5 rows:")
print(df.head())

# --------------------------
# 2A. Drop unnecessary columns
# --------------------------
cols_to_drop = ["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"]

df_dropped = df.drop(columns=cols_to_drop, errors="ignore")

print("\n--- After drop() ---")
print("Remaining columns:")
print(df_dropped.columns.tolist())
print(f"Column count: {len(df_dropped.columns)}")

# --------------------------
# 2B. Load only useful columns with usecols
# --------------------------
use_columns = [
    "EVENT_NO_TRIP",
    "VEHICLE_ID",
    "OPD_DATE",
    "ACT_TIME",
    "METERS",
    "GPS_LATITUDE",
    "GPS_LONGITUDE"
]

df_usecols = pd.read_csv(url, usecols=use_columns)

print("\n--- Using usecols in read_csv ---")
print("Loaded columns:")
print(df_usecols.columns.tolist())
print(f"Column count: {len(df_usecols.columns)}")

print("\n✅ Filtering comparison complete.")



# --------------------------
# 3. Decode timestamps
# --------------------------
print("\nRaw OPD_DATE values:")
print(df_usecols["OPD_DATE"].head())

# ✅ Convert OPD_DATE using the proper format "15FEB2023:00:00:00"
df_usecols["OPD_DATE"] = pd.to_datetime(
    df_usecols["OPD_DATE"],
    format="%d%b%Y:%H:%M:%S",
    errors="coerce"
)

# ✅ Convert seconds after midnight to time delta
df_usecols["ACT_TIME"] = pd.to_timedelta(df_usecols["ACT_TIME"], unit="s")

# ✅ Create full timestamp
df_usecols["TIMESTAMP"] = df_usecols["OPD_DATE"] + df_usecols["ACT_TIME"]

print("\nDecoded timestamps (first 5 rows):")
print(df_usecols[["OPD_DATE", "ACT_TIME", "TIMESTAMP"]].head())


# --------------------------
# 3. Decode timestamps using apply()
# --------------------------
print("\nRaw OPD_DATE values:")
print(df_usecols["OPD_DATE"].head())

# Function to build timestamp
def make_timestamp(row):
    date = pd.to_datetime(row["OPD_DATE"], format="%d%b%Y:%H:%M:%S", errors="coerce")
    time = pd.to_timedelta(row["ACT_TIME"], unit="s")
    return date + time

# Apply function row-by-row
df_usecols["TIMESTAMP"] = df_usecols.apply(make_timestamp, axis=1)

print("\nDecoded timestamps (first 5 rows):")
print(df_usecols[["OPD_DATE", "ACT_TIME", "TIMESTAMP"]].head())

# --------------------------
# Drop OPD_DATE & ACT_TIME
# --------------------------
df_usecols = df_usecols.drop(columns=["OPD_DATE", "ACT_TIME"])

print("\nRemaining columns after dropping OPD_DATE and ACT_TIME:")
print(df_usecols.columns.tolist())
print(f"Column count: {len(df_usecols.columns)}")

print("\nFinal DataFrame preview:")
print(df_usecols.head())





# --------------------------
# 4. Enhance: Compute SPEED (m/s)
# --------------------------

# Calculate differences from previous row
df_usecols["dMETERS"] = df_usecols["METERS"].diff()
df_usecols["dTIMESTAMP"] = df_usecols["TIMESTAMP"].diff().dt.total_seconds()

# SPEED = distance / time
df_usecols["SPEED"] = df_usecols.apply(
    lambda row: row["dMETERS"] / row["dTIMESTAMP"] if row["dTIMESTAMP"] and row["dTIMESTAMP"] > 0 else 0,
    axis=1
)

# Drop helper columns
df_usecols = df_usecols.drop(columns=["dMETERS", "dTIMESTAMP"])

# Display first few rows
print("\nSpeed added (first 5 rows):")
print(df_usecols.head())

# Summary statistics
min_speed = df_usecols["SPEED"].min()
max_speed = df_usecols["SPEED"].max()
avg_speed = df_usecols["SPEED"].mean()

print("\n--- Speed Statistics (m/s) ---")
print(f"Min speed: {min_speed:.4f} m/s")
print(f"Max speed: {max_speed:.4f} m/s")
print(f"Average speed: {avg_speed:.4f} m/s")

