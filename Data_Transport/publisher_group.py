

#!/usr/bin/env python3
import pandas as pd
import requests
import json
import time
from datetime import datetime
from concurrent.futures import as_completed
from google.cloud import pubsub_v1

# === CONFIG ===
PROJECT_ID = "data-engineering-ij-indiv"
TOPIC_ID = "lab-breadcrumbs-topic"
VEHICLE_CSV = "titan_vehicle_ids.csv"   # full project CSV
OUTPUT_JSON = "breadcrumbs_group.json"
TARGET_DATE = "2025-10-30"  # <-- PUT THE ONE SINGLE DAY YOU WANT (format YYYY-MM-DD)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def load_vehicle_ids(path):
    try:
        df = pd.read_csv(path)
        # handle header/no header
        if "Titans" in df.columns:
            return df["Titans"].astype(str).tolist()
        if "Titan" in df.columns:
            return df["Titan"].astype(str).tolist()
        return df.iloc[:,0].astype(str).tolist()
    except Exception as e:
        print(f"❌ Failed to read vehicle file: {e}")
        raise

def record_matches_day(rec, date_str):
    for key in ("time", "Time", "timestamp", "event_time", "utc_time", "vehicle_time"):
        if key in rec:
            val = str(rec[key])
            if val.startswith(date_str):
                return True
    return False

vehicles = load_vehicle_ids(VEHICLE_CSV)
print(f"🚍 Using {len(vehicles)} vehicles from {VEHICLE_CSV}")

all_records = []
fetch_start = time.time()

for vid in vehicles:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            filtered = [rec for rec in data if record_matches_day(rec, TARGET_DATE)]

            # fallback if API returns only same-day
            if len(filtered) == 0 and len(data) > 0:
                filtered = data

            for rec in filtered:
                rec["FETCHED_AT"] = datetime.utcnow().isoformat() + "Z"
                rec["__vehicle_id"] = vid
            
            all_records.extend(filtered)
            print(f"  ✅ Vehicle {vid}: {len(filtered)} records")
        else:
            print(f"  ❌ Vehicle {vid}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  ⚠️ Error fetching vehicle {vid}: {e}")

fetch_time = time.time() - fetch_start
print(f"\n✅ Total gathered: {len(all_records)} records in {fetch_time:.2f} sec")

# Save batch file
with open(OUTPUT_JSON, "w") as f:
    json.dump(all_records, f, indent=2)
print(f"💾 Saved combined records to {OUTPUT_JSON}")

# Publish to Pub/Sub using futures.as_completed()
publish_start = time.time()
futures = []

for rec in all_records:
    payload = json.dumps(rec).encode("utf-8")
    futures.append(publisher.publish(topic_path, payload))

for f in as_completed(futures):
    f.result()  # ensures no silent failures

publish_time = time.time() - publish_start

print(f"\n📡 Published {len(all_records)} messages to Pub/Sub topic: {TOPIC_ID}")
print(f"⏱️ Publish time: {publish_time:.2f} sec")




