

import pandas as pd
import requests
import json
import time
from datetime import datetime
from concurrent.futures import as_completed
from google.cloud import pubsub_v1

project_id = "data-engineering-ij-indiv"
topic_id = "lab-breadcrumbs-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load 100 vehicle IDs
vehicles = pd.read_csv("vehicle_sample_100.csv", header=None)[0].tolist()

print(f"🚍 Using {len(vehicles)} vehicles.")

all_data = []

# Step 1: Gather breadcrumbs
start_fetch = time.time()
for vid in vehicles:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
    try:
        resp = requests.get(url, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            for r in data:
                r["FETCHED_AT"] = datetime.now().isoformat()
                all_data.append(r)
    except Exception as e:
        print(f"⚠️ Error for {vid}: {e}")

fetch_time = time.time() - start_fetch
print(f"✅ Gathered {len(all_data)} breadcrumb records.")
print(f"⏱️ Fetch time: {fetch_time:.2f} sec")

# Save combined file
file_name = "breadcrumbs_100.json"
with open(file_name, "w") as f:
    json.dump(all_data, f, indent=2)

print(f"💾 Saved to {file_name}")

# Step 2: Publish with future batching
publish_start = time.time()

futures = []
for record in all_data:
    record_json = json.dumps(record).encode("utf-8")
    futures.append(publisher.publish(topic_path, record_json))

for future in as_completed(futures):
    future.result()  # ensure success

publish_time = time.time() - publish_start
print(f"✅ Published {len(all_data)} records")
print(f"⏱️ Publish time: {publish_time:.2f} sec")









