



import requests
import json
from datetime import datetime
from google.cloud import pubsub_v1

# === LAB PUBSUB CONFIG ===
project_id = "data-engineering-ij-indiv"
topic_id = "lab-breadcrumbs-topic"  # Lab topic name

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# === Vehicles for Lab ===
vehicles = [3003, 3007]
print(f"✅ Using these two vehicles: {vehicles}")

all_data = []
total_published = 0

# === Step 1 — Fetch breadcrumbs and save ===
for vid in vehicles:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
    print(f"🚍 Fetching breadcrumbs for vehicle {vid}...")

    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()

            if len(data) == 0:
                print(f"⚠️ No breadcrumbs returned for vehicle {vid}")

            # Add timestamp and store
            for record in data:
                record["FETCHED_AT"] = datetime.now().isoformat()
                all_data.append(record)

        else:
            print(f"❌ API error vehicle {vid}: HTTP {response.status_code}")

    except Exception as e:
        print(f"❌ Error fetching vehicle {vid}: {e}")

# Save to JSON file
with open("bcsample.json", "w") as f:
    json.dump(all_data, f, indent=2)

print(f"💾 Saved {len(all_data)} records to bcsample.json\n")

# === Step 2 — Publish to Pub/Sub ===
print("📡 Publishing records to Pub/Sub...")
for record in all_data:
    msg = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, msg)
    future.result()
    total_published += 1

print(f"✅ Published {total_published} messages to Pub/Sub")

