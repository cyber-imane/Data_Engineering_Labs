










import json
from datetime import datetime
from google.cloud import pubsub_v1

project_id = "data-engineering-ij-indiv"
subscription_id = "lab-breadcrumbs-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0

def callback(message):
    global message_count
    try:
        data = json.loads(message.data.decode("utf-8"))
        message_count += 1

        # Try all known key names for vehicle id
        vehicle_id = (
            data.get("vehicle_id") or 
            data.get("VehicleID") or 
            data.get("VEHICLE_ID") or 
            "UNKNOWN"
        )

        # Save the record
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"lab_sub_output_{today}.json"
        with open(filename, "a") as f:
            json.dump(data, f)
            f.write("\n")

        message.ack()

        print(f"📥 Received #{message_count} | Vehicle: {vehicle_id}")

    except Exception as e:
        print(f"❌ Error: {e}")
        message.nack()


print(f"📡 Listening for lab messages on {subscription_path}")

try:
    future = subscriber.subscribe(subscription_path, callback=callback)
    future.result()
except KeyboardInterrupt:
    print("\n🛑 Stopping subscriber…")
    print(f"📦 Total messages received: {message_count}")
    future.cancel()
    subscriber.close()

