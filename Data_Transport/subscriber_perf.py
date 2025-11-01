



import json
import time
from datetime import datetime
from google.cloud import pubsub_v1

project_id = "data-engineering-ij-indiv"
subscription_id = "lab-breadcrumbs-sub"

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, subscription_id)

count = 0
start_time = time.time()

def callback(msg):
    global count
    count += 1
    msg.ack()

    # Stop after all messages
    if count % 1000 == 0:
        print(f"📥 Received {count}")

def finish():
    total = time.time() - start_time
    print(f"✅ Total received: {count}")
    print(f"⏱️ Receive time: {total:.2f} sec")

print(f"📡 Listening on {sub_path}")

stream = subscriber.subscribe(sub_path, callback=callback)

try:
    stream.result()
except KeyboardInterrupt:
    finish()
    stream.cancel()
    subscriber.close()

