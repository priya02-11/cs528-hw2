"""
Service 2: Local Error Logger & Subscriber
Runs on local laptop, subscribes to Pub/Sub, logs forbidden requests
Appends logs to ONE txt file in Google Cloud Storage (GCS).
"""

import os
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed
#Configuration
PROJECT_ID = "evident-gecko-486418-c7"
SUBSCRIPTION_ID = "hw3-error-logs-sub"
BUCKET_NAME = "priya-cc-hw2"

# One single log object in the bucket 
LOG_OBJECT_NAME = "forbidden_requests/forbidden_requests_log.txt"


# Initialize clients
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

print("=" * 60)
print(" SERVICE 2: Error Logger Started")
print(f" Project: {PROJECT_ID}")
print(f" Subscription: {SUBSCRIPTION_ID}")
print(f" Bucket: {BUCKET_NAME}")
print(f" Log File (GCS): gs://{BUCKET_NAME}/{LOG_OBJECT_NAME}")
print("=" * 60)
print("\n Listening for messages from Service 1...\n")


def append_to_gcs_txt(message_data: dict, max_retries: int = 8):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(LOG_OBJECT_NAME)

    timestamp_float = message_data.get("timestamp", time.time())
    timestamp_str = datetime.fromtimestamp(timestamp_float).strftime("%Y-%m-%d %H:%M:%S")

    new_line = (
        f"[{timestamp_str}] "
        f"status={message_data.get('status')} "
        f"method={message_data.get('method')} "
        f"path={message_data.get('path')} "
        f"error_type={message_data.get('error_type')} "
        f"country={message_data.get('country')}\n"
    )

    for attempt in range(1, max_retries + 1):
        try:
            # Reload metadata to get current generation 
            try:
                blob.reload() 
                generation = blob.generation
                existing = blob.download_as_text()
            except Exception:
                # If it truly doesn't exist yet, we create it with if_generation_match=0
                generation = 0
                existing = ""

            updated = existing + new_line

            # Upload only if object hasn't changed since we read it
            blob.upload_from_string(
                updated,
                content_type="text/plain",
                if_generation_match=generation
            )

            print(f" Appended to (safe): gs://{BUCKET_NAME}/{LOG_OBJECT_NAME}")
            return

        except PreconditionFailed:
            
            wait = 0.15 * attempt
            print(f" File changed while appending, retrying (attempt {attempt})...")
            time.sleep(wait)

    print(" Too much contention: could not safely append after retries.")


def callback(message):
    """Process each message received from Pub/Sub"""
    try:
        data = json.loads(message.data.decode("utf-8"))

        status = data.get("status")
        method = data.get("method")
        path = data.get("path")
        error_type = data.get("error_type")
        country = data.get("country")
        timestamp_float = data.get("timestamp", time.time())

        timestamp_str = datetime.fromtimestamp(timestamp_float).strftime("%Y-%m-%d %H:%M:%S")

        # Print to console
        print("-" * 60)
        print(" NEW MESSAGE RECEIVED")
        print(f" Timestamp: {timestamp_str}")
        print(f" Status: {status}")
        print(f" Method: {method}")
        print(f" Path: {path}")
        print(f" Error Type: {error_type}")
        if country:
            print(f"Country: {country}")

        # Only append forbidden-country logs to the single txt file
        if error_type == "FORBIDDEN_COUNTRY" and country:
            print("\n FORBIDDEN COUNTRY REQUEST DETECTED!")
            print(f"   Country: {country}")
            print("   Action: Appending to ONE txt file in GCS...")

            append_to_gcs_txt(data)

            print("  Forbidden request logged successfully!")

        elif error_type in ["NOT_FOUND", "NOT_IMPLEMENTED"]:
            print(f"\n ERROR LOGGED: {error_type}")

        elif error_type == "INTERNAL_ERROR":
            print("\n INTERNAL ERROR LOGGED")

        print("-" * 60)
        print()

        # Ack message so it is removed from subscription backlog
        message.ack()

    except Exception as e:
        print(f" Error processing message: {e}")
        print(f"   Raw data: {message.data}")
        message.nack()  


# Start subscribing
print(" Subscribing to Pub/Sub...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(" Successfully subscribed!\n")

try:
    streaming_pull_future.result()  
except KeyboardInterrupt:
    print("\n\n Stopping Service 2...")
    streaming_pull_future.cancel()
    print(" Service 2 stopped gracefully")
    print(" Goodbye!\n")
except Exception as e:
    print(f"\n Error: {e}")
    streaming_pull_future.cancel()