import os
import json
import time
import functions_framework
from flask import Request
from google.cloud import storage
from google.cloud import logging
from google.cloud import pubsub_v1

BUCKET_NAME = "priya-cc-hw2"
PROJECT_ID = "evident-gecko-486418-c7"
TOPIC_ID = "hw3-errorlogs"

# FORBIDDEN COUNTRIES LIST
FORBIDDEN_COUNTRIES = {
    'north korea', 'iran', 'cuba', 'myanmar',
    'iraq', 'libya', 'sudan', 'zimbabwe', 'syria'
}

storage_client = storage.Client()

def publish_error(entry: dict):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    """Publish error events to Pub/Sub for Service 2"""
    if not publisher or not topic_path:
        return
    try:
        future = publisher.publish(topic_path, json.dumps(entry).encode("utf-8"))
        future.result()
    except Exception as e:
        print(json.dumps({"severity": "ERROR", "message": f"Pub/Sub publish failed: {e}"}))


def log_struct(status, method, path, country=None, severity="INFO", error_type=None):

    logging_client = logging.Client()
    logger = logging_client.logger("hw3-file-service")
    entry = {
        "status": status,
        "method": method,
        "path": path,
        "error_type": error_type,
        "timestamp": time.time(),
    }

    if country:
        entry["country"] = country

    if logger:
        logger.log_struct(entry, severity=severity)

    print(json.dumps({**entry, "severity": severity}))

    if status == 400:
        print(400, country)

    if status in [400, 404, 500, 501]:
        publish_error(entry)


@functions_framework.http
def serve_file(request: Request):
    method = request.method
    raw_path = request.path.lstrip("/")
    x_country = request.headers.get('X-country', '').strip()

    print(json.dumps({
        "severity": "INFO",
        "message": "Request details parsed",
        "method": method,
        "raw_path": raw_path,
        "x_country": x_country
    }))

    # CHECK FORBIDDEN COUNTRY
    if x_country and x_country.lower() in FORBIDDEN_COUNTRIES:
        log_struct(400, method, raw_path or "/", country=x_country, severity="ERROR", error_type="FORBIDDEN_COUNTRY")
        return (
            json.dumps({"error": "Permission Denied", "message": f"Requests from {x_country} are forbidden", "country": x_country}),
            400,
            {"Content-Type": "application/json"}
        )

    # CHECK METHOD
    if method != "GET":
        print(json.dumps({"severity": "ERROR", "message": f"Unsupported method: {method}"}))
        log_struct(501, method, raw_path or "/", country=x_country, severity="ERROR", error_type="NOT_IMPLEMENTED")
        return (
            json.dumps({"error": "Not Implemented", "message": f"{method} not supported"}),
            501,
            {"Content-Type": "application/json"},
        )

    # CHECK EMPTY PATH
    if not raw_path:
        print(json.dumps({"severity": "WARNING", "message": "Empty path received"}))
        log_struct(400, method, "/", country=x_country, severity="WARNING", error_type="EMPTY_PATH")
        return (
            json.dumps({"error": "Bad Request", "message": "No file specified"}),
            400,
            {"Content-Type": "application/json"},
        )

    # If URL includes bucket name, strip it out
    file_path = raw_path
    if raw_path.startswith(BUCKET_NAME + "/"):
        file_path = raw_path[len(BUCKET_NAME) + 1:]

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        # CHECK FILE EXISTS
        if not blob.exists(storage_client):
            print(json.dumps({"severity": "ERROR", "message": f"Blob '{file_path}' not found in bucket '{BUCKET_NAME}'"}))
            log_struct(404, method, file_path, country=x_country, severity="ERROR", error_type="NOT_FOUND")
            return (
                json.dumps({"error": "Not Found", "message": "file not found"}),
                404,
                {"Content-Type": "application/json"},
            )

        # DOWNLOAD FILE
        content = blob.download_as_bytes()
        return (content, 200, {"Content-Type": blob.content_type or "text/html"})

    except Exception as e:
        print(json.dumps({"severity": "ERROR", "message": f"Unexpected exception: {e}"}))
        log_struct(500, method, file_path, country=x_country, severity="ERROR", error_type="INTERNAL_ERROR")
        return (
            json.dumps({"error": "Internal Server Error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )