import json
import time
import threading
from flask import Flask, request, Response
from google.cloud import storage, logging, pubsub_v1

app = Flask(__name__)

BUCKET_NAME = "priya-cc-hw2"
PROJECT_ID = "evident-gecko-486418-c7"
TOPIC_ID = "hw3-errorlogs"

FORBIDDEN_COUNTRIES = {
    'north korea', 'iran', 'cuba', 'myanmar',
    'iraq', 'libya', 'sudan', 'zimbabwe', 'syria'
}


def get_storage_client():
    return storage.Client()

def get_logger():
    return logging.Client().logger("hw4-file-service")

def publish_error(entry: dict):
    def _publish():
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
            publisher.publish(topic=topic_path, data=json.dumps(entry).encode("utf-8"))
        except Exception as e:
            print(json.dumps({"severity": "ERROR", "message": f"Pub/Sub publish failed: {e}"}))
    threading.Thread(target=_publish, daemon=True).start()


def log_struct(status, method, path, country=None, severity="WARNING", error_type=None):
    entry = {
        "status": status,
        "method": method,
        "path": path,
        "error_type": error_type,
        "timestamp": time.time(),
    }
    if country:
        entry["country"] = country

    print(json.dumps({**entry, "severity": severity}))

    try:
        get_logger().log_struct(entry, severity=severity)
    except Exception as e:
        print(json.dumps({"severity": "ERROR", "message": f"Cloud logging failed: {e}"}))

    if status in [400, 404, 500, 501]:
        publish_error(entry)


@app.route("/", defaults={"file_path": ""}, methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
@app.route("/<path:file_path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
def serve_file(file_path):
    method = request.method
    x_country = request.headers.get("X-country", "").strip()

    print(json.dumps({
        "severity": "INFO",
        "message": "Request received",
        "method": method,
        "path": file_path,
        "country": x_country
    }))

    # CHECK FORBIDDEN COUNTRY
    if x_country and x_country.lower() in FORBIDDEN_COUNTRIES:
        log_struct(400, method, file_path or "/", country=x_country, severity="CRITICAL", error_type="FORBIDDEN_COUNTRY")
        return Response(
            json.dumps({"error": "Permission Denied", "message": f"Requests from {x_country} are forbidden"}),
            status=400,
            mimetype="application/json"
        )

    # CHECK METHOD
    if method != "GET":
        log_struct(501, method, file_path or "/", severity="WARNING", error_type="NOT_IMPLEMENTED")
        return Response(
            json.dumps({"error": "Not Implemented", "message": f"{method} not supported"}),
            status=501,
            mimetype="application/json"
        )

    # CHECK EMPTY PATH
    if not file_path:
        log_struct(400, method, "/", severity="WARNING", error_type="EMPTY_PATH")
        return Response(
            json.dumps({"error": "Bad Request", "message": "No file specified"}),
            status=400,
            mimetype="application/json"
        )

    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        if not blob.exists():
            log_struct(404, method, file_path, severity="WARNING", error_type="NOT_FOUND")
            return Response(
                json.dumps({"error": "Not Found", "message": f"{file_path} not found"}),
                status=404,
                mimetype="application/json"
            )

        content = blob.download_as_bytes()
        return Response(content, status=200, mimetype=blob.content_type or "text/html")

    except Exception as e:
        print(json.dumps({"severity": "ERROR", "message": f"Unexpected exception: {e}"}))
        log_struct(500, method, file_path, severity="ERROR", error_type="INTERNAL_ERROR")
        return Response(
            json.dumps({"error": "Internal Server Error", "message": str(e)}),
            status=500,
            mimetype="application/json"
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)