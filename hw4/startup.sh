#!/bin/bash
exec > /var/log/startup.log 2>&1
set -euxo pipefail

echo "=== Startup script begins ==="

apt-get update
apt-get install -y python3 python3-pip git

pip3 install flask google-cloud-storage google-cloud-logging google-cloud-pubsub --break-system-packages

APP_DIR="/opt/cs528-hw2"
rm -rf "$APP_DIR"

git clone -b hw4 https://github.com/priya02-11/cs528-hw2.git "$APP_DIR"
cd "$APP_DIR/hw4"

nohup python3 main.py > /var/log/hw4-server.log 2>&1 &

echo "=== Startup script completed ==="