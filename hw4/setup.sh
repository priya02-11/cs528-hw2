#!/bin/bash
exec > /var/log/startup.log 2>&1

# Install Python and Git
apt-get update
apt-get install -y python3 python3-pip git

# Clone the repo
git clone -b hw4 https://github.com/priya02-11/cs528-hw2.git /home/priya/cs528-hw2

# Go to correct directory
cd /home/priya/cs528-hw2/hw4

# Install requirements
pip3 install -r requirements.txt --break-system-packages

# Run the FastAPI server
nohup /home/priya/.local/bin/uvicorn main:app --host 0.0.0.0 --port 8080 > /var/log/hw4-server.log 2>&1 &