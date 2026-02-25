#!/bin/bash

PROJECT_ID="evident-gecko-486418-c7"
ZONE="us-central1-a"
SA_NAME="hw4-sa"
VM_NAME="hw4-server"

echo "=== Creating VM ==="
gcloud compute instances create $VM_NAME --project=$PROJECT_ID --zone=$ZONE --machine-type="e2-micro" --service-account="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" --scopes="cloud-platform" --tags="hw4-server" --image-family="debian-12" --image-project="debian-cloud" --boot-disk-size="10GB" --metadata-from-file=startup-script=startup.sh

echo "=== Done! ==="