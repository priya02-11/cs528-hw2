#!/bin/bash

PROJECT_ID="evident-gecko-486418-c7"
ZONE="us-central1-a"
REGION="us-central1"
SA_NAME="hw4-sa"
VM_NAME="hw4-server"
BUCKET="priya-cc-hw2"
TOPIC="hw3-errorlogs"

echo "=== Creating Service Account ==="
gcloud iam service-accounts create $SA_NAME --display-name="HW4 Service Account" --project=$PROJECT_ID

echo "=== Granting Bucket Read Access ==="
gsutil iam ch serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com:roles/storage.objectViewer gs://$BUCKET

echo "=== Granting Cloud Logging Access ==="
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/logging.logWriter"

echo "=== Granting Pub/Sub Publish Access ==="
gcloud pubsub topics add-iam-policy-binding $TOPIC --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/pubsub.publisher" --project=$PROJECT_ID

echo "=== Creating Firewall Rule ==="
gcloud compute firewall-rules create allow-hw4-server --allow=tcp:8080 --target-tags=hw4-server --project=$PROJECT_ID --description="Allow HTTP traffic on port 8080"

echo "=== Reserving Static IP ==="
gcloud compute addresses create $VM_NAME-ip --region=$REGION --project=$PROJECT_ID

echo "=== Creating VM ==="
gcloud compute instances create $VM_NAME --project=$PROJECT_ID --zone=$ZONE --machine-type="e2-micro" --service-account="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" --scopes="cloud-platform" --tags="hw4-server" --image-family="debian-12" --image-project="debian-cloud" --boot-disk-size="10GB" --metadata-from-file=startup-script=startup.sh

echo "=== Assigning Static IP ==="
gcloud compute instances delete-access-config $VM_NAME --access-config-name="external-nat" --zone=$ZONE --project=$PROJECT_ID
gcloud compute instances add-access-config $VM_NAME --access-config-name="external-nat" --address=$(gcloud compute addresses describe $VM_NAME-ip --region=$REGION --format="get(address)") --zone=$ZONE --project=$PROJECT_ID

echo "=== Setup Complete! Your server IP is: ==="
gcloud compute addresses describe $VM_NAME-ip --region=$REGION --project=$PROJECT_ID --format="get(address)"