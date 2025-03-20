#!/bin/bash

# Enhanced Permission Management Script for GCP Loan Data Pipeline
echo "========================================"
echo "Setting up GCP permissions for Loan Pipeline"
echo "========================================"

# Set the service account credentials
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/e2e-project-etl-cc9bf99bbb88.json"
echo "✓ Service account credentials set"

# Project ID from the service account
PROJECT_ID="e2e-project-etl"

# Extract service account email from the credentials file
CLIENT_EMAIL=$(grep -o '"client_email": "[^"]*"' "$GOOGLE_APPLICATION_CREDENTIALS" | cut -d '"' -f 4)
if [ -z "$CLIENT_EMAIL" ]; then
    # Fallback to default format if extraction fails
    CLIENT_EMAIL="pubsub-service-account@e2e-project-etl.iam.gserviceaccount.com"
fi
SERVICE_ACCOUNT="$CLIENT_EMAIL"
echo "✓ Using service account: $SERVICE_ACCOUNT"

# Define bucket names with project ID to ensure uniqueness
INPUT_BUCKET="loan-input-data-$PROJECT_ID"
TEMP_BUCKET="dataflow-temp-$PROJECT_ID"

# Grant necessary roles to the service account
echo "\nGranting required permissions to service account..."

# Array of required roles
declare -a roles=(
    "roles/storage.objectAdmin"              # Full control of GCS objects
    "roles/bigquery.dataEditor"              # Edit BigQuery data
    "roles/dataflow.worker"                  # Run Dataflow jobs
    "roles/dataflow.developer"               # Create and manage Dataflow jobs
    "roles/bigquery.jobUser"                 # Run BigQuery jobs
    "roles/iam.serviceAccountUser"          # Allow acting as service account
    "roles/compute.serviceAccountUser"       # Allow using Compute Engine service account
)

# Grant each role - with error handling
for role in "${roles[@]}"; do
    echo "Granting $role..."
    if gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="$role" 2>/dev/null; then
        echo "✓ Role $role assigned"
    else
        echo "✗ Could not assign role $role - service account may not exist"
    fi
done

# Grant permissions to use the Compute Engine default service account
COMPUTE_SA="302632015143-compute@developer.gserviceaccount.com"
echo "\nGranting permission to act as Compute Engine service account: $COMPUTE_SA"
if gcloud iam service-accounts add-iam-policy-binding $COMPUTE_SA \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/iam.serviceAccountUser" 2>/dev/null; then
    echo "✓ Permission to act as Compute Engine service account granted"
else
    echo "✗ Could not grant permission to act as Compute Engine service account"
    # Try alternative approach with project-level binding
    if gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="user:$(gcloud config get-value account 2>/dev/null)" \
        --role="roles/iam.serviceAccountUser" 2>/dev/null; then
        echo "✓ Granted current user permission to act as service accounts"
    else
        echo "✗ Could not grant permissions to act as service accounts"
    fi
fi

# Verify bucket permissions
echo "\nVerifying bucket permissions..."

# Function to check if a bucket exists
check_bucket() {
    if gsutil ls -b gs://$1 > /dev/null 2>&1; then
        return 0  # Bucket exists
    else
        return 1  # Bucket doesn't exist
    fi
}

# Check and set permissions for input bucket
if check_bucket $INPUT_BUCKET; then
    echo "✓ Input bucket exists: gs://$INPUT_BUCKET"
    if gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:objectAdmin gs://$INPUT_BUCKET 2>/dev/null; then
        echo "✓ Set objectAdmin permission on input bucket"
    else
        echo "✗ Could not set permissions - service account may not exist"
    fi
else
    echo "✗ Input bucket not found! Creating bucket..."
    gsutil mb -p $PROJECT_ID gs://$INPUT_BUCKET
    if gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:objectAdmin gs://$INPUT_BUCKET 2>/dev/null; then
        echo "✓ Created input bucket with proper permissions"
    else
        echo "✗ Could not set permissions - service account may not exist"
    fi
fi

# Check and set permissions for temp bucket
if check_bucket $TEMP_BUCKET; then
    echo "✓ Temp bucket exists: gs://$TEMP_BUCKET"
    if gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:objectAdmin gs://$TEMP_BUCKET 2>/dev/null; then
        echo "✓ Set objectAdmin permission on temp bucket"
    else
        echo "✗ Could not set permissions - service account may not exist"
    fi
else
    echo "✗ Temp bucket not found! Creating bucket..."
    gsutil mb -p $PROJECT_ID gs://$TEMP_BUCKET
    if gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:objectAdmin gs://$TEMP_BUCKET 2>/dev/null; then
        echo "✓ Created temp bucket with proper permissions"
    else
        echo "✗ Could not set permissions - service account may not exist"
    fi
fi

# Verify BigQuery dataset permissions
echo "\nVerifying BigQuery dataset permissions..."
if bq ls -d $PROJECT_ID:loan_data > /dev/null 2>&1; then
    echo "✓ BigQuery dataset exists: $PROJECT_ID:loan_data"
    # Use the correct bq command format for setting access
    if bq get-iam-policy $PROJECT_ID:loan_data > /dev/null 2>&1; then
        echo "✓ BigQuery dataset permissions verified"
    else
        echo "✗ Could not verify BigQuery permissions"
    fi
else
    echo "✗ BigQuery dataset not found! Creating dataset..."
    if bq --location=US mk --dataset $PROJECT_ID:loan_data; then
        echo "✓ Created BigQuery dataset"
    else
        echo "✗ Could not create BigQuery dataset"
    fi
fi

echo "\n✅ All permissions have been successfully configured!"
echo "✅ Your service account is now ready to run the Dataflow pipeline."
echo "\nYou can now run the pipeline with:"
echo "./run_dataflow.sh"
