#!/bin/bash

# Enhanced Loan Data Pipeline Deployment Script for Dataflow
echo "========================================"
echo "Deploying Loan Data Pipeline to Dataflow"
echo "========================================"

# Set the service account credentials
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/e2e-project-etl-cc9bf99bbb88.json"
echo "✓ Service account credentials set"

# Project ID from the service account
PROJECT_ID="e2e-project-etl"
REGION="us-central1"
echo "✓ Using project: $PROJECT_ID in region: $REGION"

# Define bucket names with project ID to ensure uniqueness
INPUT_BUCKET="loan-input-data-$PROJECT_ID"
TEMP_BUCKET="dataflow-temp-$PROJECT_ID"
echo "✓ Using buckets: $INPUT_BUCKET and $TEMP_BUCKET"

# Verify the input file exists in GCS
echo "Verifying input data..."
if gsutil -q stat gs://$INPUT_BUCKET/loans.csv; then
    echo "✓ Input file found: gs://$INPUT_BUCKET/loans.csv"
else
    echo "✗ Input file not found! Uploading sample data..."
    gsutil cp loans.csv gs://$INPUT_BUCKET/
    echo "✓ Sample data uploaded"
fi

# Verify BigQuery dataset exists
echo "Verifying BigQuery dataset..."
if bq ls -d $PROJECT_ID:loan_data > /dev/null 2>&1; then
    echo "✓ BigQuery dataset exists: $PROJECT_ID:loan_data"
else
    echo "✗ BigQuery dataset not found! Creating dataset..."
    bq --location=US mk --dataset $PROJECT_ID:loan_data
    echo "✓ BigQuery dataset created"
fi

# Generate a unique job name with timestamp
JOB_NAME="loan-pipeline-job-$(date +%Y%m%d-%H%M%S)"
echo "✓ Job name: $JOB_NAME"

# Extract service account email from the credentials file
CLIENT_EMAIL=$(grep -o '"client_email": "[^"]*"' "$GOOGLE_APPLICATION_CREDENTIALS" | cut -d '"' -f 4)
if [ -z "$CLIENT_EMAIL" ]; then
    # Fallback to default format if extraction fails
    CLIENT_EMAIL="pubsub-service-account@e2e-project-etl.iam.gserviceaccount.com"
fi
SERVICE_ACCOUNT="$CLIENT_EMAIL"
echo "✓ Using service account for Dataflow: $SERVICE_ACCOUNT"

# Run the pipeline on Dataflow with additional parameters for better performance
echo "\nStarting Dataflow job..."
python3 loan_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$TEMP_BUCKET/temp \
  --staging_location=gs://$TEMP_BUCKET/staging \
  --job_name=$JOB_NAME \
  --input=gs://$INPUT_BUCKET/loans.csv \
  --output=$PROJECT_ID:loan_data.customer_loans \
  --max_num_workers=4 \
  --disk_size_gb=30 \
  --machine_type=n1-standard-2 \
  --experiments=use_runner_v2 \
  --service_account_email=$SERVICE_ACCOUNT

echo "\n✅ Pipeline job submitted to Dataflow!"
echo "✅ Monitor your job at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME?project=$PROJECT_ID"
